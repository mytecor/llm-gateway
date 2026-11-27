package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/mytecor/llm-gateway/driverproto"
	"github.com/mytecor/llm-gateway/driverproto/embeddings"
)

type config struct {
	InferenceURL string `json:"inference_url"`
	ImageAlign   string `json:"image_align,omitempty"`
}
type executor struct {
	config config
	client *http.Client
}

func newExecutor(raw json.RawMessage) (*executor, error) {
	var cfg config
	if embeddings.Decode(raw, &cfg) != nil {
		return nil, errors.New("invalid embeddings driver configuration")
	}
	cfg.InferenceURL = strings.TrimRight(cfg.InferenceURL, "/")
	u, err := url.Parse(cfg.InferenceURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("inference_url must be an HTTP base URL without credentials, query or fragment")
	}
	if cfg.ImageAlign != "" && cfg.ImageAlign != imageAlignEmbeddingGemma2 {
		return nil, errors.New("image_align must be empty or \"embeddinggemma2\"")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &executor{cfg, &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (e *executor) capabilities() driverproto.Capabilities {
	return driverproto.Capabilities{Embeddings: true, EmbeddingImages: true, EmbeddingAudio: true, EmbeddingVideo: true, SafeReplay: true, SingleAttempt: true}
}

func (e *executor) modelsURL() string {
	return e.config.InferenceURL + "/v1/models"
}

func (e *executor) models(ctx context.Context, override string) ([]driverproto.ModelInfo, error) {
	endpoint := e.modelsURL()
	if override != "" && override != endpoint {
		return nil, errors.New("embeddings models_url must use the configured runtime")
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, errors.New("embedding catalog unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New("embedding catalog unavailable")
	}
	var catalog struct {
		Data []driverproto.ModelInfo `json:"data"`
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(b) > 1<<20 || json.Unmarshal(b, &catalog) != nil || len(catalog.Data) == 0 {
		return nil, errors.New("invalid embedding catalog")
	}
	for i := range catalog.Data {
		catalog.Data[i].InputModalities = []string{"text", "image", "audio", "video"}
		catalog.Data[i].EmbeddingDimensions = []int{128, 256, 512, 768}
	}
	return catalog.Data, nil
}
func fail(status int, code string) *driverproto.CallError {
	class := "5xx"
	scope := "target"
	if status == 429 {
		class = "429"
	}
	if status == 400 || status == 413 || status == 415 {
		class = "invalid_response"
		scope = "request"
	}
	if status == 504 {
		class = "timeout"
	}
	return &driverproto.CallError{Class: class, Status: status, Code: code, Scope: scope, Replay: "completed"}
}
func (e *executor) do(ctx context.Context, msg command) (json.RawMessage, *driverproto.CallError) {
	if msg.Kind != "embeddings" || msg.Stream {
		return nil, fail(400, "unsupported_request_kind")
	}
	var contract embeddings.Request
	if embeddings.Decode(msg.Body, &contract) != nil {
		return nil, fail(400, "invalid_embedding_request")
	}
	if _, err := contract.Validate(); err != nil {
		return nil, fail(400, "invalid_embedding_request")
	}
	contract.Model = msg.Model
	return e.openAIEmbeddings(ctx, contract)
}

func (e *executor) post(ctx context.Context, endpoint string, payload any) (json.RawMessage, *driverproto.CallError) {
	response, failure := e.postStream(ctx, endpoint, payload)
	if failure != nil {
		return nil, failure
	}
	defer response.Close()
	b, err := io.ReadAll(io.LimitReader(response, embeddings.MaxResponseBytes+1))
	if err != nil || len(b) > embeddings.MaxResponseBytes {
		return nil, fail(502, "invalid_embedding_response")
	}
	return b, nil
}

func (e *executor) postStream(ctx context.Context, endpoint string, payload any) (io.ReadCloser, *driverproto.CallError) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fail(400, "invalid_embedding_request")
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.config.InferenceURL+endpoint, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fail(504, "embedding_cancelled_or_timed_out")
		}
		return nil, fail(503, "embedding_runtime_unavailable")
	}
	if resp.StatusCode == 200 {
		return resp.Body, nil
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, embeddings.MaxResponseBytes+1))
	if err != nil || len(b) > embeddings.MaxResponseBytes {
		return nil, fail(502, "invalid_embedding_response")
	}
	status := resp.StatusCode
	if status != 400 && status != 413 && status != 415 && status != 429 && status != 503 && status != 504 {
		status = 502
	}
	result := fail(status, "embedding_runtime_error")
	var detail struct {
		Error struct {
			Code    json.RawMessage `json:"code"`
			Type    string          `json:"type"`
			Message string          `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(b, &detail) == nil {
		var code string
		_ = json.Unmarshal(detail.Error.Code, &code)
		// Non-causal embeddings must fit one physical batch. llama.cpp
		// 0.8 reports its size limit as a server error.
		physicalBatchLimit := detail.Error.Type == "server_error" &&
			strings.HasPrefix(detail.Error.Message, "input (") &&
			strings.Contains(detail.Error.Message, " tokens) is too large to process. increase the physical batch size ")
		if detail.Error.Type == "exceed_context_size_error" || physicalBatchLimit {
			return nil, fail(400, "context_length_exceeded")
		}
		if detail.Error.Message == "unsupported_media" {
			code = "unsupported_media"
		}
		switch code {
		case "queue_full", "context_length_exceeded", "unsupported_media", "invalid_embedding_request", "worker_failed", "deadline_exceeded", "model_not_found":
			result.Code = code
		}
	}
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		result.RetryAfterMs = int64(min(seconds, 300)) * 1000
	}
	return nil, result
}
