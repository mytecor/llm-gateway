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
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &executor{cfg, &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (e *executor) capabilities() driverproto.Capabilities {
	return driverproto.Capabilities{Embeddings: true, EmbeddingImages: true, EmbeddingAudio: true, EmbeddingVideo: true, SafeReplay: true, SingleAttempt: true}
}
func (e *executor) models(ctx context.Context, override string) ([]driverproto.ModelInfo, error) {
	endpoint := e.config.InferenceURL + "/models"
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
	body, _ := json.Marshal(contract)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.config.InferenceURL+"/embed", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fail(504, "embedding_cancelled_or_timed_out")
		}
		return nil, fail(503, "embedding_runtime_unavailable")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, embeddings.MaxResponseBytes+1))
	if err != nil || len(b) > embeddings.MaxResponseBytes {
		return nil, fail(502, "invalid_embedding_response")
	}
	if resp.StatusCode != 200 {
		status := resp.StatusCode
		if status != 400 && status != 413 && status != 415 && status != 429 && status != 503 && status != 504 {
			status = 502
		}
		result := fail(status, "embedding_runtime_error")
		var detail struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal(b, &detail) == nil {
			switch detail.Error.Code {
			case "queue_full", "context_length_exceeded", "unsupported_media", "invalid_embedding_request", "worker_failed", "deadline_exceeded", "model_not_found":
				result.Code = detail.Error.Code
			}
		}
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			result.RetryAfterMs = int64(min(seconds, 300)) * 1000
		}
		return nil, result
	}
	if _, err := contract.ValidateResponse(b); err != nil {
		return nil, fail(502, "invalid_embedding_response")
	}
	return b, nil
}
