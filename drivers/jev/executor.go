package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mytecor/llm-gateway/driverproto"
	"github.com/mytecor/llm-gateway/driverproto/systemone"
)

type config struct {
	Dialect      string `json:"dialect"`
	InferenceURL string `json:"inference_url"`
	APIKey       string `json:"api_key"`
}
type executor struct {
	config config
	client *http.Client
}

func newExecutor(raw json.RawMessage) (*executor, error) {
	var cfg config
	if systemone.Decode(raw, &cfg) != nil {
		return nil, errors.New("invalid JEV driver configuration")
	}
	cfg.InferenceURL = strings.TrimRight(strings.TrimSpace(cfg.InferenceURL), "/")
	if cfg.Dialect != "typesafe" && cfg.Dialect != "gonka" {
		return nil, errors.New("JEV dialect must be typesafe or gonka")
	}
	u, err := url.Parse(cfg.InferenceURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("JEV inference_url must be an HTTP base URL without credentials, query or fragment")
	}
	if cfg.APIKey == "" {
		return nil, errors.New("JEV api_key is required")
	}
	// No redirects or transport retries of POST decisions; retries belong to the scheduler.
	return &executor{config: cfg, client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (e *executor) capabilities() driverproto.Capabilities {
	full := e.config.Dialect == "typesafe"
	return driverproto.Capabilities{SystemOne: true, SystemOneImages: full, SystemOneBox: full, SystemOneLongContext: full, SafeReplay: true, SingleAttempt: true}
}
func fail(class string, status int, code, scope string) *driverproto.CallError {
	return &driverproto.CallError{Class: class, Status: status, Code: code, Scope: scope, Replay: "completed"}
}
func (e *executor) do(ctx context.Context, msg command) (json.RawMessage, *driverproto.CallError) {
	if msg.Kind != "systemone" || msg.Stream {
		return nil, fail("invalid_response", 400, "unsupported_request_kind", "request")
	}
	req, features, err := systemone.Parse(msg.Body, msg.Headers)
	if err != nil {
		return nil, fail("invalid_response", 400, "invalid_systemone_request", "request")
	}
	caps := e.capabilities()
	if (features.Images && !caps.SystemOneImages) || (features.Box && !caps.SystemOneBox) || (features.LongContext && !caps.SystemOneLongContext) {
		return nil, fail("invalid_response", 503, "no_compatible_provider", "target")
	}
	body, err := req.Body(msg.Model, e.config.Dialect)
	if err != nil {
		return nil, fail("invalid_response", 400, "invalid_systemone_request", "request")
	}
	contentType := "application/json"
	// Use multipart upstream for images to avoid the base64 expansion consuming the
	// upstream 32 MiB wire limit. The gateway's canonical NDJSON remains JSON.
	if len(req.Assets) > 0 {
		copyReq := *req
		copyReq.Model = msg.Model
		copyReq.Assets = make(map[string]systemone.Asset, len(req.Assets))
		for ref, a := range req.Assets {
			copyReq.Assets[ref] = systemone.Asset{Type: "image", Part: fmt.Sprintf("image-%d", len(copyReq.Assets)), Format: a.Format}
		}
		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)
		header := make(textproto.MIMEHeader)
		header.Set("Content-Disposition", `form-data; name="request"`)
		header.Set("Content-Type", "application/json")
		part, _ := writer.CreatePart(header)
		_ = json.NewEncoder(part).Encode(copyReq)
		for ref, a := range req.Assets {
			b, _ := a.Bytes()
			part, err := writer.CreateFormFile(copyReq.Assets[ref].Part, "image."+a.Format)
			if err != nil {
				return nil, fail("5xx", 502, "jev_encoding_failed", "provider")
			}
			_, _ = part.Write(b)
		}
		_ = writer.Close()
		body = buf.Bytes()
		contentType = writer.FormDataContentType()
	}
	if len(body) > systemone.MaxHTTPBytes {
		return nil, fail("invalid_response", 413, "systemone_body_too_large", "request")
	}
	call, err := http.NewRequestWithContext(ctx, http.MethodPost, e.config.InferenceURL+"/systemone", bytes.NewReader(body))
	if err != nil {
		return nil, fail("5xx", 502, "jev_request_failed", "provider")
	}
	call.Header.Set("Authorization", "Bearer "+e.config.APIKey)
	call.Header.Set("Content-Type", contentType)
	for k, v := range msg.Headers {
		call.Header.Set(k, v)
	}
	res, err := e.client.Do(call)
	if err != nil {
		if ctx.Err() == context.Canceled {
			return nil, fail("cancelled", 499, "request_cancelled", "request")
		}
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fail("timeout", 504, "jev_timeout", "provider")
		}
		return nil, fail("connection_error", 502, "jev_transport_error", "provider")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		status := res.StatusCode
		class, publicStatus, code, scope := "5xx", 502, "jev_upstream_error", "provider"
		switch {
		case status == 403 && e.config.Dialect == "gonka":
			publicStatus = 503
			code = "provider_not_eligible"
		case status == 429:
			class = "429"
			publicStatus = 429
			code = "jev_rate_limited"
		case status == 529 || status == 503:
			publicStatus = 503
		case status == 400 || status == 422 || status == 413 || status == 415:
			class = "invalid_response"
			publicStatus = status
			code = "invalid_systemone_request"
			scope = "request"
		case status == 404:
			class = "model_not_found"
			code = "model_not_found"
			scope = "target"
		}
		failure := fail(class, publicStatus, code, scope)
		failure.RetryAfterMs = retryAfter(res.Header.Get("Retry-After"), time.Now()).Milliseconds()
		return nil, failure
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, systemone.MaxResponseBytes+1))
	if err != nil {
		return nil, fail("5xx", 502, "jev_response_read_error", "provider")
	}
	result, err := req.ValidateResponse(data, e.config.Dialect)
	if err != nil {
		return nil, fail("invalid_response", 502, "invalid_systemone_response", "target")
	}
	return result, nil
}
func retryAfter(raw string, now time.Time) time.Duration {
	if n, err := strconv.ParseInt(raw, 10, 32); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(raw); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}
func (e *executor) models(ctx context.Context, explicitURL string) ([]driverproto.ModelInfo, error) {
	if e.config.Dialect == "gonka" && explicitURL == "" {
		return []driverproto.ModelInfo{{ID: "jev", Name: "Jev"}}, nil
	}
	endpoint := explicitURL
	if endpoint == "" {
		endpoint = e.config.InferenceURL + "/models"
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, errors.New("invalid models URL")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+e.config.APIKey)
	res, err := e.client.Do(req)
	if err != nil {
		return nil, errors.New("JEV models transport failed")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, errors.New("JEV models unavailable")
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var payload struct {
		Models []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"models"`
	}
	if json.Unmarshal(data, &payload) != nil {
		return nil, errors.New("invalid JEV catalog")
	}
	models := make([]driverproto.ModelInfo, 0, len(payload.Models))
	for _, m := range payload.Models {
		if m.Name != "" {
			models = append(models, driverproto.ModelInfo{ID: m.Name, Name: m.Description})
		}
	}
	return models, nil
}
