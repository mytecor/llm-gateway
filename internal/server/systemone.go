package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/mytecor/llm-gateway/driverproto/systemone"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

func (s *Server) systemOne(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	requestID := s.requestSequence.Add(1)
	r = r.WithContext(telemetry.WithRequestID(r.Context(), fmtRequestID(requestID)))
	var logical string
	var result *model.RunOutcome
	var callErr *model.CallError
	status := 400
	defer func() {
		provider, native := "", ""
		summary := model.RunOutcome{}
		if result != nil {
			summary = *result
			provider = result.Provider
			native = result.Model
		}
		outcome, event, level := "failed", "request_failed", slog.LevelWarn
		if status == 200 {
			outcome, event, level = "success", "request_completed", slog.LevelInfo
		}
		s.observeRequest(r, logical, provider, native, outcome, time.Since(started))
		code := ""
		if callErr != nil {
			code = callErr.Code
		}
		telemetry.LogEvent(r.Context(), s.logger, level, event, "kind", model.RequestSystemOne, "logical_model", logical, "provider", provider, "native_model", native, "status_code", status, "error_code", code, "duration_ms", time.Since(started).Milliseconds(), "attempts", summary.Attempts, "has_usage", summary.HasUsage, "input_tokens", summary.Usage.Input, "output_tokens", summary.Usage.Output)
	}()
	reject := func(code int, name string) { status = code; writeAPIError(w, code, name) }
	if encoding := r.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		reject(415, "unsupported_content_encoding")
		return
	}
	headers := make(map[string]string)
	for _, key := range []string{systemone.ImagesHeader, systemone.LongContextHeader} {
		values := r.Header.Values(key)
		if len(values) > 0 {
			if len(values) != 1 || values[0] != "true" {
				reject(400, "invalid_systemone_feature_header")
				return
			}
			headers[key] = values[0]
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, systemone.MaxHTTPBytes)
	body, err := readSystemOneBody(r)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			reject(413, "systemone_body_too_large")
		} else {
			reject(400, "invalid_systemone_request")
		}
		return
	}
	if len(body) > systemone.MaxCanonicalBytes {
		reject(413, "systemone_body_too_large")
		return
	}
	req, features, err := systemone.Parse(body, headers)
	if err != nil {
		reject(400, "invalid_systemone_request")
		return
	}
	logical = req.Model
	if _, exists := s.config.Models[logical]; !exists {
		reject(404, "model_not_found")
		return
	}
	telemetry.LogEvent(r.Context(), s.logger, slog.LevelInfo, "request_received", "kind", model.RequestSystemOne, "logical_model", logical)
	result, callErr = s.runner.RunWithResult(r.Context(), logical, model.ExecuteRequest{Kind: model.RequestSystemOne, Body: body, Headers: headers, SystemOneFeatures: features, Call: model.Call{ID: fmtRequestID(requestID), Principal: telemetry.ClientKeyFrom(r.Context()), LogicalModel: logical}})
	s.setRouteLimitHeader(w, logical)
	if callErr != nil {
		status = model.CallErrorStatus(callErr)
		writeCallError(w, callErr)
		return
	}
	// The native API returns the model/version that actually answered. Do not
	// apply OpenAI logical-model rewriting or Chat response sanitization here.
	s.observeTokens(r, logical, result.Provider, result.Model, result.Body)
	status = 200
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(result.Body)
}

func readSystemOneBody(r *http.Request) ([]byte, error) {
	typ, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, err
	}
	if typ == "application/json" {
		return io.ReadAll(r.Body)
	}
	if typ != "multipart/form-data" {
		return nil, errors.New("expected JSON or multipart")
	}
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, err
	}
	first, err := reader.NextPart()
	if err != nil {
		return nil, err
	}
	if first.FormName() != "request" {
		return nil, errors.New("first multipart part must be request")
	}
	data, err := io.ReadAll(first)
	if err != nil {
		return nil, err
	}
	var req systemone.Request
	if err = systemone.Decode(data, &req); err != nil {
		return nil, err
	}
	expected := make(map[string]string)
	for ref, a := range req.Assets {
		if a.Part != "" {
			if _, duplicate := expected[a.Part]; duplicate || a.Part == "request" {
				return nil, errors.New("duplicate or reserved asset part")
			}
			expected[a.Part] = ref
		}
	}
	seen := map[string]bool{"request": true}
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		name := part.FormName()
		ref, ok := expected[name]
		if !ok || name == "" || seen[name] {
			return nil, errors.New("unexpected or duplicate multipart part")
		}
		seen[name] = true
		contentType := part.Header.Get("Content-Type")
		if contentType != "" {
			media, _, err := mime.ParseMediaType(contentType)
			if err != nil || (media != "image/png" && media != "image/jpeg" && media != "image/jpg" && media != "application/octet-stream") {
				return nil, errors.New("invalid image content type")
			}
		}
		b, err := io.ReadAll(part)
		if err != nil {
			return nil, err
		}
		if len(b) > 16<<20 {
			return nil, &http.MaxBytesError{Limit: 16 << 20}
		}
		format, err := systemone.ImageFormat(b)
		if err != nil {
			return nil, err
		}
		asset := req.Assets[ref]
		asset.Part = ""
		asset.Data = base64.StdEncoding.EncodeToString(b)
		asset.Format = format
		req.Assets[ref] = asset
	}
	for name := range expected {
		if !seen[name] {
			return nil, errors.New("missing multipart image")
		}
	}
	return json.Marshal(req)
}

func fmtRequestID(id uint64) string { return strconv.FormatUint(id, 10) }
