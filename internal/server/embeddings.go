package server

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mytecor/llm-gateway/driverproto/embeddings"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

// Google SDKs use x-goog-api-key, backed by the same client keys as Bearer.
// Clone before changing headers; never put the credential in request logging.
func (s *Server) googleEmbeddingAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("x-goog-api-key")
		bearer := r.Header.Get("Authorization")
		if len(r.Header.Values("x-goog-api-key")) > 1 || len(r.Header.Values("Authorization")) > 1 || (key != "" && bearer != "" && bearer != "Bearer "+key) {
			writeEmbeddingError(w, true, 400, "conflicting_credentials")
			return
		}
		if key != "" {
			r = r.Clone(r.Context())
			r.Header.Set("Authorization", "Bearer "+key)
		}
		authenticated, ok := s.authenticated(r)
		if !ok {
			writeEmbeddingError(w, true, 401, "invalid_api_key")
			return
		}
		next(w, authenticated)
	}
}

func writeEmbeddingError(w http.ResponseWriter, google bool, status int, code string) {
	if !google {
		writeAPIError(w, status, code)
		return
	}
	name := map[int]string{400: "INVALID_ARGUMENT", 401: "UNAUTHENTICATED", 404: "NOT_FOUND", 413: "INVALID_ARGUMENT", 415: "INVALID_ARGUMENT", 429: "RESOURCE_EXHAUSTED", 502: "INTERNAL", 503: "UNAVAILABLE", 504: "DEADLINE_EXCEEDED"}[status]
	if name == "" {
		name = "INTERNAL"
	}
	writeJSON(w, status, map[string]any{"error": map[string]any{"code": status, "status": name, "message": code}})
}

func (s *Server) embeddings(w http.ResponseWriter, r *http.Request) { s.embeddingRequest(w, r, false) }
func (s *Server) googleEmbeddings(w http.ResponseWriter, r *http.Request) {
	s.embeddingRequest(w, r, true)
}

func (s *Server) embeddingRequest(w http.ResponseWriter, r *http.Request, google bool) {
	started := time.Now()
	id := strconv.FormatUint(s.requestSequence.Add(1), 10)
	r = r.WithContext(telemetry.WithRequestID(r.Context(), id))
	status, logical, provider, native := 400, "", "", ""
	var tokens int64
	defer func() {
		outcome, event := "failed", "request_failed"
		if status == 200 {
			outcome, event = "success", "request_completed"
		}
		s.observeRequest(r, logical, provider, native, outcome, time.Since(started))
		telemetry.LogEvent(r.Context(), s.logger, slog.LevelInfo, event, "kind", model.RequestEmbeddings, "logical_model", logical, "provider", provider, "native_model", native, "status_code", status, "duration_ms", time.Since(started).Milliseconds(), "input_tokens", tokens, "output_tokens", 0)
	}()
	reject := func(n int, code string) { status = n; writeEmbeddingError(w, google, n, code) }
	batch := false
	if google {
		action := r.PathValue("action")
		switch {
		case strings.HasSuffix(action, ":embedContent"):
			logical = strings.TrimSuffix(action, ":embedContent")
		case strings.HasSuffix(action, ":batchEmbedContents"):
			logical = strings.TrimSuffix(action, ":batchEmbedContents")
			batch = true
		default:
			reject(404, "unsupported_embedding_method")
			return
		}
	}
	if e := r.Header.Get("Content-Encoding"); e != "" && e != "identity" {
		reject(415, "unsupported_content_encoding")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, embeddings.MaxHTTPBytes))
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			reject(413, "embedding_body_too_large")
		} else {
			reject(400, "invalid_embedding_request")
		}
		return
	}
	var req embeddings.Request
	encoding := "float"
	if google {
		req, err = embeddings.ParseGoogle(body, logical, batch)
	} else {
		req, encoding, err = embeddings.ParseOpenAIWithDefaultModel(body, s.config.Raw.DefaultEmbeddingModel)
		logical = req.Model
	}
	if err != nil {
		reject(400, err.Error())
		return
	}
	if _, ok := s.config.Models[logical]; !ok {
		reject(404, "model_not_found")
		return
	}
	features, _ := req.Validate()
	canonical, _ := json.Marshal(req)
	if len(canonical) > embeddings.MaxCommandBytes-(64<<10) {
		reject(413, "embedding_body_too_large")
		return
	}
	result, callErr := s.runner.RunWithResult(r.Context(), logical, model.ExecuteRequest{Kind: model.RequestEmbeddings, Body: canonical, EmbeddingFeatures: features, Call: model.Call{ID: id, Principal: telemetry.ClientKeyFrom(r.Context()), LogicalModel: logical}})
	s.setRouteLimitHeader(w, logical)
	if callErr != nil {
		status = model.CallErrorStatus(callErr)
		if callErr.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(max(1, int(callErr.RetryAfter.Seconds()))))
		}
		code := callErr.Code
		if code == "" {
			code = "embedding_failed"
		}
		writeEmbeddingError(w, google, status, code)
		return
	}
	provider, native = result.Provider, result.Model
	response, err := req.ValidateResponse(result.Body)
	if err != nil {
		reject(502, "invalid_embedding_response")
		return
	}
	for _, v := range response.Embeddings {
		tokens += v.Tokens
	}
	s.metrics.ObserveTokens(logical, provider, native, telemetry.ClientKeyFrom(r.Context()), tokens, 0)
	status = 200
	if google {
		writeJSON(w, 200, response.Google(batch))
	} else {
		writeJSON(w, 200, response.OpenAI(logical, encoding))
	}
}
