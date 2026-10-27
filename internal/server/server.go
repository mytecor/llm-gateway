package server

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/scheduler"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

const maxRequestBody = 16 << 20

type Server struct {
	config          *config.CompiledConfig
	catalog         *subsystem.Catalog
	runner          *scheduler.Runner
	mux             *http.ServeMux
	logger          *slog.Logger
	metrics         *telemetry.Metrics
	requestSequence atomic.Uint64
}

func NewServer(config *config.CompiledConfig, catalog *subsystem.Catalog, runner *scheduler.Runner) *Server {
	server := &Server{
		config: config, catalog: catalog, runner: runner, mux: http.NewServeMux(), logger: config.Logger,
		metrics: runner.Metrics(),
	}
	server.mux.HandleFunc("GET /healthz", server.health)
	server.mux.HandleFunc("GET /v1/models", server.auth(server.models))
	server.mux.HandleFunc("POST /v1/chat/completions", server.auth(server.chat))
	server.mux.HandleFunc("POST /v1/responses", server.auth(server.responses))
	server.mux.HandleFunc("POST /admin/models/refresh", server.auth(server.refresh))
	return server
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	s.mux.ServeHTTP(writer, request)
}

// NewMetricsHandler builds a dedicated http.Handler serving only GET /metrics
// from the given registry. It is mounted on the separate loopback metrics
// listener and exposes no other endpoint, so a scrape cannot reach the client
// API surface.
func NewMetricsHandler(metrics *telemetry.Metrics) http.Handler {
	mux := http.NewServeMux()
	handler := &metricsOnly{metrics: metrics}
	mux.HandleFunc("GET /metrics", handler.serve)
	return mux
}

type metricsOnly struct {
	metrics *telemetry.Metrics
}

func (m *metricsOnly) serve(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	if err := m.metrics.WriteExposition(writer); err != nil {
		_, _ = io.WriteString(writer, "# error rendering metrics\n")
	}
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if len(s.config.ClientKeys) == 0 {
			next(writer, request)
			return
		}
		provided := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
		for id, expected := range s.config.ClientKeys {
			if len(provided) == len(expected) && subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1 {
				// Pin the non-secret client key ID into the request context so
				// the request- and usage-level metrics attribute this request to
				// the key it was authenticated with.
				next(writer, request.WithContext(telemetry.WithClientKey(request.Context(), id)))
				return
			}
		}
		writeAPIError(writer, http.StatusUnauthorized, "invalid_api_key")
	}
}

func (s *Server) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"status": "ok"})
}

// routeOf resolves the entry route name for a logical model, used as the
// route label on request-level metrics. Every discoverable logical model maps
// to exactly one entry route, so the label set stays low-cardinality.
func (s *Server) routeOf(logical string) string {
	if entry, ok := s.config.Models[logical]; ok {
		return entry.Name
	}
	return ""
}

// observeRequest records the request-level observation: counter, histogram and
// token accumulation share one call so the request lifecycle is recorded once
// at exactly one terminal point. The empty provider and native convey that no
// branch ever succeeded (for example a pre-dispatch rejection). The client key
// id comes from the authenticated request context (empty in keyless mode).
func (s *Server) observeRequest(request *http.Request, logical, provider, native, status string, duration time.Duration) {
	s.metrics.ObserveRequest(s.routeOf(logical), logical, provider, native, status, telemetry.ClientKeyFrom(request.Context()), duration)
}

// observeTokens accumulates usage from a non-stream response body and returns
// the parsed usage so the caller can embed it into the request_completed event.
// Providers that omit usage contribute nothing. The hasUsage flag tells whether
// the provider reported usage at all (zero tokens with hasUsage=false mean
// "unknown", not "zero"). provider and native identify the winning branch.
func (s *Server) observeTokens(request *http.Request, logical, provider, native string, response []byte) (input, output, cached int64, hasUsage bool) {
	summary, ok := model.ExtractUsageFull(response)
	if !ok {
		return 0, 0, 0, false
	}
	s.metrics.ObserveTokens(logical, provider, native, telemetry.ClientKeyFrom(request.Context()), summary.Input, summary.Output)
	return summary.Input, summary.Output, summary.CachedTokens, true
}

func (s *Server) models(writer http.ResponseWriter, request *http.Request) {
	created := time.Now().Unix()
	data := make([]map[string]any, 0, len(s.config.LogicalIDs))
	for _, id := range s.config.LogicalIDs {
		entry := map[string]any{"id": id, "object": "model", "created": created, "owned_by": "lattice"}
		if details, err := s.runner.ModelDetails(request.Context(), id); err == nil {
			if len(details.ReasoningEfforts) > 0 {
				entry["reasoning_efforts"] = details.ReasoningEfforts
				levels := map[string]string{}
				for _, effort := range details.ReasoningEfforts {
					levels[effort] = effort
				}
				entry["thinking_level_map"] = levels
			}
		}
		data = append(data, entry)
	}
	writeJSON(writer, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (s *Server) chat(writer http.ResponseWriter, request *http.Request) {
	s.inference(writer, request, model.RequestChat)
}

func (s *Server) responses(writer http.ResponseWriter, request *http.Request) {
	s.inference(writer, request, model.RequestResponses)
}

func (s *Server) inference(writer http.ResponseWriter, request *http.Request, kind model.RequestKind) {
	started := time.Now()
	requestID := strconv.FormatUint(s.requestSequence.Add(1), 10)
	request = request.WithContext(telemetry.WithRequestID(request.Context(), requestID))
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, maxRequestBody))
	if err != nil {
		telemetry.LogEvent(request.Context(), s.logger, slog.LevelWarn, "request_rejected",
			"kind", kind,
			"status_code", http.StatusBadRequest,
			"reason", "invalid_body",
		)
		writeAPIError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	var metadata struct {
		Model     string `json:"model"`
		Stream    bool   `json:"stream"`
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(body, &metadata); err != nil || strings.TrimSpace(metadata.Model) == "" {
		telemetry.LogEvent(request.Context(), s.logger, slog.LevelWarn, "request_rejected",
			"kind", kind,
			"status_code", http.StatusBadRequest,
			"reason", "invalid_json_or_model",
		)
		writeAPIError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	if _, exists := s.config.Models[metadata.Model]; !exists {
		telemetry.LogEvent(request.Context(), s.logger, slog.LevelWarn, "request_rejected",
			"kind", kind,
			"logical_model", metadata.Model,
			"status_code", http.StatusNotFound,
			"reason", "model_not_found",
		)
		writeAPIError(writer, http.StatusNotFound, "model_not_found")
		return
	}
	telemetry.LogEvent(request.Context(), s.logger, slog.LevelInfo, "request_received",
		"kind", kind,
		"logical_model", metadata.Model,
		"stream", metadata.Stream,
	)
	call := model.Call{ID: requestID, Principal: telemetry.ClientKeyFrom(request.Context()), LogicalModel: metadata.Model, CWD: strings.TrimSpace(request.Header.Get("X-Agent-CWD"))}
	for _, value := range []string{request.Header.Get("X-Session-Affinity"), request.Header.Get("Session-ID"), request.Header.Get("session_id"), request.Header.Get("X-Client-Request-ID"), metadata.SessionID} {
		if strings.TrimSpace(value) != "" {
			call.Conversation = strings.TrimSpace(value)
			break
		}
	}
	if len(call.Conversation) > 512 || strings.IndexFunc(call.Conversation, unicode.IsControl) >= 0 {
		writeAPIError(writer, http.StatusBadRequest, "invalid_session_id")
		return
	}
	if route := s.config.Models[metadata.Model]; route.Stateful {
		if kind != model.RequestChat {
			writeAPIError(writer, http.StatusBadRequest, "unsupported_request_kind")
			return
		}
		if call.Conversation == "" {
			var id [16]byte
			if _, err := rand.Read(id[:]); err != nil {
				writeAPIError(writer, 500, "session_id_failed")
				return
			}
			call.Conversation = "session-" + hex.EncodeToString(id[:])
		}
		writer.Header().Set("X-Session-ID", call.Conversation)
	}
	executeRequest := model.ExecuteRequest{Kind: kind, Body: body, Call: call}
	if metadata.Stream {
		s.stream(writer, request, metadata.Model, executeRequest, started)
		return
	}
	result, callErr := s.runner.RunWithResult(request.Context(), metadata.Model, executeRequest)
	if callErr != nil {
		telemetry.LogEvent(request.Context(), s.logger, slog.LevelWarn, "request_failed",
			"kind", kind,
			"logical_model", metadata.Model,
			"stream", metadata.Stream,
			"status_code", model.CallErrorStatus(callErr),
			"error_type", string(callErr.Class),
			"duration_ms", time.Since(started).Milliseconds(),
		)
		s.observeRequest(request, metadata.Model, "", "", "failed", time.Since(started))
		writeCallError(writer, callErr)
		return
	}
	response := result.Body
	// Bind the winning provider to the opaque Responses state identifiers;
	// the identifiers themselves and the request body never reach the logs.
	if kind == model.RequestResponses {
		s.runner.BindResponsesAffinity(metadata.Model, result.Provider, response, "")
	}
	provider := result.Provider
	input, output, cached, hasUsage := s.observeTokens(request, metadata.Model, result.Provider, result.Model, response)
	response, err = model.SanitizeJSON(response, metadata.Model)
	if err != nil {
		telemetry.LogEvent(request.Context(), s.logger, slog.LevelError, "request_failed",
			"kind", kind,
			"logical_model", metadata.Model,
			"stream", metadata.Stream,
			"status_code", http.StatusBadGateway,
			"error_type", string(model.ErrorInvalid),
			"duration_ms", time.Since(started).Milliseconds(),
		)
		s.observeRequest(request, metadata.Model, provider, result.Model, "failed", time.Since(started))
		writeAPIError(writer, http.StatusBadGateway, "invalid_upstream_response")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(response)
	telemetry.LogEvent(request.Context(), s.logger, slog.LevelInfo, "request_completed",
		"kind", kind,
		"route", s.routeOf(metadata.Model),
		"logical_model", metadata.Model,
		"provider", provider,
		"native_model", result.Model,
		"stream", metadata.Stream,
		"status_code", http.StatusOK,
		"status", "success",
		"duration_ms", time.Since(started).Milliseconds(),
		"ttft_ms", result.TTFT.Milliseconds(),
		"input_tokens", input,
		"output_tokens", output,
		"cached_tokens", cached,
		"has_usage", hasUsage,
		"attempts", result.Attempts,
	)
	s.observeRequest(request, metadata.Model, provider, result.Model, "success", time.Since(started))
}

func (s *Server) refresh(writer http.ResponseWriter, request *http.Request) {
	errorsByProvider := s.catalog.Refresh(request.Context())
	failed := make([]string, 0, len(errorsByProvider))
	for providerID := range errorsByProvider {
		failed = append(failed, providerID)
	}
	sort.Strings(failed)
	status := http.StatusOK
	if len(failed) > 0 {
		status = http.StatusBadGateway
	}
	writeJSON(writer, status, map[string]any{"refreshed": len(failed) == 0, "failed_count": len(failed)})
}

func writeCallError(writer http.ResponseWriter, callErr *model.CallError) {
	status := model.CallErrorStatus(callErr)
	typeName := "upstream_error"
	if callErr != nil {
		typeName = string(callErr.Class)
	}
	if callErr != nil && callErr.Code != "" {
		writeJSON(writer, status, map[string]any{"error": map[string]any{"message": http.StatusText(status), "type": typeName, "code": callErr.Code}})
		return
	}
	writeAPIError(writer, status, typeName)
}

func writeAPIError(writer http.ResponseWriter, status int, kind string) {
	writeJSON(writer, status, map[string]any{
		"error": map[string]any{"message": http.StatusText(status), "type": kind},
	})
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(payload)
}
