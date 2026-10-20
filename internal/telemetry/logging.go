package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// GatewayService is the constant service dimension carried by every structured
// event line (alongside the metric service label). It is stable so a logs
// pipeline (Alloy → Loki) can filter by service without knowing the deployment.
const GatewayService = "llm-gateway"

type logContextKey uint8

const (
	requestIDKey logContextKey = iota
	routeStageKey
	routeAttemptKey
	clientKeyKey
)

func NewGatewayLogger(level string) *slog.Logger {
	return NewGatewayLoggerTo(level, os.Stdout)
}

func NewGatewayLoggerTo(level string, output io.Writer) *slog.Logger {
	var minimum slog.Level
	switch level {
	case "error":
		minimum = slog.LevelError
	case "warn":
		minimum = slog.LevelWarn
	case "info":
		minimum = slog.LevelInfo
	case "debug", "trace":
		minimum = slog.LevelDebug
	default:
		output = io.Discard
		minimum = slog.LevelError
	}
	return slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: minimum}))
}

// LogEvent emits one structured event line. The slog JSON handler already
// renders a single JSON object per line with a timestamp (as "time") and a
// level; LogEvent pins the stable event name, the service dimension and the
// low-cardinality routing dimensions (request_id/route_stage/route_attempt)
// into the same object. One line always means one event. Prompt material,
// request/response bodies, headers and API keys are never passed here: the
// caller is responsible for that boundary (see f12-02 DoD).
func LogEvent(ctx context.Context, logger *slog.Logger, level slog.Level, event string, extra ...any) {
	attrs := LogRequestAttrs(ctx)
	attrs = append(attrs, "service", GatewayService, "event", event)
	attrs = append(attrs, extra...)
	logger.Log(ctx, level, event, attrs...)
}

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

// RequestIDFrom returns the request id pinned into the context by the
// inference handler (empty when absent, for example in direct unit tests).
func RequestIDFrom(ctx context.Context) string {
	if requestID, ok := ctx.Value(requestIDKey).(string); ok {
		return requestID
	}
	return ""
}

func WithClientKey(ctx context.Context, clientKey string) context.Context {
	return context.WithValue(ctx, clientKeyKey, clientKey)
}

// ClientKeyFrom returns the non-secret client-key ID the request was
// authenticated with (empty when client auth is disabled or the request is
// not authenticated). It never carries key material.
func ClientKeyFrom(ctx context.Context) string {
	if id, ok := ctx.Value(clientKeyKey).(string); ok {
		return id
	}
	return ""
}

func WithRouteAttempt(ctx context.Context, stage, attempt int) context.Context {
	ctx = context.WithValue(ctx, routeStageKey, stage)
	return context.WithValue(ctx, routeAttemptKey, attempt)
}

func LogRequestAttrs(ctx context.Context) []any {
	attrs := make([]any, 0, 6)
	if requestID, ok := ctx.Value(requestIDKey).(string); ok && requestID != "" {
		attrs = append(attrs, "request_id", requestID)
	}
	if stage, ok := ctx.Value(routeStageKey).(int); ok && stage > 0 {
		attrs = append(attrs, "route_stage", stage)
	}
	if attempt, ok := ctx.Value(routeAttemptKey).(int); ok && attempt > 0 {
		attrs = append(attrs, "route_attempt", attempt)
	}
	return attrs
}

func SafeLogDetail(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	const maxDetailBytes = 512
	if len(value) > maxDetailBytes {
		value = value[:maxDetailBytes] + "..."
	}
	return value
}
