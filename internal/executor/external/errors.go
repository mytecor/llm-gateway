package external

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

func classifyContextError(ctx context.Context) *model.CallError {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &model.CallError{Class: model.ErrorTimeout, Status: 504, Cause: ctx.Err(), Replay: model.ReplayUnknown}
	}
	return &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err(), Replay: model.ReplayUnknown}
}

func fromWireCallError(ctx context.Context, w *wireCallError, fallbackErr string) *model.CallError {
	if ctx.Err() != nil {
		return classifyContextError(ctx)
	}
	if w == nil {
		var cause error
		if fallbackErr != "" {
			cause = errors.New(fallbackErr)
		}
		return &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: cause, Scope: model.FailureScopeProvider}
	}
	var cause error
	if w.Detail != "" {
		cause = errors.New(w.Detail)
	}
	return &model.CallError{
		Code:   w.Code,
		Class:  model.ErrorClass(w.Class),
		Status: w.Status,
		Cause:  cause,
		Scope:  model.FailureScope(w.Scope),
		Replay: model.ReplaySafety(w.Replay),
	}
}

func (d *Driver) logResult(ctx context.Context, target model.Target, kind model.RequestKind, started time.Time, wireErr *wireCallError, callErr *model.CallError) {
	if d.logger == nil {
		return
	}
	if wireErr != nil && wireErr.Log == "provider_request_rejected" {
		attrs := []any{
			"provider", target.Provider,
			"native_model", target.Model,
			"kind", kind,
			"status_code", callErr.Status,
			"error_type", string(callErr.Class),
		}
		if callErr.Cause != nil {
			attrs = append(attrs, "detail", telemetry.SafeLogDetail(callErr.Cause.Error()))
		}
		telemetry.LogEvent(ctx, d.logger, slog.LevelWarn, "provider_request_rejected", attrs...)
		return
	}
	attrs := []any{
		"provider", target.Provider,
		"native_model", target.Model,
		"kind", kind,
		"status_code", callErr.Status,
		"error_type", string(callErr.Class),
		"duration_ms", time.Since(started).Milliseconds(),
	}
	if wireErr != nil && wireErr.Detail != "" {
		if detail := telemetry.SafeLogDetail(wireErr.Detail); detail != "" {
			attrs = append(attrs, "detail", detail)
		}
	}
	if callErr.Class == model.ErrorCancelled {
		telemetry.LogEvent(ctx, d.logger, slog.LevelDebug, "upstream_request_cancelled", attrs...)
		return
	}
	telemetry.LogEvent(ctx, d.logger, slog.LevelWarn, "upstream_request_failed", attrs...)
}

func (d *Driver) logUpstreamSuccess(ctx context.Context, target model.Target, kind model.RequestKind, started time.Time) {
	if d.logger == nil {
		return
	}
	telemetry.LogEvent(ctx, d.logger, slog.LevelDebug, "upstream_request_accepted",
		"provider", target.Provider,
		"native_model", target.Model,
		"kind", kind,
		"status_code", 200,
		"duration_ms", time.Since(started).Milliseconds(),
	)
}
