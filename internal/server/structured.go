package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/scheduler"
	"github.com/mytecor/llm-gateway/internal/structured"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

func (s *Server) strictInference(writer http.ResponseWriter, request *http.Request, logical string, executeRequest model.ExecuteRequest, contract *structured.Contract, stream bool, started time.Time) {
	route := s.config.Models[logical]
	ctx, cancel := structured.Deadline(request.Context(), started, route.RouteTimeout)
	defer cancel()
	var sink *sseStreamSink
	if stream {
		flusher, ok := writer.(http.Flusher)
		if !ok {
			writeAPIError(writer, 500, "streaming_not_supported")
			return
		}
		sink = &sseStreamSink{writer: writer, flusher: flusher, logical: logical, kind: model.RequestChat, requestID: telemetry.RequestIDFrom(ctx), strict: true}
	}
	var final *structured.Generation
	var callErr *model.CallError
	status := "failed"
	defer func() {
		provider, native := "", ""
		summary := model.RunOutcome{}
		if final != nil {
			summary = final.Outcome
			provider, native = summary.Provider, summary.Model
		}
		if ctx.Err() == context.Canceled {
			status = "cancelled"
		}
		s.observeRequest(request, logical, provider, native, status, time.Since(started))
		event, level := "request_completed", slog.LevelInfo
		code := 200
		if status != "success" {
			event, level = "request_failed", slog.LevelWarn
			code = model.CallErrorStatus(callErr)
		}
		telemetry.LogEvent(ctx, s.logger, level, event, "kind", model.RequestChat, "route", s.routeOf(logical), "logical_model", logical, "provider", provider, "native_model", native, "stream", stream, "status", status, "status_code", code, "duration_ms", time.Since(started).Milliseconds(), "ttft_ms", summary.TTFT.Milliseconds(), "attempts", summary.Attempts, "input_tokens", summary.Usage.Input, "output_tokens", summary.Usage.Output, "cached_tokens", summary.Usage.CachedTokens, "has_usage", summary.HasUsage)
	}()
	fail := func(err *model.CallError) {
		callErr = err
		if sink != nil {
			sink.Fail(err, false)
		} else {
			writeCallError(writer, err)
		}
	}
	if route.Stateful {
		executeRequest.Exchange, callErr = s.runner.BeginExchange(ctx, logical, executeRequest)
		if callErr != nil {
			fail(callErr)
			return
		}
		defer executeRequest.Exchange.Abort()
	}
	generation := 0
	execute := func(ctx context.Context, req model.ExecuteRequest) (*structured.Generation, *model.CallError) {
		if generation > 0 {
			s.metrics.ObserveStructuredCorrection(logical)
		}
		generation++
		if !stream {
			result, err := s.runner.RunGeneration(ctx, logical, req)
			if result == nil {
				return nil, err
			}
			if result.HasUsage {
				s.metrics.ObserveTokens(logical, result.Provider, result.Model, telemetry.ClientKeyFrom(ctx), result.Usage.Input, result.Usage.Output)
			}
			return &structured.Generation{Outcome: *result}, err
		}
		req.MaxBufferBytes = structured.MaxBufferBytes
		buffer := &structured.Buffer{KeepWaiting: func() bool { return ctx.Err() == nil && sink.KeepAlive(scheduler.StreamHeartbeat{}) }}
		outcome := s.runner.RunGenerationStream(ctx, logical, req, started, buffer)
		outcome.Body = buffer.Body()
		err := buffer.Err
		if err == nil && !buffer.Completed {
			err = &model.CallError{Class: model.ErrorCancelled, Status: 499}
		}
		return &structured.Generation{Outcome: *outcome, Events: buffer.Events}, err
	}
	final, callErr = structured.Run(ctx, contract, executeRequest, execute, func(o structured.Observation) {
		s.metrics.ObserveStructuredValidation(logical, o.Outcome.Provider, o.Status)
	})
	if callErr != nil {
		fail(callErr)
		return
	}
	if stream {
		var options struct {
			StreamOptions struct {
				IncludeUsage bool `json:"include_usage"`
			} `json:"stream_options"`
		}
		_ = json.Unmarshal(executeRequest.Body, &options)
		events := structured.PublicEvents(final.Events, final.Outcome.Usage, final.Outcome.HasUsage, options.StreamOptions.IncludeUsage)
		sink.Begin()
		for _, event := range events {
			if ctx.Err() != nil || !sink.Write(event) {
				status = "cancelled"
				return
			}
		}
		sink.Complete()
	} else {
		response, err := model.SanitizeJSON(final.Outcome.Body, logical)
		if err != nil {
			fail(structured.Error(502, "invalid_upstream_response"))
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(200)
		if _, err = writer.Write(response); err != nil {
			status = "cancelled"
			return
		}
	}
	status = "success"
}
