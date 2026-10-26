package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

// StreamSink serializes a run to its transport. Calls are synchronous and made
// by the run goroutine. A false Write or KeepAlive stops the run and releases
// its resources. Fail may precede Begin when selection fails.
type StreamSink interface {
	Begin()
	Write(model.StreamEvent) bool
	KeepAlive(StreamHeartbeat) bool
	Complete()
	Fail(*model.CallError, bool)
}

// StreamHeartbeat is the output position needed to encode a transport keep-alive.
// Synthetic keep-alives never enter history, usage, or repetition detection.
type StreamHeartbeat struct {
	ChunkID       string
	TextStarted   bool
	ToolCallIndex *int
}

// streamRun owns one client stream across selection, continuation and recovery.
// Only its goroutine mutates the selected stream or its admission release.
type streamRun struct {
	runner                               *Runner
	ctx                                  context.Context
	logical                              string
	request                              model.ExecuteRequest
	started                              time.Time
	sink                                 StreamSink
	begun, stopped                       bool
	current                              *SelectedStream
	partial                              *subsystem.PartialStreamOutput
	continuation                         policy.ContinueConfig
	idleTimeout                          time.Duration
	idleTimer                            *time.Timer
	repDetector                          *subsystem.RepetitionDetector
	sawFinishReason                      bool
	inTokens, outTokens, cachedTokens    int64
	hasUsage                             bool
	brokenProviders                      []string
	chainRetriesLeft, holdRecoveriesLeft int
}

// RunStream owns the complete streaming lifecycle, including the selected
// provider's cancellation and the route admission permit, until transport exit.
func (r *Runner) RunStream(ctx context.Context, logical string, request model.ExecuteRequest, started time.Time, sink StreamSink) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	continuation, _ := r.ContinuePolicy(logical)
	x := &streamRun{
		runner: r, ctx: ctx, logical: logical, request: request, started: started,
		sink: sink, partial: &subsystem.PartialStreamOutput{},
		continuation: continuation, idleTimeout: r.StreamIdleTimeout(logical),
		chainRetriesLeft: continuation.Retries, holdRecoveriesLeft: max(1, continuation.Retries),
	}
	defer x.closeCurrent()
	if !x.selectWinner() {
		return
	}
	x.begin()
	if cfg, enabled := r.RepetitionPolicy(logical); enabled {
		x.repDetector = subsystem.NewRepetitionDetector(cfg)
	}
	if x.idleTimeout > 0 {
		x.idleTimer = time.NewTimer(x.idleTimeout)
		defer x.idleTimer.Stop()
	}
	x.relay()
}

func closeSelected(selected *SelectedStream) {
	if selected == nil {
		return
	}
	selected.Cancel()
	if selected.Release != nil {
		selected.Release()
		selected.Release = nil
	}
}

func (x *streamRun) closeCurrent() { closeSelected(x.current) }

func (x *streamRun) begin() {
	if !x.begun {
		x.begun = true
		x.sink.Begin()
	}
}

func (x *streamRun) write(event model.StreamEvent) bool {
	if x.stopped {
		return false
	}
	if !x.sink.Write(event) {
		x.stopped = true
		return false
	}
	return true
}

func (x *streamRun) writeClientKeepAlive() bool {
	x.begin()
	heartbeat := StreamHeartbeat{ChunkID: x.partial.LastChunkID, TextStarted: x.partial.GotText}
	if x.partial.GotToolCalls && len(x.partial.ToolCalls) > 0 {
		index := x.partial.ToolCalls[len(x.partial.ToolCalls)-1].Index
		heartbeat.ToolCallIndex = &index
	}
	if !x.sink.KeepAlive(heartbeat) {
		x.stopped = true
		return false
	}
	return true
}

func (x *streamRun) selectWinner() bool {
	type result struct {
		selected *SelectedStream
		err      *model.CallError
	}
	// An unbuffered handoff keeps ownership in the selection goroutine until
	// this run accepts the winner. Early transport exit cancels and releases it.
	results := make(chan result)
	go func() {
		selected, err := x.runner.SelectStream(x.ctx, x.logical, x.request)
		select {
		case results <- result{selected, err}:
		case <-x.ctx.Done():
			closeSelected(selected)
		}
	}()
	ticker := time.NewTicker(policy.ContinueRecoveryKeepAlive)
	defer ticker.Stop()
	for {
		select {
		case <-x.ctx.Done():
			return false
		case <-ticker.C:
			if !x.writeClientKeepAlive() {
				return false
			}
		case result := <-results:
			if result.err != nil {
				telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelWarn, "request_failed",
					"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
					"status_code", model.CallErrorStatus(result.err), "error_type", string(result.err.Class),
					"duration_ms", time.Since(x.started).Milliseconds())
				if x.begun {
					x.observeRequest("", "", "failed")
				}
				x.fail(result.err)
				return false
			}
			x.current = result.selected
			return true
		}
	}
}

func (x *streamRun) routeName() string {
	if route := x.runner.config.Models[x.logical]; route != nil {
		return route.Name
	}
	return ""
}

func (x *streamRun) observeRequest(provider, native, status string) {
	x.runner.metrics.ObserveRequest(x.routeName(), x.logical, provider, native, status,
		telemetry.ClientKeyFrom(x.ctx), time.Since(x.started))
}

func (x *streamRun) fail(err *model.CallError) {
	if !x.stopped {
		x.sink.Fail(err, x.runner.StreamRetryable(x.logical, err))
	}
}

func (x *streamRun) bind(event model.StreamEvent) {
	if x.request.Kind == model.RequestResponses {
		x.runner.BindResponsesAffinity(x.logical, x.current.Provider, event.Data, event.Event)
	}
}

func (x *streamRun) markFinish(data []byte) {
	if !x.sawFinishReason && x.request.Kind == model.RequestChat && model.ExtractFinishReason(data) {
		x.sawFinishReason = true
	}
}

func (x *streamRun) observePartial(data []byte) { subsystem.AccumulatePartial(data, x.partial) }

func (x *streamRun) rearmIdle() {
	if x.idleTimer == nil {
		return
	}
	if !x.idleTimer.Stop() {
		select {
		case <-x.idleTimer.C:
		default:
		}
	}
	x.idleTimer.Reset(x.idleTimeout)
}

func (x *streamRun) observe(data []byte) {
	if summary, ok := model.ExtractUsageFull(data); ok {
		x.inTokens += summary.Input
		x.outTokens += summary.Output
		x.cachedTokens += summary.CachedTokens
		x.hasUsage = true
		x.runner.metrics.ObserveTokens(x.logical, x.current.Provider, x.current.Model, telemetry.ClientKeyFrom(x.ctx), summary.Input, summary.Output)
	}
}

func (x *streamRun) swapTo(next *SelectedStream) bool {
	x.current.Cancel()
	x.brokenProviders = append(x.brokenProviders, x.current.Provider)
	// Admission belongs to the client run, so a takeover retains the same
	// permit until the final winner ends or the transport disconnects.
	if x.current.Release != nil {
		if next.Release != nil {
			next.Release()
		}
		next.Release = x.current.Release
		x.current.Release = nil
	}

	x.current = next
	x.sawFinishReason = false
	if x.repDetector != nil {
		x.repDetector.Reset()
	}
	x.rearmIdle()
	for _, event := range x.current.Buffered {
		x.observePartial(event.Data)
		x.observe(event.Data)
		x.markFinish(event.Data)
		if !x.write(event) {
			return false
		}
	}
	return true
}

func (x *streamRun) logContinue(event string, from string, next *SelectedStream) {
	telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelInfo, event,
		"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
		"from_provider", from,
		"to_provider", next.Provider,
		"reshare", x.continuation.Reshare,
		"established_ms", time.Since(x.started).Milliseconds(),
	)
}

func (x *streamRun) dispatchContinue(broken []string) *SelectedStream {
	next, callErr := x.runner.ContinueStream(x.ctx, x.logical, x.request, x.partial, broken)
	if callErr != nil {
		telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelWarn, "llm_continue_failed",
			"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
			"from_provider", x.current.Provider,
			"status_code", model.CallErrorStatus(callErr), "error_type", string(callErr.Class),
			"attempts", x.current.Attempts,
		)
		return nil
	}
	return next
}

func (x *streamRun) takeover() bool {
	if x.stopped || x.ctx.Err() != nil || !x.continuation.Enabled || x.request.Kind != model.RequestChat {
		return false
	}
	if x.partial.GotToolCalls {
		// A partial tool call cannot be resumed as assistant prose on another
		// provider. Complete calls are handled by finalizeCompletedStream.
		return false
	}
	excluded := append(append([]string(nil), x.brokenProviders...), x.current.Provider)
	if next := x.dispatchContinue(excluded); next != nil {
		from := x.current.Provider
		continued := x.swapTo(next)
		x.runner.metrics.ObserveContinue(from, next.Provider, "takeover")
		x.logContinue("llm_continue", from, next)
		return continued
	}
	retried := false
	for x.chainRetriesLeft > 0 {
		x.chainRetriesLeft--
		retried = true
		telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelWarn, "llm_chain_retry",
			"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
			"from_provider", x.current.Provider,
			"retries_left", x.chainRetriesLeft,
			"established_ms", time.Since(x.started).Milliseconds(),
		)
		x.runner.metrics.ObserveChainRetry("started")
		x.brokenProviders = nil
		if next := x.dispatchContinue(nil); next != nil {
			from := x.current.Provider
			continued := x.swapTo(next)
			x.runner.metrics.ObserveContinue(from, next.Provider, "chain_retry")
			x.runner.metrics.ObserveChainRetry("completed")
			x.logContinue("llm_continue", from, next)
			return continued
		}
	}
	if retried {
		x.runner.metrics.ObserveChainRetry("exhausted")
	}
	return false
}

func (x *streamRun) holdForRecovery(broken *model.CallError) bool {
	if x.stopped || x.ctx.Err() != nil || !x.continuation.Enabled || x.request.Kind != model.RequestChat || x.holdRecoveriesLeft <= 0 {
		return false
	}
	if x.partial.GotToolCalls {
		return false
	}
	wait := x.continuation.Wait
	if wait <= 0 {
		return false
	}
	x.holdRecoveriesLeft--
	horizon := time.NewTimer(wait)
	defer horizon.Stop()
	recheck := time.NewTicker(policy.ContinueRecoveryRecheck)
	defer recheck.Stop()
	keepAlive := time.NewTicker(policy.ContinueRecoveryKeepAlive)
	defer keepAlive.Stop()
	telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelWarn, "llm_stream_held",
		"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
		"provider", x.current.Provider,
		"native_model", x.current.Model,
		"status_code", model.CallErrorStatus(broken), "error_type", string(broken.Class),
		"wait_ms", wait.Milliseconds(),
		"attempts", x.current.Attempts,
		"duration_ms", time.Since(x.started).Milliseconds(),
	)
	for {
		select {
		case <-x.ctx.Done():
			return false
		case <-horizon.C:
			telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelWarn, "llm_stream_hold_exhausted",
				"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
				"provider", x.current.Provider,
				"wait_ms", wait.Milliseconds(),
				"duration_ms", time.Since(x.started).Milliseconds(),
			)
			return false
		case <-keepAlive.C:
			if !x.writeClientKeepAlive() {
				return false
			}
			x.rearmIdle()
		case <-recheck.C:
			next, callErr := x.runner.ContinueStream(x.ctx, x.logical, x.request, x.partial, nil)
			if callErr != nil {
				continue
			}
			from := x.current.Provider
			continued := x.swapTo(next)
			x.rearmIdle()
			x.runner.metrics.ObserveContinue(from, next.Provider, "wait_recovery")
			x.logContinue("llm_continue", from, next)
			return continued
		}
	}
}

func (x *streamRun) stopLoop() bool {
	x.runner.metrics.ObserveRepetition(x.routeName(), x.current.Provider)
	telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelWarn, "llm_repetition_detected",
		"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
		"provider", x.current.Provider,
		"native_model", x.current.Model,
		"partial_chars", len(x.partial.Content),
		"established_ms", time.Since(x.started).Milliseconds(),
	)
	loop := &model.CallError{Class: model.ErrorUpstream, Status: 502,
		Cause: errors.New("repetition loop detected in stream")}
	x.runner.RecordStreamFailure(x.ctx, x.logical, x.current.Provider, x.current.Model, loop)
	if x.takeover() || x.holdForRecovery(loop) {
		return true
	}
	x.observeRequest(x.current.Provider, x.current.Model, "failed")
	x.fail(loop)
	return false
}

func (x *streamRun) completeStreamSuccess() {
	x.sink.Complete()
	telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelInfo, "request_completed",
		"kind", x.request.Kind,
		"route", x.routeName(),
		"logical_model", x.logical,
		"provider", x.current.Provider,
		"native_model", x.current.Model,
		"stream", true,
		"status_code", 200,
		"status", "success",
		"duration_ms", time.Since(x.started).Milliseconds(),
		"ttft_ms", x.current.TTFT.Milliseconds(),
		"input_tokens", x.inTokens,
		"output_tokens", x.outTokens,
		"cached_tokens", x.cachedTokens,
		"has_usage", x.hasUsage,
		"attempts", x.current.Attempts,
	)
	x.observeRequest(x.current.Provider, x.current.Model, "success")
}

func (x *streamRun) finalizeCompletedStream() bool {
	if x.stopped {
		return true
	}
	if x.request.Kind != model.RequestChat {
		return false
	}
	if x.sawFinishReason && (x.partial.GotText || x.partial.GotToolCalls) {
		x.completeStreamSuccess()
		return true
	}
	if x.continuation.Enabled && !x.sawFinishReason && x.partial.HasCompleteToolCalls() {
		chunk := subsystem.SynthesizeToolCallFinishChunk(x.partial.LastChunkID, x.logical)
		if !x.write(model.StreamEvent{Data: chunk}) {
			return true
		}
		x.sawFinishReason = true
		x.runner.metrics.ObserveContinue(x.current.Provider, x.current.Provider, "finalize_tool_calls")
		telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelInfo, "llm_continue",
			"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
			"from_provider", x.current.Provider,
			"to_provider", x.current.Provider,
			"reshare", "finalize_tool_calls",
			"established_ms", time.Since(x.started).Milliseconds(),
		)
		x.completeStreamSuccess()
		return true
	}
	return false
}

func (x *streamRun) handleStreamAbruptEnd(broken *model.CallError, reason string) bool {
	telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelWarn, "request_failed",
		"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
		"provider", x.current.Provider,
		"native_model", x.current.Model,
		"status_code", model.CallErrorStatus(broken), "error_type", string(broken.Class),
		"reason", reason,
		"attempts", x.current.Attempts,
		"duration_ms", time.Since(x.started).Milliseconds(),
		"input_tokens", x.inTokens,
		"output_tokens", x.outTokens,
		"cached_tokens", x.cachedTokens,
		"has_usage", x.hasUsage,
	)
	x.runner.RecordStreamFailure(x.ctx, x.logical, x.current.Provider, x.current.Model, broken)
	return x.takeover() || x.holdForRecovery(broken)
}

func (x *streamRun) relay() {
	var idleC <-chan time.Time
	if x.idleTimer != nil {
		idleC = x.idleTimer.C
	}
	for _, event := range x.current.Buffered {
		x.bind(event)
		x.observePartial(event.Data)
		x.observe(event.Data)
		x.markFinish(event.Data)
		if x.request.Kind == model.RequestChat && x.repDetector != nil && x.repDetector.Detect(subsystem.StreamTextDelta(string(event.Data))) {
			if x.stopLoop() {
				break
			}
			return
		}
		if !x.write(event) {
			return
		}
	}
	clientKeepAlive := time.NewTicker(policy.ContinueRecoveryKeepAlive)
	defer clientKeepAlive.Stop()

	for {
		select {
		case <-clientKeepAlive.C:
			if !x.writeClientKeepAlive() {
				return
			}
		case <-x.ctx.Done():
			telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelDebug, "request_cancelled",
				"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
				"provider", x.current.Provider,
				"native_model", x.current.Model,
				"attempts", x.current.Attempts,
				"duration_ms", time.Since(x.started).Milliseconds(),
			)
			x.observeRequest(x.current.Provider, x.current.Model, "cancelled")
			return
		case <-idleC:
			stall := &model.CallError{Class: model.ErrorTimeout, Status: 504, Cause: fmt.Errorf("no stream events for %s", x.idleTimeout)}
			telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelWarn, "llm_stream_stalled",
				"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
				"provider", x.current.Provider,
				"native_model", x.current.Model,
				"idle_ms", x.idleTimeout.Milliseconds(),
				"attempts", x.current.Attempts,
				"duration_ms", time.Since(x.started).Milliseconds(),
			)
			x.runner.RecordStreamFailure(x.ctx, x.logical, x.current.Provider, x.current.Model, stall)
			if x.finalizeCompletedStream() {
				return
			}
			if !x.sawFinishReason && (x.takeover() || x.holdForRecovery(stall)) {
				continue
			}
			x.observeRequest(x.current.Provider, x.current.Model, "failed")
			x.fail(stall)
			return
		case event, open := <-x.current.Remaining:
			if !open {
				if x.request.Kind == model.RequestChat && !x.sawFinishReason {
					broken := &model.CallError{Class: model.ErrorUpstream, Status: 502,
						Cause: errors.New("winner stream ended without finish_reason")}
					recovered := x.handleStreamAbruptEnd(broken, "missing_finish_reason")
					if x.finalizeCompletedStream() {
						return
					}
					if recovered {
						continue
					}
					x.observeRequest(x.current.Provider, x.current.Model, "failed")
					x.fail(broken)
					return
				}
				if x.request.Kind == model.RequestChat && x.sawFinishReason && !x.partial.GotText && !x.partial.GotToolCalls {
					broken := &model.CallError{Class: model.ErrorUpstream, Status: 502,
						Cause: errors.New("winner stream ended with empty completion (no content, no tool calls)")}
					if x.handleStreamAbruptEnd(broken, "empty_completion") {
						continue
					}
					x.observeRequest(x.current.Provider, x.current.Model, "failed")
					x.fail(broken)
					return
				}
				x.completeStreamSuccess()
				return
			}
			if event.Err != nil {
				telemetry.LogEvent(x.ctx, x.runner.logger, slog.LevelWarn, "request_failed",
					"kind", x.request.Kind, "logical_model", x.logical, "stream", true,
					"provider", x.current.Provider,
					"native_model", x.current.Model,
					"status_code", model.CallErrorStatus(event.Err), "error_type", string(event.Err.Class),
					"attempts", x.current.Attempts,
					"duration_ms", time.Since(x.started).Milliseconds(),
					"input_tokens", x.inTokens,
					"output_tokens", x.outTokens,
					"cached_tokens", x.cachedTokens,
					"has_usage", x.hasUsage,
				)
				x.runner.RecordStreamFailure(x.ctx, x.logical, x.current.Provider, x.current.Model, event.Err)
				if x.finalizeCompletedStream() {
					return
				}
				if !x.sawFinishReason && (x.takeover() || x.holdForRecovery(event.Err)) {
					continue
				}
				x.observeRequest(x.current.Provider, x.current.Model, "failed")
				x.fail(event.Err)
				return
			}
			x.rearmIdle()
			x.observePartial(event.Data)
			x.observe(event.Data)
			x.markFinish(event.Data)
			if x.request.Kind == model.RequestChat && x.repDetector != nil && x.repDetector.Detect(subsystem.StreamTextDelta(string(event.Data))) {
				if x.stopLoop() {
					continue
				}
				return
			}
			x.bind(event)
			if !x.write(event) {
				return
			}
		}
	}
}
