package scheduler

import (
	"context"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/telemetry"
	"log/slog"
)

type routeTransition struct {
	kind     string
	target   *policy.CompiledRoute
	attempts int
	backoff  policy.BackoffConfig
	internal bool
}

func terminalOutcome(outcome *routeOutcome) bool {
	return outcome.err == nil || outcome.err.Class == model.ErrorCancelled || outcome.err.Replay == model.ReplayUnknown || outcome.pinned
}

func (r *Runner) followTransition(ctx context.Context, logical string, source *policy.CompiledRoute, transition routeTransition, request model.ExecuteRequest, streamMode bool, runtime *routeRuntime, outcome *routeOutcome, original *model.CallError) *routeOutcome {
	for attempt := 0; attempt < transition.attempts; attempt++ {
		if transition.internal {
			if !isReschedulableError(outcome.err) {
				break
			}
		} else if !transition.target.Applicable(outcome.err, attempt) {
			break
		}
		r.observeTransition(ctx, source, transition, outcome, attempt)
		if transition.kind == "retry" {
			if err := runtime.waitBackoff(ctx, backoffDuration(transition.backoff, attempt), r.sleep); err != nil {
				return &routeOutcome{err: err}
			}
		}
		var next *routeOutcome
		if transition.internal {
			for target := range outcome.failedTargets {
				runtime.excludeTargets[target] = true
			}
			if len(outcome.failedTargets) == 0 && outcome.failedProvider != "" {
				runtime.exclude[outcome.failedProvider] = true
			}
			next = r.raceRoute(ctx, logical, transition.target, request, streamMode, runtime)
		} else {
			next = r.executeRoute(ctx, logical, transition.target, request, streamMode, runtime)
		}
		if terminalOutcome(next) {
			if next.err == nil && transition.kind == "retry" {
				telemetry.LogEvent(ctx, r.logger, slog.LevelInfo, "llm_attempt",
					"route", transition.target.Name, "provider", next.provider, "native_model", next.model,
					"status", "success", "attempt", attempt+1)
			}
			return next
		}
		if next.empty {
			// An unusable named retry restores the source failure before a sibling
			// fallback is considered. Empty internal retries and fallbacks keep it.
			if transition.kind == "retry" && !transition.internal {
				return &routeOutcome{err: original}
			}
			break
		}
		outcome = next
	}
	return outcome
}

func (r *Runner) observeTransition(ctx context.Context, source *policy.CompiledRoute, transition routeTransition, outcome *routeOutcome, attempt int) {
	if transition.kind == "retry" {
		telemetry.LogEvent(ctx, r.logger, slog.LevelWarn, "llm_retry",
			"route", source.Name, "provider", outcome.failedProvider,
			"error_type", string(outcome.err.Class), "status_code", outcome.err.Status, "attempt", attempt+1)
		return
	}
	to := ""
	if len(transition.target.Pool) > 0 {
		to = transition.target.Pool[0].Provider
	}
	reason := string(outcome.err.Class)
	r.metrics.ObserveFallback(outcome.failedProvider, to, reason)
	telemetry.LogEvent(ctx, r.logger, slog.LevelWarn, "llm_fallback",
		"route", source.Name, "from_provider", outcome.failedProvider, "to_provider", to, "reason", reason)
}
