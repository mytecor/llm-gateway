package scheduler

import (
	"strings"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

const (
	minAdaptiveAttemptSlice   = 25 * time.Millisecond
	minHedgeWorthLatency      = 50 * time.Millisecond
	minTransitionWorthLatency = 100 * time.Millisecond
)

// adaptiveAttemptPlan is an immutable dispatch snapshot. A serial failover may
// cut a stalled attempt only when both targets have fresh, warmed-up p95 profiles
// and enough time remains for conservative primary and backup latency estimates.
type adaptiveAttemptPlan struct {
	after   time.Duration
	reserve time.Duration
}

func conservativeAttemptLatency(p95 time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if p95 > maxDuration/2 {
		return maxDuration
	}
	return max(minAdaptiveAttemptSlice, 2*p95)
}

// planAdaptiveAttempt runs only in the scheduling goroutine. Parallel race and
// hedge already control tail latency by selecting a winner; they keep primaries
// alive until that winner exists instead of adding another cancellation loop.
func (sc *schedule) planAdaptiveAttempt(current model.Target, primaryP95 time.Duration) adaptiveAttemptPlan {
	if sc.pinned || sc.route == nil || !sc.route.AdaptiveTiming || primaryP95 <= 0 ||
		sc.route.RaceCount != 1 || sc.route.Hedge.Enabled || sc.route.HedgeTarget != nil ||
		sc.runtime == nil || sc.runtime.budget == nil || sc.runtime.deadline.IsZero() ||
		!sc.r.plan.Providers[current.Provider].SafeReplay {
		return adaptiveAttemptPlan{}
	}
	budget := sc.runtime.budget
	if budget.callsExhausted() {
		return adaptiveAttemptPlan{}
	}
	predictedFailure := &model.CallError{Code: "attempt_timeout", Class: model.ErrorTimeout, Status: 504, Scope: model.FailureScopeProvider}
	var reserve time.Duration
	consider := func(route *policy.CompiledRoute, delay time.Duration) {
		if route == nil || !route.Applicable(predictedFailure, 0) {
			return
		}
		for _, cand := range route.Pool {
			if cand == current || sc.runtime.exclude[cand.Provider] || sc.runtime.excludeTargets[targetKey{provider: cand.Provider, model: cand.Model}] ||
				(route.ProviderUnused && sc.runtime.used[cand.Provider]) ||
				(budget.maxCallsPerProvider > 0 && budget.perProvider[cand.Provider] >= budget.maxCallsPerProvider) {
				continue
			}
			if !sc.r.targetEligible(cand.Provider, cand.Model) || len(sc.r.capacity.Available([]model.Target{cand})) == 0 || sc.r.validateTarget(cand) != nil {
				continue
			}
			p95 := sc.r.hedgeLatency.ReadyThreshold(cand.Provider, cand.Model, sc.stream)
			if p95 <= 0 {
				continue
			}
			if limit := sc.r.plan.Providers[cand.Provider].AttemptTimeout; limit > 0 && limit < p95 {
				continue
			}
			predicted := conservativeAttemptLatency(p95)
			if delay > time.Duration(1<<63-1)-predicted {
				continue
			}
			predicted += delay
			if reserve == 0 || predicted < reserve {
				reserve = predicted
			}
		}
	}
	if sc.route.Retry.Attempts > 0 {
		delay := backoffDuration(sc.route.Retry.Backoff, 0)
		if strings.EqualFold(sc.route.Retry.Backoff.Type, "exponential") {
			delay = sc.route.Retry.Backoff.Max.Duration
			if delay <= 0 {
				delay = time.Second
			}
		}
		target := sc.route.RetryTarget
		if sc.route.Retry.Internal {
			target = sc.route
		}
		consider(target, delay)
	}
	consider(sc.route.FallbackTarget, 0)
	if reserve == 0 {
		return adaptiveAttemptPlan{}
	}
	return adaptiveAttemptPlan{after: conservativeAttemptLatency(primaryP95), reserve: reserve}
}

func (sc *schedule) attemptTimeoutFor(providerTimeout time.Duration, safeReplay bool, plan adaptiveAttemptPlan) (time.Duration, bool, bool) {
	if sc.runtime == nil || sc.runtime.deadline.IsZero() {
		if providerTimeout > 0 {
			return providerTimeout, true, false
		}
		return 0, false, false
	}
	remaining := max(time.Millisecond, time.Until(sc.runtime.deadline))
	best, enforced := remaining, false
	if providerTimeout > 0 && providerTimeout < best {
		best, enforced = providerTimeout, true
	}
	// Recheck the remaining time when the branch actually starts. Never shorten
	// a healthy primary merely to reserve time for a backup that cannot fit.
	if safeReplay && plan.after > 0 && plan.reserve > 0 && plan.after < best && plan.reserve < remaining-plan.after {
		return plan.after, true, true
	}
	return best, enforced, false
}

func (sc *schedule) canAffordHedge(fireAt time.Time) bool {
	if sc.route == nil || !sc.route.AdaptiveTiming || sc.runtime == nil || sc.runtime.deadline.IsZero() {
		return true
	}
	remAtFire := sc.runtime.deadline.Sub(fireAt)
	if remAtFire < minAdaptiveAttemptSlice {
		return false
	}
	if sc.hedgeG >= 0 && sc.hedgeG < len(sc.groups) && len(sc.groups[sc.hedgeG]) > 0 {
		return len(sc.r.filterWorthwhileHedgeTargets(sc.groups[sc.hedgeG], sc.stream, remAtFire)) > 0
	}
	return true
}

func (r *Runner) filterWorthwhileHedgeTargets(targets []model.Target, stream bool, remaining time.Duration) []model.Target {
	if len(targets) == 0 || remaining < minAdaptiveAttemptSlice {
		return nil
	}
	out := make([]model.Target, 0, len(targets))
	for _, target := range targets {
		ewma, ok := r.scores.KnownLatencyOfMode(target.Provider, stream)
		if !ok || ewma <= minHedgeWorthLatency || remaining >= ewma/2 {
			out = append(out, target)
		}
	}
	return out
}

func (r *Runner) worthTransitioning(target *policy.CompiledRoute, stream bool, remaining time.Duration) bool {
	if target == nil || remaining <= 0 {
		return false
	}
	if len(target.Pool) == 0 {
		return true
	}
	for _, cand := range target.Pool {
		ewma, ok := r.scores.KnownLatencyOfMode(cand.Provider, stream)
		if !ok || ewma <= minTransitionWorthLatency || remaining >= ewma/4 {
			return true
		}
	}
	return false
}
