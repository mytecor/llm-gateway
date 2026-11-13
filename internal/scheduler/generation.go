package scheduler

import (
	"context"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

// RequestRun shares one budget across sequential generations of a client
// request. Its methods must be called sequentially, as in structured.Run's
// correction loop.
type RequestRun struct {
	runner   *Runner
	logical  string
	budget   *RequestBudget
	excluded map[string]bool
}

func (r *Runner) NewRequestRun(ctx context.Context, logical string) *RequestRun {
	runtime := newRouteRuntime(r.plan.Models[logical])
	if deadline, ok := ctx.Deadline(); ok && (runtime.budget.deadline.IsZero() || deadline.Before(runtime.budget.deadline)) {
		runtime.budget.deadline = deadline
	}
	return &RequestRun{runner: r, logical: logical, budget: runtime.budget, excluded: make(map[string]bool)}
}

// ExcludeProvider prevents subsequent generations in this RequestRun from
// selecting provider again (for example after truncated or invalid structured
// output from an upstream aggregator).
func (r *RequestRun) ExcludeProvider(provider string) {
	if r == nil || provider == "" {
		return
	}
	if r.excluded == nil {
		r.excluded = make(map[string]bool)
	}
	r.excluded[provider] = true
}

// CanFailoverProvider reports whether another non-excluded, non-cooling
// provider with remaining request budget is reachable along the entry route's
// fallback chain.
func (r *RequestRun) CanFailoverProvider(excludeProvider string) bool {
	if r == nil || r.runner == nil || r.budget == nil || r.budget.expired() || r.budget.callsExhausted() {
		return false
	}
	entry := r.runner.plan.Models[r.logical]
	if entry == nil || entry.Stateful {
		return false
	}
	return r.runner.hasSelectableRoute(entry, excludeProvider, r.excluded, r.budget)
}

func (r *RequestRun) RunGeneration(ctx context.Context, request model.ExecuteRequest) (*model.RunOutcome, *model.CallError) {
	return r.runner.runGeneration(ctx, r.logical, request, r.budget, r.excluded)
}

func (r *RequestRun) RunGenerationStream(ctx context.Context, request model.ExecuteRequest, started time.Time, sink StreamSink) *model.RunOutcome {
	return r.runner.runStreamWithBudget(ctx, r.logical, request, started, sink, true, r.budget, r.excluded)
}

func (r *Runner) hasSelectableRoute(entry *policy.CompiledRoute, excludeExtra string, excluded map[string]bool, budget *RequestBudget) bool {
	seen := make(map[*policy.CompiledRoute]bool)
	for route := entry; route != nil && !seen[route]; route = route.FallbackTarget {
		seen[route] = true
		if r.routeHasSelectableTarget(route, excludeExtra, excluded, budget) {
			return true
		}
	}
	return false
}

func (r *Runner) firstActiveRoute(entry *policy.CompiledRoute, excluded map[string]bool, budget *RequestBudget) *policy.CompiledRoute {
	if len(excluded) == 0 {
		return entry
	}
	seen := make(map[*policy.CompiledRoute]bool)
	for route := entry; route != nil && !seen[route]; route = route.FallbackTarget {
		seen[route] = true
		if r.routeHasSelectableTarget(route, "", excluded, budget) {
			return route
		}
	}
	return entry
}

func (r *Runner) routeHasSelectableTarget(route *policy.CompiledRoute, excludeExtra string, excluded map[string]bool, budget *RequestBudget) bool {
	candidates := make([]model.Target, 0, len(route.Pool))
	for _, target := range route.Pool {
		if target.Provider == excludeExtra || excluded[target.Provider] {
			continue
		}
		if budget != nil && budget.maxCallsPerProvider > 0 && budget.perProvider[target.Provider] >= budget.maxCallsPerProvider {
			continue
		}
		candidates = append(candidates, target)
	}
	if len(candidates) == 0 {
		return false
	}
	return len(r.availableTargets(candidates)) > 0
}

// BeginExchange reserves the native conversation of a compiler-validated fixed
// stateful route. Routing and provider capacity still run for each generation.
func (r *Runner) BeginExchange(ctx context.Context, logical string, request model.ExecuteRequest) (model.Exchange, *model.CallError) {
	entry := r.plan.Models[logical]
	if entry == nil || !entry.Stateful || len(entry.Pool) != 1 {
		return nil, &model.CallError{Class: model.ErrorInvalid, Status: 400, Code: "unsupported_exchange", Scope: model.FailureScopeRequest}
	}
	if executor, ok := r.executor.(interface {
		BeginExchange(context.Context, model.Target, model.ExecuteRequest) (model.Exchange, *model.CallError)
	}); ok {
		return executor.BeginExchange(ctx, entry.Pool[0], request)
	}
	return nil, &model.CallError{Class: model.ErrorInvalid, Status: 400, Code: "unsupported_exchange", Scope: model.FailureScopeRequest}
}

// RunGenerationStream retains the normal routing/stream machinery, but leaves
// client-level observation to the owner of the multi-generation exchange.
// Token and branch observations still belong to the actual provider attempts.
func (r *Runner) RunGenerationStream(ctx context.Context, logical string, request model.ExecuteRequest, started time.Time, sink StreamSink) *model.RunOutcome {
	return r.runStream(ctx, logical, request, started, sink, true)
}
