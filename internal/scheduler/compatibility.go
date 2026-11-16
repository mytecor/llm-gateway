package scheduler

import (
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

func noCompatibleProvider() *model.CallError {
	return &model.CallError{Class: model.ErrorUpstream, Status: 503, Code: "no_compatible_provider", Scope: model.FailureScopeTarget, Replay: model.ReplayNotSent}
}

// Compatibility is a hard boundary, applied before health/balance/availability.
// Cooling fail-open must only ever see the compatible subset.
func (r *Runner) compatiblePool(pool []model.Target, request model.ExecuteRequest) []model.Target {
	result := make([]model.Target, 0, len(pool))
	for _, target := range pool {
		p := r.plan.Providers[target.Provider]
		caps := model.Capabilities{}
		if p.Capabilities != nil {
			caps = *p.Capabilities
		}
		compatible := false
		switch request.Kind {
		case model.RequestSystemOne:
			f := request.SystemOneFeatures
			compatible = caps.SystemOne && !caps.Stateful && (!f.Images || caps.SystemOneImages) && (!f.Box || caps.SystemOneBox) && (!f.LongContext || caps.SystemOneLongContext)
			// Box is unavailable on the older official pinned model, even though
			// the same provider also serves newer aliases.
			if f.Box && target.Model == "jev-1.13.0" {
				compatible = false
			}
		case "", model.RequestChat:
			compatible = p.Capabilities == nil || caps.Chat
		case model.RequestResponses:
			compatible = p.Capabilities == nil || caps.Responses
		}
		if compatible {
			result = append(result, target)
		}
	}
	return result
}
func (r *Runner) hasCompatibleRoute(route *policy.CompiledRoute, request model.ExecuteRequest, seen map[*policy.CompiledRoute]bool) bool {
	if route == nil || seen[route] {
		return false
	}
	seen[route] = true
	return len(r.compatiblePool(route.Pool, request)) > 0 || r.hasCompatibleRoute(route.RetryTarget, request, seen) || r.hasCompatibleRoute(route.FallbackTarget, request, seen) || r.hasCompatibleRoute(route.HedgeTarget, request, seen)
}
