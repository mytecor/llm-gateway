package policy

import "time"

// RuntimePlan contains only routing data needed after startup. It carries no
// credentials, driver configuration, persistence paths or decoding state.
type RuntimePlan struct {
	Models            map[string]*CompiledRoute
	Providers         map[string]RuntimeProvider
	StreamIdleTimeout time.Duration
}

type RuntimeProvider struct {
	Cooldown      time.Duration
	MaxConcurrent int
}

// MaxBalanceWindow covers every reachable subroute as well as entry routes.
func (p RuntimePlan) MaxBalanceWindow() time.Duration {
	var longest time.Duration
	seen := make(map[*CompiledRoute]bool)
	var visit func(*CompiledRoute)
	visit = func(route *CompiledRoute) {
		if route == nil || seen[route] {
			return
		}
		seen[route] = true
		if route.Balance.Window > longest {
			longest = route.Balance.Window
		}
		visit(route.RetryTarget)
		visit(route.FallbackTarget)
		visit(route.HedgeTarget)
	}
	for _, route := range p.Models {
		visit(route)
	}
	return longest
}
