package policy

import (
	"fmt"

	"github.com/mytecor/llm-gateway/internal/model"
)

func (p Provider) Capabilities() model.Capabilities {
	if p.capabilities != (model.Capabilities{}) {
		return p.capabilities
	}
	return model.Capabilities{Chat: true, Responses: true, Streaming: true, Tools: true, SafeReplay: true}
}

// Stateful work cannot be speculated or migrated without executor guarantees.
// Check the entire graph, including transitions from otherwise stateless routes.
func validateSessionRoutes(routes map[string]*CompiledRoute, providers map[string]Provider) error {
	for _, route := range routes {
		for _, target := range route.Pool {
			if providers[target.Provider].Capabilities().Stateful {
				route.Stateful = true
			}
		}
		if !route.Stateful {
			continue
		}
		if len(route.Pool) != 1 || route.RaceCount != 1 || route.Retry.Attempts > 0 || route.Fallback.Target != "" || route.Hedge.After > 0 || route.Hedge.Target != "" || route.Continue.Enabled || route.Repetition.Enabled {
			return fmt.Errorf("route %q: stateful executors require one target, race count 1 and no retry/fallback/hedge/continue/repetition", route.Name)
		}
		for _, parent := range routes {
			if parent.RetryTarget == route || parent.FallbackTarget == route || parent.HedgeTarget == route {
				return fmt.Errorf("route %q: a stateful executor must be an entry route, not a retry/fallback/hedge target", route.Name)
			}
		}
	}
	return nil
}
