package scheduler

import (
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"

	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

// --- balance runtime wiring -------------------------------------------------

// balanceEntryRules builds an entry route with a balance action before race:
// filter model → filter provider → map → rank → balance → race(count=1).
func balanceEntryRules(providers []string) []policy.Rule {
	return []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", providers...),
		policytest.MapRuleHelper("standard", "native-model"),
		policytest.RankRuleHelper("standard"),
		policytest.BalanceRuleCustom("standard", func(r *policy.BalanceRule) { r.Strategy = "round_robin" }),
		policytest.RaceRuleHelper("standard", 1),
	}
}

// TestBalanceRequestServedByBalancedProvider verifies that with race count 1
// the balance action's runtime choice is the only provider executed, and that
// round_robin distributes consecutive requests over the healthy pool.
func TestBalanceRequestServedByBalancedProvider(t *testing.T) {
	compiled := configtest.RulesConfig(t, 3, balanceEntryRules([]string{"a", "b", "c"}))
	var mu sync.Mutex
	called := []string{}
	executor := &fakeExecutor{do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		mu.Lock()
		called = append(called, target.Provider)
		mu.Unlock()
		return successBody(target.Provider), nil
	}}
	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
	winners := map[string]int{}
	for range 6 {
		body, err := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		winner, _ := payload["winner"].(string)
		winners[winner]++
	}
	mu.Lock()
	defer mu.Unlock()
	if len(called) != 6 {
		t.Fatalf("expected exactly one provider per request, got %#v", called)
	}
	if len(winners) < 2 {
		t.Fatalf("round_robin must distribute over the pool, got %#v", winners)
	}
}

// TestBalanceExcludesUnhealthyProvider verifies that a provider over its error
// budget is not balanced to the front while the adaptive strategy is active.
func TestBalanceExcludesUnhealthyProvider(t *testing.T) {
	compiled := configtest.RulesConfig(t, 3, []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a", "b", "c"),
		policytest.MapRuleHelper("standard", "native-model"),
		policytest.RankRuleHelper("standard"),
		policytest.BalanceRuleCustom("standard", func(r *policy.BalanceRule) {
			r.Strategy = "adaptive"
			r.Window = policy.Duration{Duration: 5 * time.Minute}
			r.ErrorBudget = 0.2
		}),
		policytest.RaceRuleHelper("standard", 1),
	})
	executor := &fakeExecutor{do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		return successBody(target.Provider), nil
	}}
	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
	// Pre-populate the health store: a and b are over the error budget, c is
	// healthy. The adaptive strategy must then narrow the balance choice to c.
	for _, provider := range []string{"a", "b"} {
		for range 4 {
			runner.scores.Observe(provider, &model.CallError{Class: model.ErrorUpstream, Status: 503}, 0)
		}
		runner.scores.Observe(provider, nil, time.Second)
	}
	// Now c must be the only healthy provider and get balanced to the front.
	seen := map[string]int{}
	for range 4 {
		body, err := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		seen[payload["winner"].(string)]++
	}
	if len(seen) != 1 || seen["c"] != 4 {
		t.Fatalf("unhealthy providers must be excluded, got %#v", seen)
	}
}
