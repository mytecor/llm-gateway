package scheduler

import (
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"

	"context"
	"sync/atomic"
	"testing"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

func TestNestedTransitionStopsOnUnknownReplaySafety(t *testing.T) {
	rules := append(policytest.EntryRules([]string{"a"}, 1),
		policytest.RetryRuleHelper("standard", "retry", 2),
		policytest.FallbackRuleHelper("standard", "fallback"),
		policytest.FilterProvider("retry", "b"), policytest.MapRuleHelper("retry", "native-model"), policytest.RaceRuleHelper("retry", 1),
		policytest.FilterProvider("fallback", "c"), policytest.MapRuleHelper("fallback", "native-model"), policytest.RaceRuleHelper("fallback", 1),
	)
	cfg := configtest.RulesConfig(t, 3, rules)
	var a, b, c atomic.Int32
	executor := &fakeExecutor{do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		switch target.Provider {
		case "a":
			a.Add(1)
			return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503, Replay: model.ReplayNotSent}
		case "b":
			b.Add(1)
			return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503, Replay: model.ReplayUnknown}
		default:
			c.Add(1)
			return []byte(`{"ok":true}`), nil
		}
	}}
	runner := NewRunner(cfg.RoutingPlan(), subsystem.NewCatalog(cfg), executor)
	defer runner.Close()
	_, err := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err == nil || err.Replay != model.ReplayUnknown || a.Load() != 1 || b.Load() != 1 || c.Load() != 0 {
		t.Fatalf("unsafe transition: a=%d b=%d c=%d err=%+v", a.Load(), b.Load(), c.Load(), err)
	}
}
