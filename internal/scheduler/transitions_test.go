package scheduler

import (
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"

	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
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

func TestTransitionDistinguishesDeadlineFromCallBudgetExhaustion(t *testing.T) {
	for _, expired := range []bool{true, false} {
		t.Run(fmt.Sprintf("expired=%t", expired), func(t *testing.T) {
			route := &policy.CompiledRoute{Name: "standard", Pool: []model.Target{{Provider: "a", Model: "m"}}}
			runtime := newRouteRuntime(route)
			if expired {
				runtime.budget.deadline = time.Now().Add(-time.Second)
			} else {
				runtime.budget.maxPhysicalCalls = 1
				runtime.budget.calls = 1
			}
			original := &model.CallError{Class: model.ErrorUpstream, Status: 503}
			runner := &Runner{}
			result := runner.followTransition(context.Background(), "standard", route, routeTransition{kind: "retry", target: route, attempts: 1, internal: true}, model.ExecuteRequest{}, false, runtime, &routeOutcome{err: original}, original)
			if expired {
				if result.err == nil || result.err.Code != "route_timeout" || result.err.Status != 504 {
					t.Fatalf("lost deadline error: %+v", result.err)
				}
			} else if result.err != original {
				t.Fatalf("call exhaustion replaced original failure: %+v", result.err)
			}
		})
	}
}

func TestAttemptTimeoutRetriesNextProviderWhenSafeReplay(t *testing.T) {
	rules := append(policytest.EntryRules([]string{"a", "b"}, 1),
		&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 2 * time.Second}},
		policytest.RetryRuleHelper("standard", "retry", 1),
		policytest.FilterProvider("retry", "b"), policytest.MapRuleHelper("retry", "native-model"), policytest.RaceRuleHelper("retry", 1),
	)
	cfg := configtest.RulesConfig(t, 2, rules)
	plan := cfg.RoutingPlan()
	plan.Providers["a"] = policy.RuntimeProvider{AttemptTimeout: 30 * time.Millisecond, SafeReplay: true}
	plan.Providers["b"] = policy.RuntimeProvider{AttemptTimeout: 500 * time.Millisecond, SafeReplay: true}

	var aCalls, bCalls atomic.Int32
	var seenTimeout atomic.Int64
	executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
		switch target.Provider {
		case "a":
			aCalls.Add(1)
			seenTimeout.Store(int64(req.AttemptTimeout))
			<-ctx.Done()
			return nil, &model.CallError{Class: model.ErrorTimeout, Status: 504, Replay: model.ReplayUnknown, Cause: ctx.Err()}
		default:
			bCalls.Add(1)
			return []byte(`{"ok":true}`), nil
		}
	}}
	runner := NewRunner(plan, subsystem.NewCatalog(cfg), executor)
	defer runner.Close()
	out, err := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err != nil {
		t.Fatalf("expected retry to succeed after SafeReplay attempt timeout, got %#v", err)
	}
	if string(out) != `{"ok":true}` || aCalls.Load() != 1 || bCalls.Load() != 1 {
		t.Fatalf("unexpected outcome: body=%s a=%d b=%d", out, aCalls.Load(), bCalls.Load())
	}
	if time.Duration(seenTimeout.Load()) != 30*time.Millisecond {
		t.Fatalf("expected AttemptTimeout=30ms on request, got %s", time.Duration(seenTimeout.Load()))
	}
}

func TestAttemptTimeoutStopsReplayWhenUnsafeReplay(t *testing.T) {
	rules := append(policytest.EntryRules([]string{"a", "b"}, 1),
		&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 2 * time.Second}},
		policytest.RetryRuleHelper("standard", "retry", 1),
		policytest.FilterProvider("retry", "b"), policytest.MapRuleHelper("retry", "native-model"), policytest.RaceRuleHelper("retry", 1),
	)
	cfg := configtest.RulesConfig(t, 2, rules)
	plan := cfg.RoutingPlan()
	plan.Providers["a"] = policy.RuntimeProvider{AttemptTimeout: 30 * time.Millisecond, SafeReplay: false}
	plan.Providers["b"] = policy.RuntimeProvider{AttemptTimeout: 500 * time.Millisecond, SafeReplay: true}

	var aCalls, bCalls atomic.Int32
	executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		switch target.Provider {
		case "a":
			aCalls.Add(1)
			<-ctx.Done()
			return nil, &model.CallError{Class: model.ErrorTimeout, Status: 504, Cause: ctx.Err()}
		default:
			bCalls.Add(1)
			return []byte(`{"ok":true}`), nil
		}
	}}
	runner := NewRunner(plan, subsystem.NewCatalog(cfg), executor)
	defer runner.Close()
	_, err := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err == nil || err.Replay != model.ReplayUnknown || aCalls.Load() != 1 || bCalls.Load() != 0 {
		t.Fatalf("expected non-safe-replay attempt timeout to stop retry: a=%d b=%d err=%#v", aCalls.Load(), bCalls.Load(), err)
	}
}

func TestStreamAttemptTimeoutBeforeMeaningfulEvent(t *testing.T) {
	for _, beforeOpen := range []bool{true, false} {
		for _, safe := range []bool{true, false} {
			t.Run(fmt.Sprintf("before_open=%t/safe_replay=%t", beforeOpen, safe), func(t *testing.T) {
				rules := append(policytest.EntryRules([]string{"a", "b"}, 1),
					&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 2 * time.Second}},
					policytest.RetryRuleHelper("standard", "retry", 1),
					policytest.FilterProvider("retry", "b"), policytest.MapRuleHelper("retry", "native-model"), policytest.RaceRuleHelper("retry", 1),
				)
				cfg := configtest.RulesConfig(t, 2, rules)
				plan := cfg.RoutingPlan()
				plan.Providers["a"] = policy.RuntimeProvider{AttemptTimeout: 30 * time.Millisecond, SafeReplay: safe}
				plan.Providers["b"] = policy.RuntimeProvider{SafeReplay: true}
				var backupCalls atomic.Int32
				cancelled := make(chan struct{})
				executor := &fakeExecutor{stream: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
					if target.Provider == "b" {
						backupCalls.Add(1)
						events := make(chan model.StreamEvent, 1)
						events <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{"content":"backup"}}]}`), Meaningful: true}
						close(events)
						return events, nil
					}
					if beforeOpen {
						<-ctx.Done()
						close(cancelled)
						return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499, Replay: model.ReplayUnknown}
					}
					events := make(chan model.StreamEvent, 1)
					events <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{"role":"assistant"}}]}`)}
					go func() {
						<-ctx.Done()
						close(cancelled)
						close(events)
					}()
					return events, nil
				}}
				runner := NewRunner(plan, subsystem.NewCatalog(cfg), executor)
				defer runner.Close()
				selected, err := runner.SelectStream(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
				if safe {
					if err != nil || selected == nil || selected.Provider != "b" || backupCalls.Load() != 1 {
						t.Fatalf("expected backup after selection timeout: selected=%+v calls=%d err=%+v", selected, backupCalls.Load(), err)
					}
					selected.Cancel()
				} else if err == nil || err.Code != "attempt_timeout" || err.Replay != model.ReplayUnknown || backupCalls.Load() != 0 {
					t.Fatalf("unsafe stream was replayed: calls=%d err=%+v", backupCalls.Load(), err)
				}
				select {
				case <-cancelled:
				case <-time.After(time.Second):
					t.Fatal("timed-out stream retained its context")
				}
			})
		}
	}
}

func TestStreamAttemptTimeoutStopsAfterSelection(t *testing.T) {
	cfg := configtest.RulesConfig(t, 1, policytest.EntryRules([]string{"a"}, 1))
	plan := cfg.RoutingPlan()
	plan.Providers["a"] = policy.RuntimeProvider{AttemptTimeout: 30 * time.Millisecond, SafeReplay: true}
	executor := &fakeExecutor{stream: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		events := make(chan model.StreamEvent, 1)
		events <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{"content":"selected"}}]}`), Meaningful: true}
		go func() {
			defer close(events)
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			select {
			case events <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{},"finish_reason":"stop"}]}`)}:
			case <-ctx.Done():
			}
		}()
		return events, nil
	}}
	runner := NewRunner(plan, subsystem.NewCatalog(cfg), executor)
	defer runner.Close()
	selected, err := runner.SelectStream(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Cancel()
	select {
	case event, ok := <-selected.Remaining:
		if !ok || event.Err != nil {
			t.Fatalf("selection timeout cancelled the winning stream: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("winning stream did not finish")
	}
}
