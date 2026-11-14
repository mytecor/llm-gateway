package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

// These tests construct only a runtime plan: no driver discovery, credentials
// or application configuration are needed to exercise a client run.
func budgetPlan(providers ...string) policy.RuntimePlan {
	route := &policy.CompiledRoute{
		Name: "standard", RaceCount: 1,
		Admission: policy.AdmissionConfig{MaxInFlight: 1},
		Continue:  policy.ContinueConfig{Enabled: true, Idle: time.Second, Wait: time.Millisecond},
	}
	plan := policy.RuntimePlan{Models: map[string]*policy.CompiledRoute{"standard": route}, Providers: map[string]policy.RuntimeProvider{}}
	for _, id := range providers {
		route.Pool = append(route.Pool, model.Target{Provider: id, Model: "native"})
		plan.Providers[id] = policy.RuntimeProvider{Cooldown: time.Second}
	}
	return plan
}

func TestClientRunPreservesCallBudgetAcrossContinuations(t *testing.T) {
	for _, perProvider := range []bool{false, true} {
		t.Run(map[bool]string{false: "total", true: "per-provider"}[perProvider], func(t *testing.T) {
			plan := budgetPlan("a", "b")
			if perProvider {
				plan = budgetPlan("a")
				plan.Models["standard"].Semaphore.MaxCallsPerProvider = 1
				plan.Models["standard"].Continue.Retries = 2
			} else {
				plan.Models["standard"].Semaphore.MaxCalls = 1
			}
			var calls atomic.Int32
			exec := &fakeExecutor{stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
				calls.Add(1)
				return completionStream("partial", false), nil
			}}
			runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
			defer runner.Close()
			sink := &recordingStreamSink{}
			runner.RunStream(context.Background(), "standard", streamRunRequest(), time.Now(), sink)
			if calls.Load() != 1 || !sink.failed || sink.completed {
				t.Fatalf("continuation replenished call budget: calls=%d sink=%+v", calls.Load(), sink)
			}
		})
	}
}

func TestClientRunDoesNotExtendDeadlineAfterWinnerBreaks(t *testing.T) {
	plan := budgetPlan("a", "b")
	plan.Models["standard"].RouteTimeout = 20 * time.Millisecond
	plan.Models["standard"].Continue.Idle = time.Second
	var calls atomic.Int32
	exec := &fakeExecutor{stream: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		calls.Add(1)
		ch := make(chan model.StreamEvent, 1)
		ch <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{"content":"partial"}}]}`), Meaningful: true}
		go func() {
			defer close(ch)
			select {
			case <-ctx.Done():
			case <-time.After(60 * time.Millisecond):
			}
		}()
		return ch, nil
	}}
	runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
	defer runner.Close()
	sink := &recordingStreamSink{}
	runner.RunStream(context.Background(), "standard", streamRunRequest(), time.Now(), sink)
	if calls.Load() != 1 || !sink.failed || sink.failure.Class != model.ErrorTimeout {
		t.Fatalf("continuation extended selection deadline: calls=%d error=%+v", calls.Load(), sink.failure)
	}
}

func TestClientRunDeadlineBoundsRecoveryHold(t *testing.T) {
	plan := budgetPlan("a")
	plan.Models["standard"].RouteTimeout = 30 * time.Millisecond
	plan.Models["standard"].Continue.Wait = time.Second
	exec := &fakeExecutor{stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		return completionStream("partial", false), nil
	}}
	runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
	defer runner.Close()
	sink := &recordingStreamSink{}
	started := time.Now()
	runner.RunStream(context.Background(), "standard", streamRunRequest(), started, sink)
	if time.Since(started) > 500*time.Millisecond || !sink.failed || sink.failure.Class != model.ErrorTimeout {
		t.Fatalf("hold ignored original deadline: elapsed=%s error=%+v", time.Since(started), sink.failure)
	}
}

func TestContinuationBufferLimitKeepsOriginalOutput(t *testing.T) {
	plan := budgetPlan("a", "b")
	plan.Models["standard"].Continue.MaxBufferBytes = 4
	var calls atomic.Int32
	exec := &fakeExecutor{stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		calls.Add(1)
		return completionStream("long answer", false), nil
	}}
	runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
	defer runner.Close()
	sink := &recordingStreamSink{}
	runner.RunStream(context.Background(), "standard", streamRunRequest(), time.Now(), sink)
	if calls.Load() != 1 || !sink.failed || len(sink.events) != 1 {
		t.Fatalf("overflow replayed incomplete context or dropped output: calls=%d sink=%+v", calls.Load(), sink)
	}
}

func TestLateLiveStreamFailureRetainsOriginalClass(t *testing.T) {
	plan := budgetPlan("a")
	plan.Models["standard"].Continue.Enabled = false
	plan.Models["standard"].RouteTimeout = 20 * time.Millisecond
	plan.Models["standard"].StreamIdleTimeout = time.Second
	exec := &fakeExecutor{stream: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		ch := make(chan model.StreamEvent, 2)
		ch <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{"content":"partial"}}]}`), Meaningful: true}
		go func() {
			defer close(ch)
			select {
			case <-ctx.Done():
			case <-time.After(60 * time.Millisecond):
				ch <- model.StreamEvent{Err: &model.CallError{Class: model.ErrorConnection, Status: 502}}
			}
		}()
		return ch, nil
	}}
	runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
	defer runner.Close()
	sink := &recordingStreamSink{}
	runner.RunStream(context.Background(), "standard", streamRunRequest(), time.Now(), sink)
	if !sink.failed || sink.failure.Class != model.ErrorConnection {
		t.Fatalf("selection deadline changed a later live stream failure: %+v", sink.failure)
	}
}

func TestRecoveryReleasesBrokenProviderBeforeReselecting(t *testing.T) {
	oldRecheck := policy.ContinueRecoveryRecheck
	policy.ContinueRecoveryRecheck = 5 * time.Millisecond
	t.Cleanup(func() { policy.ContinueRecoveryRecheck = oldRecheck })
	plan := budgetPlan("a")
	plan.Providers["a"] = policy.RuntimeProvider{MaxConcurrent: 1, Cooldown: time.Millisecond}
	plan.Models["standard"].Continue.Wait = 100 * time.Millisecond
	var calls atomic.Int32
	exec := &fakeExecutor{stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		return completionStream("part", calls.Add(1) > 1), nil
	}}
	runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
	defer runner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sink := &recordingStreamSink{}
	runner.RunStream(ctx, "standard", streamRunRequest(), time.Now(), sink)
	if calls.Load() != 2 || !sink.completed || sink.failed {
		t.Fatalf("broken stream held the only provider permit: calls=%d sink=%+v", calls.Load(), sink)
	}
	if runner.capacity.Count("a") != 0 {
		t.Fatal("recovery leaked provider permit")
	}
}

func TestStructuredFailoverSkipsPerProviderBudgetExhaustedTarget(t *testing.T) {
	plan := budgetPlan("a", "b")
	plan.Models["standard"].Semaphore.MaxCalls = 4
	plan.Models["standard"].Semaphore.MaxCallsPerProvider = 1
	plan.Models["standard"].Balance = policy.BalanceConfig{Enabled: true, Strategy: "expected-ttft", Window: time.Minute, ErrorBudget: 0.2}
	counts := map[string]int{}
	exec := &fakeExecutor{
		do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
			counts[target.Provider]++
			return []byte(`{"id":"ok","choices":[{"finish_reason":"stop","message":{"content":"{\"ok\":true}"}}]}`), nil
		},
	}
	runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
	defer runner.Close()

	run := runner.NewRequestRun(context.Background(), "standard")
	first, callErr := run.RunGeneration(context.Background(), model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil || first == nil {
		t.Fatalf("first generation failed: outcome=%+v err=%v", first, callErr)
	}
	other := "b"
	if first.Provider == "b" {
		other = "a"
	}
	// Clear exclusions while first.Provider still has 1/1 per-provider budget
	// spent; the dynamic pool must skip first.Provider and route to other.
	run.excluded = map[string]bool{}
	second, callErr := run.RunGeneration(context.Background(), model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil || second == nil || second.Provider != other {
		t.Fatalf("expected generation to route to %q instead of exhausted %q, got outcome=%+v err=%v", other, first.Provider, second, callErr)
	}
	if counts[first.Provider] != 1 || counts[other] != 1 {
		t.Fatalf("unexpected provider call counts: %+v", counts)
	}
}
