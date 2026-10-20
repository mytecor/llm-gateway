package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

func TestProviderCapacityHeldForSelectedStreamLifetime(t *testing.T) {
	cfg := policy.TestConfig()
	cfg.Providers = cfg.Providers[:1]
	cfg.Providers[0].MaxConcurrent = 1
	cfg.Rules = []policy.Rule{
		policy.FilterModel("standard", "standard"),
		policy.MapRuleDirect("standard", []string{"a"}, "model-x", 0),
		policy.RankRuleHelper("standard"),
		policy.RaceRuleHelper("standard", 1),
	}
	compiled, err := policy.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	executor := &fakeExecutor{stream: func(ctx context.Context, target model.Target, req model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		calls.Add(1)
		stream := make(chan model.StreamEvent, 1)
		stream <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{"content":"token"}}]}`), Meaningful: true}
		go func() {
			<-ctx.Done()
			close(stream)
		}()
		return stream, nil
	}}
	runner := NewRunner(compiled, subsystem.NewCatalog(compiled), executor)

	first, callErr := runner.SelectStream(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil {
		t.Fatal(callErr)
	}
	secondResult := make(chan *SelectedStream, 1)
	secondError := make(chan *model.CallError, 1)
	go func() {
		selected, err := runner.SelectStream(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
		secondResult <- selected
		secondError <- err
	}()

	select {
	case <-secondResult:
		t.Fatal("second stream exceeded provider max_concurrent while first stream was active")
	case <-time.After(40 * time.Millisecond):
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one upstream stream before release, got %d", calls.Load())
	}

	first.Cancel()
	select {
	case second := <-secondResult:
		if err := <-secondError; err != nil {
			t.Fatal(err)
		}
		if second == nil {
			t.Fatal("second stream was not selected after capacity release")
		}
		second.Cancel()
	case <-time.After(time.Second):
		t.Fatal("provider capacity release did not wake the waiting scheduler")
	}
}

func TestInternalRetryExcludesEveryFailedRaceTarget(t *testing.T) {
	cfg := policy.TestConfig()
	cfg.Rules = []policy.Rule{
		policy.FilterModel("standard", "standard"),
		policy.MapRuleDirect("standard", []string{"a", "b", "c"}, "model-x", 0),
		policy.RankRuleHelper("standard"),
		policy.RaceRuleHelper("standard", 2),
		&policy.RetryRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionRetry}, Attempts: 1},
	}
	compiled, err := policy.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	calls := map[string]int{}
	executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
		mu.Lock()
		calls[target.Provider]++
		mu.Unlock()
		if target.Provider == "c" {
			return []byte(`{"choices":[{"message":{"content":"ok"}}]}`), nil
		}
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503, Scope: model.FailureScopeProvider}
	}}
	runner := NewRunner(compiled, subsystem.NewCatalog(compiled), executor)
	if _, callErr := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat}); callErr != nil {
		t.Fatalf("retry did not reach unused target c: %v", callErr)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["a"] != 1 || calls["b"] != 1 || calls["c"] != 1 {
		t.Fatalf("expected one call per target, got %#v", calls)
	}
}

func TestInternalHedgeReevaluatesCapacityAtFireTime(t *testing.T) {
	cfg := policy.TestConfig()
	for i := range cfg.Providers {
		cfg.Providers[i].MaxConcurrent = 1
	}
	cfg.Rules = []policy.Rule{
		policy.FilterModel("standard", "standard"),
		policy.MapRuleDirect("standard", []string{"a", "b", "c"}, "model-x", 0),
		policy.RankRuleHelper("standard"),
		policy.RaceRuleHelper("standard", 1),
		&policy.HedgeRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionHedge}, After: policy.Duration{Duration: 40 * time.Millisecond}},
	}
	compiled, err := policy.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	startedA := make(chan struct{})
	executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "a" {
			close(startedA)
			<-ctx.Done()
			return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
		}
		if target.Provider == "c" {
			return []byte(`{"choices":[{"message":{"content":"hedged"}}]}`), nil
		}
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503, Scope: model.FailureScopeProvider}
	}}
	runner := NewRunner(compiled, subsystem.NewCatalog(compiled), executor)
	done := make(chan *model.CallError, 1)
	go func() {
		_, callErr := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
		done <- callErr
	}()
	<-startedA
	if !runner.Scores().TryAcquireProvider("b") {
		t.Fatal("failed to occupy provider b before hedge fired")
	}
	defer runner.Scores().DecrInFlight("b")

	select {
	case callErr := <-done:
		if callErr != nil {
			t.Fatalf("dynamic hedge failed: %v", callErr)
		}
	case <-time.After(time.Second):
		t.Fatal("hedge did not re-evaluate capacity and dispatch provider c")
	}
}

func TestScopedInvalidUpstreamFailureIsReschedulable(t *testing.T) {
	if !isReschedulableError(&model.CallError{Class: model.ErrorInvalid, Status: 400, Scope: model.FailureScopeTarget}) {
		t.Fatal("target-scoped unsupported parameters must be reschedulable")
	}
	if isReschedulableError(&model.CallError{Class: model.ErrorInvalid, Status: 400, Scope: model.FailureScopeRequest}) {
		t.Fatal("request-scoped malformed input must remain terminal")
	}
}

func TestStreamingInternalRetryHidesPreWinnerFailure(t *testing.T) {
	cfg := policy.TestConfig()
	cfg.Rules = []policy.Rule{
		policy.FilterModel("standard", "standard"),
		policy.MapRuleDirect("standard", []string{"a", "b"}, "model-x", 0),
		policy.RankRuleHelper("standard"),
		policy.RaceRuleHelper("standard", 1),
		&policy.RetryRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionRetry}},
		&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 10 * time.Second}},
	}
	compiled, err := policy.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, req model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			stream := make(chan model.StreamEvent, 2)
			if target.Provider == "a" {
				stream <- model.StreamEvent{Err: &model.CallError{Class: model.ErrorUpstream, Status: 500}}
				close(stream)
				return stream, nil
			}
			go func() {
				stream <- model.StreamEvent{
					Data:       []byte(`data: {"choices":[{"delta":{"content":"token-b"}}]}`),
					Meaningful: true,
				}
				stream <- model.StreamEvent{
					Data: []byte(`data: [DONE]`),
					Done: true,
				}
				close(stream)
			}()
			return stream, nil
		},
	}

	runner := NewRunner(compiled, subsystem.NewCatalog(compiled), executor)
	sel, callErr := runner.SelectStream(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil {
		t.Fatalf("expected SelectStream to succeed via retry, got: class=%q status=%d cause=%v", callErr.Class, callErr.Status, callErr.Cause)
	}
	if sel.Provider != "b" {
		t.Fatalf("expected winner b, got %s", sel.Provider)
	}
	if sel.Release != nil {
		sel.Release()
	}
}
