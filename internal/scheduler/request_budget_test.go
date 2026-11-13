package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

func declarativeTestConfig() config.Config {
	zero := 0
	return config.Config{
		Providers: []provider.Provider{
			{ID: "gonka-1", Priority: 30, Labels: map[string]string{"pool": "gonka"}, Config: map[string]any{"inference_url": "https://gonka-1.invalid"}},
			{ID: "gonka-2", Priority: 20, Labels: map[string]string{"pool": "gonka"}, Config: map[string]any{"inference_url": "https://gonka-2.invalid"}},
			{ID: "gonka-3", Priority: 10, Labels: map[string]string{"pool": "gonka"}, Config: map[string]any{"inference_url": "https://gonka-3.invalid"}},
			{ID: "hyperfusion", Priority: 5, Labels: map[string]string{"pool": "backup"}, Config: map[string]any{"inference_url": "https://hyperfusion.invalid"}},
		},
		Models: map[string]policy.ModelSpec{
			"standard": {
				Budget: &policy.BudgetSpec{
					MaxCalls:            3,
					MaxInFlight:         2,
					MaxHedges:           &zero,
					MaxCallsPerProvider: 1,
				},
				Tiers: []policy.TierSpec{
					{Targets: []policy.TargetSpec{{Select: map[string]string{"pool": "gonka"}, Native: "ds-v4"}}},
					{Targets: []policy.TargetSpec{{Provider: "hyperfusion", Native: "gonka/ds-v4"}}},
				},
			},
		},
	}
}

func TestRequestBudgetCapsPhysicalCallsAcrossTiersAndPreservesError(t *testing.T) {
	compiled, err := config.PrepareAndCompile(declarativeTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	executor := &fakeExecutor{do: func(_ context.Context, _ model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		calls.Add(1)
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503, Scope: model.FailureScopeProvider}
	}}
	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
	defer runner.Close()

	_, callErr := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr == nil || callErr.Class != model.ErrorUpstream {
		t.Fatalf("expected preserved 503 ErrorUpstream, got %#v", callErr)
	}
	if calls.Load() != 3 {
		t.Fatalf("RequestBudget MaxCalls=3 violated: got %d physical calls", calls.Load())
	}
}

func TestDeclarativeTiersFallbackOnFailureAndStopOnRequestBadRequest(t *testing.T) {
	cfg := declarativeTestConfig()
	cfg.Models["standard"].Budget.MaxCalls = 6
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Case 1: All tier 0 providers return 503 -> falls back to tier 1 hyperfusion and succeeds.
	var backupCalls atomic.Int32
	execFallback := &fakeExecutor{do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "hyperfusion" {
			backupCalls.Add(1)
			return []byte(`{"tier":"backup"}`), nil
		}
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503, Scope: model.FailureScopeProvider}
	}}
	runner1 := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), execFallback)
	defer runner1.Close()
	out, callErr := runner1.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil || string(out) != `{"tier":"backup"}` || backupCalls.Load() != 1 {
		t.Fatalf("expected tier 1 backup success, out=%s backupCalls=%d err=%#v", out, backupCalls.Load(), callErr)
	}

	// Case 2: Request-scoped 400 stops immediately without trying other targets or tier 1.
	var totalCalls atomic.Int32
	execBadReq := &fakeExecutor{do: func(_ context.Context, _ model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		totalCalls.Add(1)
		return nil, &model.CallError{Class: model.ErrorInvalid, Status: 400, Scope: model.FailureScopeRequest}
	}}
	runner2 := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), execBadReq)
	defer runner2.Close()
	_, callErr = runner2.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr == nil || callErr.Scope != model.FailureScopeRequest || totalCalls.Load() != 1 {
		t.Fatalf("expected FailureScopeRequest to stop after 1 call, calls=%d err=%#v", totalCalls.Load(), callErr)
	}
}

func TestRequestBudgetCapsMaxHedgesAcrossRetries(t *testing.T) {
	one := 1
	cfg := declarativeTestConfig()
	cfg.Models["standard"] = policy.ModelSpec{
		Timeout: config.Duration{Duration: 2 * time.Second},
		Budget: &policy.BudgetSpec{
			MaxCalls:            4,
			MaxInFlight:         2,
			MaxHedges:           &one,
			MaxCallsPerProvider: 1,
		},
		Tiers: []policy.TierSpec{
			{Targets: []policy.TargetSpec{
				{Provider: "gonka-1", Native: "ds-v4"},
				{Provider: "gonka-2", Native: "ds-v4"},
				{Provider: "gonka-3", Native: "ds-v4"},
			}},
		},
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Seed a fast baseline observation so dynamic hedge timer fires quickly (~15ms).
	var slowCalls atomic.Int32
	executor := &fakeExecutor{do: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		if slowCalls.Add(1) <= 2 {
			// Primary fails slowly after hedge could arm/launch; hedge also fails.
			select {
			case <-time.After(35 * time.Millisecond):
				return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503, Scope: model.FailureScopeProvider}
			case <-ctx.Done():
				return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499}
			}
		}
		return []byte(`{"ok":true}`), nil
	}}

	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
	defer runner.Close()
	runner.hedgeLatency.Begin("gonka-1", "ds-v4", false).Success(10 * time.Millisecond)

	out, callErr := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil || string(out) != `{"ok":true}` {
		t.Fatalf("expected third candidate to succeed within budget, out=%s err=%#v", out, callErr)
	}
	if slowCalls.Load() > 4 {
		t.Fatalf("exceeded RequestBudget MaxCalls=4: %d", slowCalls.Load())
	}
}
