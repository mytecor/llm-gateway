package scheduler

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
)

func TestDeclarativeHedgeDelayFiresBeforeWarmup(t *testing.T) {
	cfg := configtest.TestConfig()
	cfg.Rules = nil
	cfg.Models = map[string]policy.ModelSpec{
		"standard": {
			Profile:    "high-reliability",
			HedgeDelay: policy.Duration{Duration: 30 * time.Millisecond},
			Tiers: []policy.TierSpec{{
				Targets: []policy.TargetSpec{
					{Provider: "a", Native: "model-x"},
					{Provider: "b", Native: "model-x"},
				},
			}},
		},
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var callsA, callsB atomic.Int32
	executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "a" {
			callsA.Add(1)
			<-ctx.Done()
			return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
		}
		callsB.Add(1)
		return []byte(`{"choices":[{"message":{"content":"ok-b"}}]}`), nil
	}}
	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)

	started := time.Now()
	body, callErr := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil {
		t.Fatalf("cold hedge failed: %v", callErr)
	}
	if !strings.Contains(string(body), "ok-b") {
		t.Fatalf("expected hedged winner b, got %s", string(body))
	}
	if elapsed := time.Since(started); elapsed < 25*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("expected hedge near 30ms delay, took %v", elapsed)
	}
	if callsA.Load() != 1 || callsB.Load() != 1 {
		t.Fatalf("expected 1 call each on a and b, got a=%d b=%d", callsA.Load(), callsB.Load())
	}
}

func TestDeclarativeHedgeDelayCapsHighLearnedThreshold(t *testing.T) {
	cfg := configtest.TestConfig()
	cfg.Rules = nil
	cfg.Models = map[string]policy.ModelSpec{
		"standard": {
			Profile:    "high-reliability",
			HedgeDelay: policy.Duration{Duration: 30 * time.Millisecond},
			Tiers: []policy.TierSpec{{
				Targets: []policy.TargetSpec{
					{Provider: "a", Native: "model-x"},
					{Provider: "b", Native: "model-x"},
				},
			}},
		},
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}

	executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "a" {
			<-ctx.Done()
			return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
		}
		return []byte(`{"choices":[{"message":{"content":"ok-b"}}]}`), nil
	}}
	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
	warmHedge(runner, "model-x", false, 10*time.Second, "a")

	started := time.Now()
	body, callErr := runner.Run(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil {
		t.Fatalf("capped hedge failed: %v", callErr)
	}
	if !strings.Contains(string(body), "ok-b") {
		t.Fatalf("expected hedged winner b, got %s", string(body))
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("hedge waited for learned 10s threshold instead of 30ms cap: %v", elapsed)
	}
}
