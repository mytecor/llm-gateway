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

const schedulerDecision = `{"model":"jev","state":"fixture","questions":{"q":{"type":"noul"}}}`
const schedulerAnswer = `{"model":"jev-1.14.0","answers":{"q":{"type":"noul","noul":0.7}}}`

func TestSystemOneCompatibilityAcrossRecoveryGraph(t *testing.T) {
	for _, scenario := range []string{"cooling", "retry", "fallback", "hedge", "hedge-only", "no-compatible"} {
		t.Run(scenario, func(t *testing.T) {
			basic := &model.Capabilities{SystemOne: true, SafeReplay: true, SingleAttempt: true}
			full := &model.Capabilities{SystemOne: true, SystemOneImages: true, SystemOneBox: true, SystemOneLongContext: true, SafeReplay: true, SingleAttempt: true}
			entry := &policy.CompiledRoute{Name: "jev", Pool: []model.Target{{Provider: "official-a", Model: "jev-latest"}, {Provider: "gonka", Model: "jev"}}, RaceCount: 1, RouteTimeout: time.Second}
			recovery := &policy.CompiledRoute{Name: "recovery", Pool: []model.Target{{Provider: "gonka", Model: "jev"}, {Provider: "official-b", Model: "jev-latest"}}, RaceCount: 1}
			switch scenario {
			case "retry":
				entry.Retry.Attempts = 1
				entry.RetryTarget = recovery
			case "fallback":
				entry.FallbackTarget = recovery
			case "hedge":
				entry.Hedge = policy.HedgeConfig{Enabled: true, Target: "recovery", Delay: time.Millisecond}
				entry.HedgeTarget = recovery
			case "hedge-only":
				entry.Pool = []model.Target{{Provider: "gonka", Model: "jev"}}
				entry.Hedge = policy.HedgeConfig{Enabled: true, Target: "recovery", Delay: time.Millisecond}
				entry.HedgeTarget = recovery
			case "no-compatible":
				entry.Pool = []model.Target{{Provider: "gonka", Model: "jev"}}
			}
			var unsupported, first, second atomic.Int32
			exec := &fakeExecutor{do: func(ctx context.Context, target model.Target, request model.ExecuteRequest) ([]byte, *model.CallError) {
				switch target.Provider {
				case "gonka":
					unsupported.Add(1)
				case "official-a":
					first.Add(1)
					switch scenario {
					case "retry", "fallback":
						return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503, Scope: model.FailureScopeProvider, Replay: model.ReplayCompleted}
					case "hedge":
						<-ctx.Done()
						return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499}
					}
				case "official-b":
					second.Add(1)
				}
				return []byte(schedulerAnswer), nil
			}}
			plan := policy.RuntimePlan{Models: map[string]*policy.CompiledRoute{"jev": entry}, Providers: map[string]policy.RuntimeProvider{"gonka": {Capabilities: basic}, "official-a": {Capabilities: full}, "official-b": {Capabilities: full}}}
			runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
			defer runner.Close()
			if scenario == "cooling" {
				runner.cooling[cooldownKey{provider: "official-a", model: "jev-latest"}] = time.Now().Add(time.Minute)
				runner.cooling[cooldownKey{provider: "gonka", model: "jev"}] = time.Now().Add(time.Minute)
			}
			_, err := runner.Run(context.Background(), "jev", model.ExecuteRequest{Kind: model.RequestSystemOne, Body: []byte(schedulerDecision), SystemOneFeatures: model.SystemOneFeatures{LongContext: true}})
			if unsupported.Load() != 0 {
				t.Fatal("incompatible target reached executor")
			}
			if scenario == "no-compatible" {
				if err == nil || err.Status != 503 || err.Code != "no_compatible_provider" {
					t.Fatalf("wrong compatibility failure: %+v", err)
				}
			} else if err != nil {
				t.Fatalf("recovery failed: %+v first=%d second=%d", err, first.Load(), second.Load())
			}
			if scenario == "retry" || scenario == "fallback" || scenario == "hedge" || scenario == "hedge-only" {
				if second.Load() != 1 {
					t.Fatalf("compatible recovery not used: %d", second.Load())
				}
			}
		})
	}
}
func TestSystemOneInvalidCustomDriverAnswerCannotWin(t *testing.T) {
	caps := &model.Capabilities{SystemOne: true}
	route := &policy.CompiledRoute{Name: "jev", Pool: []model.Target{{Provider: "bad", Model: "jev"}, {Provider: "good", Model: "jev"}}, RaceCount: 2, RouteTimeout: time.Second}
	exec := &fakeExecutor{do: func(ctx context.Context, target model.Target, request model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "bad" {
			return []byte(`{"model":"jev","answers":{"other":{"type":"noul","noul":0.9}}}`), nil
		}
		return []byte(schedulerAnswer), nil
	}}
	runner := NewRunner(policy.RuntimePlan{Models: map[string]*policy.CompiledRoute{"jev": route}, Providers: map[string]policy.RuntimeProvider{"bad": {Capabilities: caps}, "good": {Capabilities: caps}}}, subsystem.NewCatalogWithSources(nil), exec)
	defer runner.Close()
	result, err := runner.RunWithResult(context.Background(), "jev", model.ExecuteRequest{Kind: model.RequestSystemOne, Body: []byte(schedulerDecision)})
	if err != nil || result.Provider != "good" {
		t.Fatalf("invalid answer won: %+v %+v", result, err)
	}
}
