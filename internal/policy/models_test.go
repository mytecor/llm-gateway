package policy

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/executor/external"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/provider"
)

func testDeclarativeProviders() map[string]provider.Provider {
	return map[string]provider.Provider{
		"gonka-b": {
			ID:       "gonka-b",
			Priority: 10,
			Labels:   map[string]string{"pool": "gonka", "region": "eu"},
		},
		"gonka-a": {
			ID:       "gonka-a",
			Priority: 20,
			Labels:   map[string]string{"pool": "gonka", "region": "us"},
		},
		"hyperfusion": {
			ID:       "hyperfusion",
			Priority: 5,
			Labels:   map[string]string{"pool": "backup"},
		},
		"codex-session": {
			ID: "codex-session",
			Description: &external.Description{
				Capabilities: model.Capabilities{Stateful: true, Chat: true},
			},
		},
	}
}

func TestCompileModelsWithTiersAndLabelSelectors(t *testing.T) {
	providers := testDeclarativeProviders()
	models := map[string]ModelSpec{
		"standard": {
			Tiers: []TierSpec{
				{Targets: []TargetSpec{
					{Select: map[string]string{"pool": "gonka"}, Native: "deepseek-v4"},
				}},
				{Targets: []TargetSpec{
					{Provider: "hyperfusion", Native: "gonka/deepseek-v4"},
				}},
			},
		},
	}

	compiled, err := CompileWithModels(nil, models, providers)
	if err != nil {
		t.Fatal(err)
	}
	entry := compiled.Models["standard"]
	if entry == nil || !entry.Entry || entry.ModelEq != "standard" {
		t.Fatalf("unexpected entry route: %#v", entry)
	}
	if len(entry.Pool) != 2 || entry.Pool[0].Provider != "gonka-a" || entry.Pool[1].Provider != "gonka-b" {
		t.Fatalf("tier 0 pool order mismatch (want Priority desc): %#v", entry.Pool)
	}
	if !entry.Retry.Internal || entry.Retry.Attempts != 1 {
		t.Fatalf("expected internal retry across tier 0 pool, got %#v", entry.Retry)
	}
	if !entry.Balance.Enabled || entry.Balance.Strategy != "expected-ttft" {
		t.Fatalf("expected expected-ttft balance on multi-provider tier, got %#v", entry.Balance)
	}
	if !entry.Hedge.Enabled || !entry.Hedge.Internal {
		t.Fatalf("expected internal hedge on balanced multi-provider tier, got %#v", entry.Hedge)
	}
	if entry.Continue.Enabled {
		t.Fatal("continue must remain disabled unless safe_continue=true is explicitly set")
	}
	if entry.Semaphore.MaxCalls != 4 || entry.Semaphore.MaxInFlight != 2 || entry.Semaphore.MaxHedges != 1 {
		t.Fatalf("unexpected balanced budget defaults: %#v", entry.Semaphore)
	}
	if entry.FallbackTarget == nil || len(entry.FallbackTarget.Pool) != 1 || entry.FallbackTarget.Pool[0].Provider != "hyperfusion" || !entry.FallbackTarget.ReschedulableOnly {
		t.Fatalf("expected tier 1 fallback route, got %#v", entry.FallbackTarget)
	}
	if entry.FallbackTarget.Pool[0].Tier != 1 {
		t.Fatalf("expected tier 1 target Tier=1, got %d", entry.FallbackTarget.Pool[0].Tier)
	}
}

func TestCompileModelsSafeContinueIsExplicitOptIn(t *testing.T) {
	providers := testDeclarativeProviders()

	// Continue block without safe_continue=true must fail.
	_, err := CompileWithModels(nil, map[string]ModelSpec{
		"standard": {
			Tiers:    []TierSpec{{Targets: []TargetSpec{{Provider: "gonka-a", Native: "m"}}}},
			Continue: &ContinueSpec{Retries: 1},
		},
	}, providers)
	if err == nil || !strings.Contains(err.Error(), "safe_continue=true") {
		t.Fatalf("expected safe_continue=true requirement error, got %v", err)
	}

	safe := true
	compiled, err := CompileWithModels(nil, map[string]ModelSpec{
		"standard": {
			Tiers:        []TierSpec{{Targets: []TargetSpec{{Provider: "gonka-a", Native: "m"}}}},
			SafeContinue: &safe,
			Continue:     &ContinueSpec{Idle: Duration{Duration: 30 * time.Second}, Retries: 3},
		},
	}, providers)
	if err != nil {
		t.Fatal(err)
	}
	c := compiled.Models["standard"].Continue
	if !c.Enabled || c.Idle != 30*time.Second || c.Retries != 3 {
		t.Fatalf("unexpected ContinueConfig: %#v", c)
	}
}

func TestCompileModelsStatefulAndValidationErrors(t *testing.T) {
	providers := testDeclarativeProviders()

	// Single stateful target succeeds without retry/hedge/continue.
	compiled, err := CompileWithModels(nil, map[string]ModelSpec{
		"agent": {
			Tiers: []TierSpec{{Targets: []TargetSpec{{Provider: "codex-session", Native: "codex"}}}},
		},
	}, providers)
	if err != nil {
		t.Fatalf("single stateful target should compile: %v", err)
	}
	agent := compiled.Models["agent"]
	if !agent.Stateful || agent.Retry.Attempts != 0 || agent.Hedge.Enabled || agent.Semaphore.MaxHedges != 0 {
		t.Fatalf("unexpected stateful compiled route: %#v", agent)
	}

	// Stateful multi-tier fails validateSessionRoutes.
	_, err = CompileWithModels(nil, map[string]ModelSpec{
		"agent": {
			Tiers: []TierSpec{
				{Targets: []TargetSpec{{Provider: "codex-session", Native: "codex"}}},
				{Targets: []TargetSpec{{Provider: "gonka-a", Native: "m"}}},
			},
		},
	}, providers)
	if err == nil {
		t.Fatal("expected error for multi-tier stateful model")
	}

	// Selector with no matching providers fails.
	_, err = CompileWithModels(nil, map[string]ModelSpec{
		"missing": {
			Tiers: []TierSpec{{Targets: []TargetSpec{{Select: map[string]string{"pool": "nope"}, Native: "m"}}}},
		},
	}, providers)
	if err == nil || !strings.Contains(err.Error(), "matched no providers") {
		t.Fatalf("expected unmatched selector error, got %v", err)
	}

	// Unknown JSON field fails strict decode.
	var decoded ModelSpec
	if err := json.Unmarshal([]byte(`{"tiers":[{"targets":[{"provider":"gonka-a","native":"m"}]}],"unknown_opt":true}`), &decoded); err == nil {
		t.Fatal("expected DisallowUnknownFields error on unknown ModelSpec key")
	}
}
