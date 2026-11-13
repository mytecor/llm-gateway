package subsystem

import (
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

func TestScoreStoreIsolatesStreamAndNonStreamStats(t *testing.T) {
	clock := newStoreAt(time.Unix(1_700_000_000, 0))
	// Provider a: fast and healthy when streaming, failing when non-streaming.
	for range 4 {
		clock.store.ObserveMode("a", true, nil, 100*time.Millisecond)
		clock.store.ObserveMode("a", false, &model.CallError{Class: model.ErrorUpstream, Status: 503}, 0)
	}
	clock.store.ObserveMode("a", false, nil, 3*time.Second)

	// Provider b: failing when streaming, fast and healthy when non-streaming.
	for range 4 {
		clock.store.ObserveMode("b", true, &model.CallError{Class: model.ErrorUpstream, Status: 503}, 0)
		clock.store.ObserveMode("b", false, nil, 250*time.Millisecond)
	}
	clock.store.ObserveMode("b", true, nil, 200*time.Millisecond)

	bal := policy.BalanceConfig{
		Enabled:     true,
		Strategy:    "expected-ttft",
		Window:      5 * time.Minute,
		ErrorBudget: 0.2,
	}
	targets := []model.Target{
		{Provider: "b", Model: "m"},
		{Provider: "a", Model: "m"},
	}

	if h := clock.store.HealthForMode("a", true, bal.Window, bal.ErrorBudget); h <= 0 {
		t.Fatalf("expected provider a streaming health > 0, got %v", h)
	}
	if h := clock.store.HealthForMode("a", false, bal.Window, bal.ErrorBudget); h != 0 {
		t.Fatalf("expected provider a non-stream health = 0, got %v", h)
	}
	if h := clock.store.HealthForMode("b", true, bal.Window, bal.ErrorBudget); h != 0 {
		t.Fatalf("expected provider b streaming health = 0, got %v", h)
	}
	if h := clock.store.HealthForMode("b", false, bal.Window, bal.ErrorBudget); h <= 0 {
		t.Fatalf("expected provider b non-stream health > 0, got %v", h)
	}

	streamSelected := clock.store.SelectForMode("standard", targets, bal, true)
	if streamSelected[0].Provider != "a" {
		t.Fatalf("streaming request must select healthy streaming provider a, got %q", streamSelected[0].Provider)
	}
	nonStreamSelected := clock.store.SelectForMode("standard", targets, bal, false)
	if nonStreamSelected[0].Provider != "b" {
		t.Fatalf("non-streaming request must select healthy non-stream provider b, got %q", nonStreamSelected[0].Provider)
	}
}

func TestScoreStoreExpectedTTFTVsCompletionSelection(t *testing.T) {
	clock := newStoreAt(time.Unix(1_700_000_000, 0))
	// Provider a: faster TTFT on stream (100ms), much slower full completion (4s).
	// Provider b: slower TTFT on stream (450ms), much faster full completion (700ms).
	for range 5 {
		clock.store.ObserveMode("a", true, nil, 100*time.Millisecond)
		clock.store.ObserveMode("a", false, nil, 4*time.Second)
		clock.store.ObserveMode("b", true, nil, 450*time.Millisecond)
		clock.store.ObserveMode("b", false, nil, 700*time.Millisecond)
	}

	targets := []model.Target{
		{Provider: "b", Model: "m"},
		{Provider: "a", Model: "m"},
	}

	autoBal := policy.BalanceConfig{
		Enabled:     true,
		Strategy:    "expected-ttft",
		Window:      5 * time.Minute,
		ErrorBudget: 0.2,
	}
	if got := clock.store.SelectForMode("standard", targets, autoBal, true)[0].Provider; got != "a" {
		t.Fatalf("expected-ttft with stream=true must pick lowest TTFT provider a, got %q", got)
	}
	if got := clock.store.SelectForMode("standard", targets, autoBal, false)[0].Provider; got != "b" {
		t.Fatalf("expected-ttft with stream=false must automatically pick lowest completion latency provider b, got %q", got)
	}

	compBal := autoBal
	compBal.Strategy = "expected-completion"
	if got := clock.store.SelectForMode("standard", targets, compBal, true)[0].Provider; got != "b" {
		t.Fatalf("expected-completion must pick lowest completion latency provider b even when stream=true, got %q", got)
	}
}

func TestExpectedCompletionUsesRequestedModeHealth(t *testing.T) {
	clock := newStoreAt(time.Unix(1_700_000_000, 0))
	for range 5 {
		// a is a fast completion provider, but its streaming API is failing.
		clock.store.ObserveMode("a", false, nil, 10*time.Millisecond)
		clock.store.ObserveMode("b", false, nil, time.Second)
		clock.store.ObserveMode("a", true, &model.CallError{Class: model.ErrorUpstream, Status: 503}, 0)
		clock.store.ObserveMode("b", true, nil, 200*time.Millisecond)
	}
	bal := policy.BalanceConfig{Enabled: true, Strategy: "expected-completion", Window: time.Minute, ErrorBudget: 0.2}
	targets := []model.Target{{Provider: "a", Model: "m"}, {Provider: "b", Model: "m"}}
	if got := clock.store.SelectForMode("standard", targets, bal, true)[0].Provider; got != "b" {
		t.Fatalf("streaming completion objective selected unhealthy stream provider %q", got)
	}
	if got := clock.store.SelectForMode("standard", targets, bal, false)[0].Provider; got != "a" {
		t.Fatalf("non-stream completion objective ignored healthy fast provider a: %q", got)
	}
}
