package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"
)

func TestAdaptiveAttemptTimeoutSlicesRemainingBudgetForFallback(t *testing.T) {
	zero := 0
	cfg := config.Config{
		Providers: []provider.Provider{
			{ID: "a", Priority: 20, Config: map[string]any{"inference_url": "https://a.invalid"}},
			{ID: "b", Priority: 10, Config: map[string]any{"inference_url": "https://b.invalid"}},
		},
		Models: map[string]policy.ModelSpec{
			"standard": {
				Timeout: policy.Duration{Duration: 220 * time.Millisecond},
				Budget:  &policy.BudgetSpec{MaxCalls: 2, MaxHedges: &zero},
				Tiers: []policy.TierSpec{
					{Targets: []policy.TargetSpec{{Provider: "a", Native: "m"}}},
					{Targets: []policy.TargetSpec{{Provider: "b", Native: "m"}}},
				},
			},
		},
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	plan := compiled.RoutingPlan()
	if plan.Providers["a"].AttemptTimeout != 0 {
		t.Fatalf("expected explicit AttemptTimeout=0 on provider a, got %v", plan.Providers["a"].AttemptTimeout)
	}

	var aTimeoutSeen atomic.Int64
	var bTimeoutSeen atomic.Int64
	executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "a" {
			aTimeoutSeen.Store(int64(req.AttemptTimeout))
			<-ctx.Done()
			return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
		}
		bTimeoutSeen.Store(int64(req.AttemptTimeout))
		return successBody("b"), nil
	}}

	runner := NewRunner(plan, subsystem.NewCatalog(compiled), executor)
	defer runner.Close()
	warmHedge(runner, "m", false, 20*time.Millisecond, "a", "b")
	out, callErr := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil || out.Provider != "b" {
		t.Fatalf("expected Tier 1 fallback b to succeed after adaptive attempt slice, got %#v err=%v", out, callErr)
	}

	// A stalled warmed-up primary is bounded by its conservative learned p95,
	// leaving enough time for the trained backup, rather than a deadline fraction.
	gotA := time.Duration(aTimeoutSeen.Load())
	if gotA <= 0 || gotA >= 100*time.Millisecond {
		t.Fatalf("expected learned AttemptTimeout on a with time reserved for b, got %v", gotA)
	}
	// Final attempt ("b", no alternatives remaining) uses remaining deadline directly.
	if gotB := time.Duration(bTimeoutSeen.Load()); gotB <= 0 {
		t.Fatalf("expected positive remaining timeout passed to b, got %v", gotB)
	}
	if !strings.Contains(scrape(t, runner.Metrics()), `llm_adaptive_timeouts_total{route="standard",provider="a"} 1`) {
		t.Fatalf("missing adaptive timeout metric:\n%s", scrape(t, runner.Metrics()))
	}
}

func TestAdaptiveTimingPreservesHealthyColdOrUnaffordablePrimary(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, warm := range []string{"cold", "primary-only", "backup-only", "both-slow"} {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, warm), func(t *testing.T) {
				backup := &policy.CompiledRoute{Name: "backup", Pool: []model.Target{{Provider: "b", Model: "m"}}, RaceCount: 1, AdaptiveTiming: true}
				entry := &policy.CompiledRoute{Name: "standard", Pool: []model.Target{{Provider: "a", Model: "m"}}, RaceCount: 1, AdaptiveTiming: true,
					RouteTimeout: 200 * time.Millisecond, FallbackTarget: backup, Semaphore: policy.SemaphoreConfig{MaxCalls: 2, MaxInFlight: 1, MaxCallsPerProvider: 1}}
				plan := policy.RuntimePlan{Models: map[string]*policy.CompiledRoute{"standard": entry}, Providers: map[string]policy.RuntimeProvider{"a": {SafeReplay: true}, "b": {SafeReplay: true}}}
				var backupCalls atomic.Int32
				wait := func(ctx context.Context, target model.Target) *model.CallError {
					if target.Provider == "b" {
						backupCalls.Add(1)
					}
					select {
					case <-time.After(140 * time.Millisecond):
						return nil
					case <-ctx.Done():
						return &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
					}
				}
				exec := &fakeExecutor{
					do: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
						if err := wait(ctx, target); err != nil {
							return nil, err
						}
						return successBody(target.Provider), nil
					},
					stream: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
						if err := wait(ctx, target); err != nil {
							return nil, err
						}
						return completionStream("healthy", true), nil
					},
				}
				runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
				defer runner.Close()
				if warm == "primary-only" {
					warmHedge(runner, "m", stream, 20*time.Millisecond, "a")
				}
				if warm == "backup-only" {
					warmHedge(runner, "m", stream, 20*time.Millisecond, "b")
				}
				if warm == "both-slow" {
					warmHedge(runner, "m", stream, 140*time.Millisecond, "a", "b")
				}
				if stream {
					selected, err := runner.SelectStream(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
					if err != nil {
						t.Fatalf("healthy stream cancelled before its deadline: %+v", err)
					}
					defer selected.Cancel()
					if selected.Provider != "a" {
						t.Fatalf("selected %q instead of healthy primary", selected.Provider)
					}
				} else {
					out, err := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
					if err != nil || out.Provider != "a" {
						t.Fatalf("healthy primary cancelled: out=%+v error=%+v", out, err)
					}
				}
				if backupCalls.Load() != 0 {
					t.Fatalf("unnecessary fallback calls: %d", backupCalls.Load())
				}
			})
		}
	}
}

func TestAdaptiveTimingRespectsAffinityAndUnavailableAlternatives(t *testing.T) {
	for _, reason := range []string{"affinity", "cooling", "half-open", "capacity", "error-filter", "last-attempt"} {
		t.Run(reason, func(t *testing.T) {
			backup := &policy.CompiledRoute{Name: "backup", Pool: []model.Target{{Provider: "b", Model: "m"}}, RaceCount: 1, AdaptiveTiming: true}
			entry := &policy.CompiledRoute{Name: "standard", Pool: []model.Target{{Provider: "a", Model: "m"}}, RaceCount: 1, AdaptiveTiming: true,
				RouteTimeout: 200 * time.Millisecond, FallbackTarget: backup, Semaphore: policy.SemaphoreConfig{MaxCalls: 2, MaxInFlight: 1, MaxCallsPerProvider: 1}}
			plan := policy.RuntimePlan{Models: map[string]*policy.CompiledRoute{"standard": entry}, Providers: map[string]policy.RuntimeProvider{"a": {SafeReplay: true}, "b": {SafeReplay: true, Cooldown: time.Second}}}
			var backupCalls atomic.Int32
			exec := &fakeExecutor{do: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
				if target.Provider == "b" {
					backupCalls.Add(1)
					return successBody("b"), nil
				}
				select {
				case <-time.After(140 * time.Millisecond):
					return successBody("a"), nil
				case <-ctx.Done():
					return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
				}
			}}
			runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
			defer runner.Close()
			warmHedge(runner, "m", false, 20*time.Millisecond, "a", "b")
			request := model.ExecuteRequest{Kind: model.RequestChat}
			switch reason {
			case "affinity":
				entry.Pool = append(entry.Pool, backup.Pool...)
				entry.FallbackTarget = nil
				entry.Retry = policy.RetryConfig{Internal: true, Attempts: 1}
				entry.Affinity = policy.AffinityConfig{Enabled: true, Sources: []string{"responses.previous_response_id"}, TTL: time.Hour, OnProviderFailure: "fail-closed"}
				runner.Affinity().Bind("previous", "a", time.Hour)
				request.Kind, request.Body = model.RequestResponses, []byte(`{"previous_response_id":"previous","input":"hi"}`)
			case "cooling", "half-open":
				now := time.Now()
				runner.now = func() time.Time { return now }
				runner.record(context.Background(), "b", "m", &model.CallError{Class: model.ErrorUpstream, Status: 503})
				if reason == "half-open" {
					now = runner.cooldownUntil("b", "m").Add(time.Second)
					lease, ok := runner.acquireHalfOpenProbe(context.Background(), "b", "m", false)
					if !ok {
						t.Fatal("cannot reserve backup probe")
					}
					defer runner.releaseHalfOpenProbe(lease)
				}
			case "capacity":
				runner.capacity.SetLimit("b", 1)
				permit := runner.capacity.Acquire("b")
				defer permit.Release()
			case "error-filter":
				backup.ErrorIn = map[model.ErrorClass]bool{model.ErrorUpstream: true}
			case "last-attempt":
				entry.FallbackTarget = nil
			}
			out, err := runner.RunWithResult(context.Background(), "standard", request)
			if err != nil || out.Provider != "a" || backupCalls.Load() != 0 {
				t.Fatalf("illegal adaptive alternative: out=%+v callsB=%d error=%+v", out, backupCalls.Load(), err)
			}
		})
	}
}

func TestAdaptiveRemainingBudgetSkipsHopelessHedge(t *testing.T) {
	one := 1
	cfg := config.Config{
		Providers: []provider.Provider{
			{ID: "a", Priority: 20, Config: map[string]any{"inference_url": "https://a.invalid"}},
			{ID: "b", Priority: 10, Config: map[string]any{"inference_url": "https://b.invalid"}},
		},
		Models: map[string]policy.ModelSpec{
			"standard": {
				Timeout: policy.Duration{Duration: 150 * time.Millisecond},
				Budget:  &policy.BudgetSpec{MaxCalls: 2, MaxHedges: &one},
				Tiers: []policy.TierSpec{
					{Targets: []policy.TargetSpec{
						{Provider: "a", Native: "m"},
						{Provider: "b", Native: "m"},
					}},
				},
			},
		},
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	plan := compiled.RoutingPlan()
	plan.Models["standard"].Balance.ExplorationRate = 0

	var bCalls atomic.Int32
	executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "b" {
			bCalls.Add(1)
			return successBody("b"), nil
		}
		select {
		case <-time.After(80 * time.Millisecond):
			return successBody("a"), nil
		case <-ctx.Done():
			return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499}
		}
	}}

	runner := NewRunner(plan, subsystem.NewCatalog(compiled), executor)
	// Warm a so hedge delay triggers at ~25ms; keep a preferred over b in non-stream.
	warmHedge(runner, "m", false, 25*time.Millisecond, "a")
	for range 5 {
		runner.observeProviderOutcomeMode("a", false, nil, 15*time.Millisecond)
		// Backup b averages 500ms >> ~125ms remaining when hedge would fire.
		runner.observeProviderOutcomeMode("b", false, nil, 500*time.Millisecond)
	}

	out, callErr := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil || out.Provider != "a" {
		t.Fatalf("expected primary a to finish cleanly, got %#v err=%v", out, callErr)
	}
	if bCalls.Load() != 0 {
		t.Fatalf("expected scheduler to skip hopeless 500ms hedge within 150ms deadline, got %d calls to b", bCalls.Load())
	}
}

func TestAdaptiveRemainingBudgetSkipsHopelessTransition(t *testing.T) {
	zero := 0
	cfg := config.Config{
		Providers: []provider.Provider{
			{ID: "a", Priority: 20, Config: map[string]any{"inference_url": "https://a.invalid"}},
			{ID: "b", Priority: 10, Config: map[string]any{"inference_url": "https://b.invalid"}},
		},
		Models: map[string]policy.ModelSpec{
			"standard": {
				Timeout: policy.Duration{Duration: 200 * time.Millisecond},
				Budget:  &policy.BudgetSpec{MaxCalls: 2, MaxHedges: &zero},
				Tiers: []policy.TierSpec{
					{Targets: []policy.TargetSpec{{Provider: "a", Native: "m"}}},
					{Targets: []policy.TargetSpec{{Provider: "b", Native: "m"}}},
				},
			},
		},
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	plan := compiled.RoutingPlan()
	plan.Providers["a"] = policy.RuntimeProvider{SafeReplay: true}
	plan.Providers["b"] = policy.RuntimeProvider{SafeReplay: true}

	var bCalls atomic.Int32
	executor := &fakeExecutor{do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "b" {
			bCalls.Add(1)
			return successBody("b"), nil
		}
		// Finish inside adaptive slice (~110ms) at ~80ms with 503, leaving ~120ms (< 2s/4).
		time.Sleep(80 * time.Millisecond)
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503}
	}}

	runner := NewRunner(plan, subsystem.NewCatalog(compiled), executor)
	for range 5 {
		// Fallback b has learned EWMA of 2s; with only ~20ms remaining (< 500ms),
		// fallback cannot plausibly succeed and should preserve 503.
		runner.observeProviderOutcomeMode("b", false, nil, 2*time.Second)
	}

	_, callErr := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr == nil || callErr.Status != 503 {
		t.Fatalf("expected original 503 error preserved when fallback is hopeless, got %#v", callErr)
	}
	if bCalls.Load() != 0 {
		t.Fatalf("expected hopeless Tier 1 fallback skipped, got %d calls to b", bCalls.Load())
	}
}

func TestHalfOpenProbeMetricsRecorded(t *testing.T) {
	rules := policytest.EntryRules([]string{"a"}, 1)
	compiled := configtest.RulesConfig(t, 2, rules)
	cooldown := compiled.Providers["a"].Cooldown.Duration
	now := time.Unix(1_700_000_000, 0)

	var calls atomic.Int32
	started := make(chan struct{}, 2)
	executor := &fakeExecutor{do: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		if calls.Add(1) == 1 {
			started <- struct{}{}
			<-ctx.Done()
			return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499}
		}
		return successBody("a"), nil
	}}

	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
	runner.now = func() time.Time { return now }

	runner.record(context.Background(), "a", "native-model", &model.CallError{Class: model.ErrorUpstream, Status: 503})
	now = now.Add(cooldown + time.Second)

	reqCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = runner.RunWithResult(reqCtx, "standard", model.ExecuteRequest{Kind: model.RequestChat})
	}()
	<-started
	cancel()
	<-done

	if _, err := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat}); err != nil {
		t.Fatalf("expected follow-up recovery probe to succeed: %v", err)
	}

	metricsText := scrape(t, runner.Metrics())
	for _, want := range []string{
		`llm_half_open_probes_total{provider="a",domain="inference",result="admitted"} 2`,
		`llm_half_open_probes_total{provider="a",domain="inference",result="released"} 1`,
		`llm_half_open_probes_total{provider="a",domain="inference",result="recovered"} 1`,
	} {
		if !strings.Contains(metricsText, want) {
			t.Fatalf("missing half-open metric %q in:\n%s", want, metricsText)
		}
	}
}
