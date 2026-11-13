package scheduler

import (
	"context"
	"fmt"
	"sync"
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

func TestHalfOpenSingleProviderRejectsConcurrentProbeEvenWithFailOpen(t *testing.T) {
	for _, status := range []int{401, 503} {
		t.Run(fmt.Sprintf("status=%d", status), func(t *testing.T) {
			route := &policy.CompiledRoute{Name: "standard", RaceCount: 1, Pool: []model.Target{{Provider: "a", Model: "m"}}}
			plan := policy.RuntimePlan{Models: map[string]*policy.CompiledRoute{"standard": route}, Providers: map[string]policy.RuntimeProvider{"a": {Cooldown: time.Second}}}
			now := time.Unix(1_700_000_000, 0)
			started, release := make(chan struct{}), make(chan struct{})
			releaseProbe := sync.OnceFunc(func() { close(release) })
			defer releaseProbe()
			var calls atomic.Int32
			exec := &fakeExecutor{do: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
				if calls.Add(1) == 1 {
					close(started)
				}
				select {
				case <-release:
					return successBody("a"), nil
				case <-ctx.Done():
					return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499}
				}
			}}
			runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
			defer runner.Close()
			runner.now = func() time.Time { return now }
			runner.record(context.Background(), "a", "m", &model.CallError{Class: model.ErrorUpstream, Status: status})
			now = runner.cooldownUntil("a", "m").Add(time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			first := make(chan *model.CallError, 1)
			go func() {
				_, err := runner.Run(ctx, "standard", model.ExecuteRequest{Kind: model.RequestChat})
				first <- err
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("initial probe did not start")
			}
			second := make(chan *model.CallError, 1)
			go func() {
				_, err := runner.Run(ctx, "standard", model.ExecuteRequest{Kind: model.RequestChat})
				second <- err
			}()
			select {
			case err := <-second:
				if err == nil || err.Status != 503 || calls.Load() != 1 {
					t.Fatalf("concurrent request bypassed half-open: calls=%d error=%+v", calls.Load(), err)
				}
			case <-ctx.Done():
				t.Fatalf("concurrent request entered the held probe; calls=%d", calls.Load())
			}
			releaseProbe()
			if err := <-first; err != nil {
				t.Fatalf("initial probe failed: %+v", err)
			}
		})
	}
}

func TestRaceLoserReleasesProbeWithoutConsumingBufferedResult(t *testing.T) {
	plan := policy.RuntimePlan{Providers: map[string]policy.RuntimeProvider{"b": {Cooldown: time.Second}}}
	runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), &fakeExecutor{})
	defer runner.Close()
	now := time.Unix(1_700_000_000, 0)
	runner.now = func() time.Time { return now }
	runner.record(context.Background(), "b", "m", &model.CallError{Class: model.ErrorUpstream, Status: 503})
	now = runner.cooldownUntil("b", "m").Add(time.Second)
	probe, ok := runner.acquireHalfOpenProbe(context.Background(), "b", "m", false)
	if !ok || !probe.active {
		t.Fatal("initial probe was not reserved")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc := &schedule{
		r: runner, ctx: context.Background(),
		cancels:        map[int]context.CancelFunc{2: cancel},
		activeBranches: map[int]activeBranch{2: {provider: "b", started: now, probe: probe, observation: runner.hedgeLatency.Begin("b", "m", false)}},
		results:        make(chan *branchResult, 1),
	}
	// The loser has delivered its result, but winner 1 ends selection first.
	sc.results <- &branchResult{id: 2, provider: "b", err: &model.CallError{Class: model.ErrorCancelled, Status: 499}}
	sc.cancelOthers(1)
	if ctx.Err() == nil {
		t.Fatal("losing branch was not cancelled")
	}
	next, ok := runner.acquireHalfOpenProbe(context.Background(), "b", "m", false)
	if !ok || !next.active {
		t.Fatal("unconsumed loser result retained the half-open probe")
	}
	defer runner.releaseHalfOpenProbe(next)
	// A late cleanup from the old branch must not release the new epoch's lease.
	runner.releaseHalfOpenProbe(probe)
	if _, ok := runner.acquireHalfOpenProbe(context.Background(), "b", "m", true); ok {
		t.Fatal("old lease cleanup released the next probe")
	}
}

func TestHalfOpenRecoveryAdmitsSingleProbeNoStampede(t *testing.T) {
	rules := append(policytest.EntryRules([]string{"a", "b"}, 1),
		policytest.FallbackRuleHelper("standard", "backup"),
		policytest.FilterProviderUnused("backup", "b"),
		policytest.MapRuleHelper("backup", "native-model"),
		policytest.RankRuleHelper("backup"),
		policytest.RaceRuleHelper("backup", 1),
	)
	compiled := configtest.RulesConfig(t, 2, rules)
	cooldown := compiled.Providers["a"].Cooldown.Duration
	now := time.Unix(1_700_000_000, 0)
	var nowMu sync.Mutex
	getNow := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		now = now.Add(d)
		nowMu.Unlock()
	}

	var aCalls atomic.Int32
	var bCalls atomic.Int32
	var probeMode atomic.Int32 // 0: fail immediately, 1: hold probe & fail, 2: hold probe & succeed
	probeStarted := make(chan struct{}, 4)
	probeRelease := make(chan struct{})

	executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "b" {
			bCalls.Add(1)
			return successBody("b"), nil
		}
		aCalls.Add(1)
		mode := probeMode.Load()
		if mode == 0 {
			return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503}
		}
		probeStarted <- struct{}{}
		<-probeRelease
		if mode == 1 {
			return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503}
		}
		return successBody("a"), nil
	}}

	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
	runner.now = getNow

	// 1. Initial call fails on a, falls over within race/budget or retry -> cools a.
	runner.record(context.Background(), "a", "native-model", &model.CallError{Class: model.ErrorUpstream, Status: 503})
	if until := runner.cooldownUntil("a", "native-model"); until.IsZero() {
		t.Fatal("expected provider a to enter cooldown")
	}

	// 2. Advance past cooldown so a enters half-open state.
	advance(cooldown + time.Second)
	probeMode.Store(1) // first half-open probe blocks and then fails

	probeDone := make(chan *model.RunOutcome, 1)
	go func() {
		out, err := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
		if err != nil {
			t.Errorf("probe request unexpected terminal error: %v", err)
		}
		probeDone <- out
	}()

	<-probeStarted
	// While a's half-open probe is in flight, 5 concurrent requests must NOT stampede a;
	// all 5 must route directly to b.
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
			if err != nil {
				t.Errorf("concurrent request error: %v", err)
				return
			}
			if out.Provider != "b" {
				t.Errorf("concurrent request while probe in flight hit %q, want b", out.Provider)
			}
		}()
	}
	wg.Wait()
	if got := aCalls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 half-open probe on a while in flight, got %d", got)
	}

	// Release probe to fail -> a re-enters cooldown, request finishes on b.
	close(probeRelease)
	if out := <-probeDone; out == nil || out.Provider != "b" {
		t.Fatalf("expected failed probe request to fall back to b, got %#v", out)
	}
	if until := runner.cooldownUntil("a", "native-model"); until.IsZero() {
		t.Fatal("expected failed half-open probe to put provider a back into cooldown")
	}

	// Subsequent request while re-cooled goes straight to b without hitting a.
	prevA := aCalls.Load()
	out, err := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err != nil || out.Provider != "b" {
		t.Fatalf("expected re-cooled request on b, got %#v err=%v", out, err)
	}
	if aCalls.Load() != prevA {
		t.Fatalf("expected no calls to re-cooled provider a, got %d -> %d", prevA, aCalls.Load())
	}

	// 3. Advance past second cooldown -> next half-open probe succeeds -> transitions to Open.
	advance(cooldown + time.Second)
	probeMode.Store(2)
	probeRelease = make(chan struct{})

	probeDone2 := make(chan *model.RunOutcome, 1)
	go func() {
		out, err := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
		if err != nil {
			t.Errorf("recovery probe unexpected error: %v", err)
		}
		probeDone2 <- out
	}()
	<-probeStarted
	close(probeRelease)

	if out := <-probeDone2; out == nil || out.Provider != "a" {
		t.Fatalf("expected recovery probe to succeed on a, got %#v", out)
	}
	if until := runner.cooldownUntil("a", "native-model"); !until.IsZero() {
		t.Fatalf("expected recovered provider to have zero cooldown, got %v", until)
	}

	// Provider a is now Open: concurrent requests hit a freely again.
	beforeA := aCalls.Load()
	out, err = runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err != nil || out.Provider != "a" {
		t.Fatalf("expected open provider a to handle request, got %#v err=%v", out, err)
	}
	if aCalls.Load() <= beforeA {
		t.Fatal("expected open provider a to receive calls")
	}
}

func TestHalfOpenAuthCooldownAdmitsSingleProbeAcrossModels(t *testing.T) {
	compiled := configtest.RulesConfig(t, 2, []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a", "b"),
		policytest.MapRuleHelper("standard", "model-y"),
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 1),
		policytest.FallbackRuleHelper("standard", "backup"),
		policytest.FilterProviderUnused("backup", "b"),
		policytest.MapRuleHelper("backup", "model-y"),
		policytest.RankRuleHelper("backup"),
		policytest.RaceRuleHelper("backup", 1),
	})
	now := time.Unix(1_700_000_000, 0)
	var nowMu sync.Mutex
	getNow := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		now = now.Add(d)
		nowMu.Unlock()
	}

	var aCalls atomic.Int32
	var succeedA atomic.Bool
	probeStarted := make(chan struct{}, 2)
	probeRelease := make(chan struct{})

	executor := &fakeExecutor{do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "b" {
			return successBody("b"), nil
		}
		aCalls.Add(1)
		probeStarted <- struct{}{}
		<-probeRelease
		if succeedA.Load() {
			return successBody("a"), nil
		}
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 401}
	}}

	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
	runner.now = getNow

	// Auth failure on model-x cools provider a across ALL models (including model-y) for 10m.
	runner.record(context.Background(), "a", "model-x", &model.CallError{Class: model.ErrorUpstream, Status: 401})
	if until := runner.cooldownUntil("a", "model-y"); until.Sub(getNow()) != 10*time.Minute {
		t.Fatalf("expected 10m provider-wide auth cooldown, got %v", until.Sub(getNow()))
	}

	// Expire the 10m auth cooldown -> enters provider-wide half-open state.
	advance(10*time.Minute + time.Second)
	succeedA.Store(false)

	done := make(chan struct{})
	go func() {
		defer close(done)
		out, err := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
		if err != nil || out.Provider != "b" {
			t.Errorf("expected failed auth probe to fall back to b, got %#v err=%v", out, err)
		}
	}()
	<-probeStarted

	// Concurrent call while auth probe is in flight must bypass a and go to b.
	out, err := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err != nil || out.Provider != "b" {
		t.Fatalf("expected concurrent call during auth probe to hit b, got %#v err=%v", out, err)
	}
	if aCalls.Load() != 1 {
		t.Fatalf("expected single auth probe on a, got %d", aCalls.Load())
	}

	// Releasing failing probe triggers second consecutive 401 -> doubles cooldown to 20m.
	close(probeRelease)
	<-done
	if got := runner.cooldownUntil("a", "model-y").Sub(getNow()); got != 20*time.Minute {
		t.Fatalf("expected second consecutive 401 to double auth cooldown to 20m, got %v", got)
	}

	// Advance past 20m and allow probe to succeed -> clears auth cooldown and streak.
	advance(20*time.Minute + time.Second)
	succeedA.Store(true)
	probeRelease = make(chan struct{})
	close(probeRelease)
	out, err = runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err != nil || out.Provider != "a" {
		t.Fatalf("expected successful auth probe on a, got %#v err=%v", out, err)
	}
	runner.mu.Lock()
	streak := runner.authStreaks["a"]
	runner.mu.Unlock()
	if streak != 0 {
		t.Fatalf("expected auth streak cleared after recovery, got %d", streak)
	}
}

func TestHalfOpenProbeCancellationReleasesSlot(t *testing.T) {
	compiled := configtest.RulesConfig(t, 2, policytest.EntryRules([]string{"a"}, 1))
	cooldown := compiled.Providers["a"].Cooldown.Duration
	now := time.Unix(1_700_000_000, 0)

	var calls atomic.Int32
	started := make(chan struct{}, 2)
	executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		n := calls.Add(1)
		if n == 1 {
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
	errCh := make(chan *model.CallError, 1)
	go func() {
		_, callErr := runner.RunWithResult(reqCtx, "standard", model.ExecuteRequest{Kind: model.RequestChat})
		errCh <- callErr
	}()
	<-started
	cancel()
	if callErr := <-errCh; callErr == nil || callErr.Class != model.ErrorCancelled {
		t.Fatalf("expected cancelled probe error, got %#v", callErr)
	}

	// Half-open probe slot must have been released on cancellation so the very
	// next request can probe and recover instead of remaining wedged.
	out, callErr := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil || out.Provider != "a" {
		t.Fatalf("expected follow-up request to probe and succeed on a, got %#v err=%v", out, callErr)
	}
	runner.mu.Lock()
	halfOpenLen := len(runner.halfOpen)
	runner.mu.Unlock()
	if halfOpenLen != 0 {
		t.Fatalf("expected halfOpen map empty after recovery, got %d", halfOpenLen)
	}
}

func TestHalfOpenSingleProviderTierFallsBackToTier1DuringProbe(t *testing.T) {
	zero := 0
	cfg := config.Config{
		Providers: []provider.Provider{
			{ID: "a", Priority: 20, Config: map[string]any{"inference_url": "https://a.invalid"}},
			{ID: "b", Priority: 10, Config: map[string]any{"inference_url": "https://b.invalid"}},
		},
		Models: map[string]policy.ModelSpec{
			"standard": {
				Budget: &policy.BudgetSpec{MaxCalls: 2, MaxHedges: &zero},
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
	now := time.Unix(1_700_000_000, 0)
	cooldown := compiled.Providers["a"].Cooldown.Duration

	var aCalls atomic.Int32
	probeStarted := make(chan struct{}, 1)
	probeRelease := make(chan struct{})
	executor := &fakeExecutor{do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "b" {
			return successBody("b"), nil
		}
		aCalls.Add(1)
		probeStarted <- struct{}{}
		<-probeRelease
		return successBody("a"), nil
	}}

	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
	runner.now = func() time.Time { return now }

	runner.record(context.Background(), "a", "m", &model.CallError{Class: model.ErrorUpstream, Status: 503})
	now = now.Add(cooldown + time.Second)

	probeDone := make(chan *model.RunOutcome, 1)
	go func() {
		out, callErr := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
		if callErr != nil {
			t.Errorf("probe error: %v", callErr)
		}
		probeDone <- out
	}()
	<-probeStarted

	// Concurrent request on single-target Tier 0 must skip Tier 0 and hit Tier 1 directly.
	out, callErr := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil || out.Provider != "b" {
		t.Fatalf("expected concurrent request to fall straight to Tier 1 provider b, got %#v err=%v", out, callErr)
	}
	if aCalls.Load() != 1 {
		t.Fatalf("expected only 1 call to half-open Tier 0 provider a, got %d", aCalls.Load())
	}

	close(probeRelease)
	if out := <-probeDone; out == nil || out.Provider != "a" {
		t.Fatalf("expected probe to finish on a, got %#v", out)
	}
}

func TestSchedulerStreamVsNonStreamBalanceIsolation(t *testing.T) {
	zero := 0
	cfg := config.Config{
		Providers: []provider.Provider{
			{ID: "a", Priority: 20, Config: map[string]any{"inference_url": "https://a.invalid"}},
			{ID: "b", Priority: 10, Config: map[string]any{"inference_url": "https://b.invalid"}},
		},
		Models: map[string]policy.ModelSpec{
			"standard": {
				Budget: &policy.BudgetSpec{MaxCalls: 1, MaxHedges: &zero},
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

	// Disable exploration randomness for deterministic winner checks.
	entry := compiled.RoutingPlan().Models["standard"]
	entry.Balance.ExplorationRate = 0

	executor := &fakeExecutor{
		do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
			return successBody(target.Provider), nil
		},
		stream: func(_ context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			ch := make(chan model.StreamEvent, 2)
			ch <- model.StreamEvent{Data: []byte("hi"), Meaningful: true}
			ch <- model.StreamEvent{Done: true}
			close(ch)
			return ch, nil
		},
	}

	runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)

	// Feed isolated stream vs non-stream metrics through observeProviderOutcomeMode:
	// - streaming: a is fast (50ms TTFT) and healthy; b is slow (500ms TTFT)
	// - non-stream: a is slow (2s completion), b is fast (100ms completion)
	for range 5 {
		runner.observeProviderOutcomeMode("a", true, nil, 50*time.Millisecond)
		runner.observeProviderOutcomeMode("b", true, nil, 500*time.Millisecond)
		runner.observeProviderOutcomeMode("a", false, nil, 2*time.Second)
		runner.observeProviderOutcomeMode("b", false, nil, 100*time.Millisecond)
	}

	// Non-stream request must route to b.
	out, callErr := runner.RunWithResult(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil || out.Provider != "b" {
		t.Fatalf("expected non-stream request to choose fast-completion b, got %#v err=%v", out, callErr)
	}

	// Streaming request must route to a.
	streamHandle, callErr := runner.SelectStream(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if callErr != nil {
		t.Fatalf("SelectStream unexpected error: %v", callErr)
	}
	if streamHandle.Provider != "a" {
		streamHandle.Cancel()
		t.Fatalf("expected stream request to choose fast-TTFT a, got %q", streamHandle.Provider)
	}
	streamHandle.Cancel()
}
