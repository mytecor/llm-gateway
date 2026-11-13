package scheduler

import (
	"context"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"
)

func adaptiveRunner(t *testing.T, executor *fakeExecutor, hedge bool, extra ...policy.Rule) *Runner {
	t.Helper()
	rules := policytest.EntryRules([]string{"a", "b", "c"}, 1)
	if hedge {
		rules = append(rules, policytest.HedgeRuleHelper("standard", ""))
	}
	rules = append(rules, extra...)
	cfg := configtest.RulesConfig(t, 3, rules)
	return NewRunner(cfg.RoutingPlan(), subsystem.NewCatalog(cfg), executor)
}

func TestAdaptiveHedgeWarmupOptInAndEarlyWinner(t *testing.T) {
	for _, scenario := range []string{"warmup", "not enabled", "early winner"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
				calls.Add(1)
				if scenario == "early winner" {
					return successBody(target.Provider), nil
				}
				<-ctx.Done()
				return nil, &model.CallError{Class: model.ErrorCancelled, Cause: ctx.Err()}
			}}
			runner := adaptiveRunner(t, executor, scenario != "not enabled")
			if scenario != "warmup" {
				warmHedge(runner, "native-model", false, 5*time.Millisecond, "a")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			_, err := runner.Run(ctx, "standard", model.ExecuteRequest{Kind: model.RequestChat})
			if scenario == "early winner" && err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatalf("unexpected backup: %d calls", calls.Load())
			}
			if strings.Contains(scrape(t, runner.metrics), "llm_hedges_total{") {
				t.Fatal("counted a backup without dispatch")
			}
		})
	}
}

func TestAdaptiveHedgeLoserAndClientCancellationFeedback(t *testing.T) {
	for _, scenario := range []string{"hedge wins", "client cancel", "route timeout", "deadline before hedge", "call budget"} {
		t.Run(scenario, func(t *testing.T) {
			primaryDone := make(chan struct{})
			var backups atomic.Int32
			executor := &fakeExecutor{do: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
				if target.Provider != "a" {
					backups.Add(1)
					return successBody(target.Provider), nil
				}
				defer close(primaryDone)
				<-ctx.Done()
				return nil, &model.CallError{Class: model.ErrorCancelled, Cause: ctx.Err()}
			}}
			var extra []policy.Rule
			if scenario == "route timeout" || scenario == "deadline before hedge" {
				extra = append(extra, policytest.TimeoutRuleHelper("standard", 30*time.Millisecond))
			}
			if scenario == "call budget" {
				extra = append(extra, semaphoreRule("standard", 1, 2, 1))
			}
			runner := adaptiveRunner(t, executor, scenario == "hedge wins" || scenario == "call budget" || scenario == "deadline before hedge", extra...)
			delay := 5 * time.Millisecond
			if scenario == "deadline before hedge" {
				delay = time.Hour
			}
			warmHedge(runner, "native-model", false, delay, "a")
			before := runner.hedgeLatency.Begin("a", "native-model", false).Threshold
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()
			_, err := runner.Run(ctx, "standard", model.ExecuteRequest{Kind: model.RequestChat})
			select {
			case <-primaryDone:
			case <-time.After(time.Second):
				t.Fatal("primary not cancelled")
			}
			after := runner.hedgeLatency.Begin("a", "native-model", false).Threshold
			if scenario == "hedge wins" || scenario == "route timeout" {
				expected := time.Duration(float64(before) * math.Exp(.095))
				if after != expected {
					t.Fatalf("expected exactly one censored update: before=%v after=%v expected=%v", before, after, expected)
				}
			} else if after != before {
				t.Fatal("client cancellation changed quantile")
			}
			if scenario == "hedge wins" {
				if err != nil || backups.Load() != 1 {
					t.Fatalf("hedge failed: %v backups=%d", err, backups.Load())
				}
				if !strings.Contains(scrape(t, runner.metrics), `llm_hedges_total{route="standard"} 1`) {
					t.Fatal("missing dispatch metric")
				}
			} else if backups.Load() != 0 {
				t.Fatal("unexpected backup")
			}
		})
	}
}

func TestAdaptiveHedgeTimerUsesAllActivePrimarySnapshots(t *testing.T) {
	runner := adaptiveRunner(t, &fakeExecutor{}, true)
	warmHedge(runner, "native-model", false, 5*time.Millisecond, "a")
	warmHedge(runner, "native-model", false, time.Hour, "b")
	now := time.Now()
	sc := &schedule{r: runner, hedgeG: 1, armed: make([]bool, 2), launched: make([]bool, 2), activeBranches: map[int]activeBranch{
		1: {group: 0, started: now, observation: runner.hedgeLatency.Begin("a", "native-model", false)},
		2: {group: 0, started: now, observation: runner.hedgeLatency.Begin("b", "native-model", false)},
	}}
	defer sc.stopHedge()
	sc.armHedge()
	select {
	case <-sc.hedgeC:
		t.Fatal("did not wait for slowest primary threshold")
	case <-time.After(15 * time.Millisecond):
	}
	// Removing a failed long-threshold primary must immediately rearm for a.
	delete(sc.activeBranches, 2)
	sc.armHedge()
	select {
	case <-sc.hedgeC:
	case <-time.After(time.Second):
		t.Fatal("did not rearm after failure")
	}
	// A newly dispatched, unproven primary disarms the timer.
	sc.activeBranches[3] = activeBranch{group: 0, started: time.Now(), observation: runner.hedgeLatency.Begin("c", "native-model", false)}
	sc.armHedge()
	if sc.hedgeC != nil {
		t.Fatal("timer armed during partial warmup")
	}
	delete(sc.activeBranches, 1)
	delete(sc.activeBranches, 3)
	sc.armHedge()
	if sc.hedgeC != nil || sc.hedgeTimer != nil {
		t.Fatal("timer retained without primaries")
	}
}

func TestAdaptiveHedgeStreamingSelectsMeaningfulEvent(t *testing.T) {
	var backups atomic.Int32
	executor := &fakeExecutor{stream: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		events := make(chan model.StreamEvent, 2)
		if target.Provider == "a" {
			events <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{"role":"assistant"}}]}`)}
			go func() { <-ctx.Done(); close(events) }()
		} else {
			backups.Add(1)
			events <- model.StreamEvent{Meaningful: true, Data: []byte(`{"choices":[{"delta":{"reasoning_content":"thinking"}}]}`)}
			events <- model.StreamEvent{Done: true}
			close(events)
		}
		return events, nil
	}}
	runner := adaptiveRunner(t, executor, true)
	warmHedge(runner, "native-model", true, 10*time.Millisecond, "a")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := runner.SelectStream(ctx, "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Cancel()
	if stream.Provider == "a" || backups.Load() != 1 {
		t.Fatalf("role event selected a winner or no hedge: %s", stream.Provider)
	}
	if runner.hedgeLatency.Begin("a", "native-model", false).Threshold != 0 {
		t.Fatal("stream feedback leaked into non-stream profile")
	}
}

func TestBranchLatencyRecordedBeforeResultConsumption(t *testing.T) {
	executor := &fakeExecutor{do: func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError) {
		return successBody("a"), nil
	}}
	runner := adaptiveRunner(t, executor, false)
	for range 20 {
		ctx, cancel := context.WithCancel(context.Background())
		sc := &schedule{r: runner, ctx: ctx, results: make(chan *branchResult, 1)}
		sample := runner.hedgeLatency.Begin("a", "native-model", false)
		permit := runner.capacity.Acquire("a")
		if permit == nil {
			t.Fatal("no permit")
		}
		sc.runBranch(ctx, 1, cancel, model.Target{Provider: "a", Model: "native-model"}, permit, activeBranch{started: time.Now().Add(-time.Millisecond), observation: sample})
		cancel() // never consume sc.results: the measurement must already be recorded
	}
	if runner.hedgeLatency.Begin("a", "native-model", false).Threshold == 0 {
		t.Fatal("unconsumed branch successes were lost")
	}
}
