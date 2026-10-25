package scheduler

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

func continueConfig(t *testing.T) *config.CompiledConfig {
	t.Helper()
	cfg := config.TestConfig()
	cfg.Providers = cfg.Providers[:2] // a, b
	cfg.Rules = []policy.Rule{
		policy.FilterModel("standard", "standard"),
		policy.FilterProvider("standard", "a", "b"),
		policy.MapRuleHelper("standard", "native-model"),
		policy.RankRuleHelper("standard"),
		policy.RaceRuleHelper("standard", 2),
		policy.ContinueRuleWait("standard", 90*time.Second, "full", 0, 50*time.Millisecond),
	}
	compiled, err := config.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestContinueStreamExcludesUsedProviderAndResharesPartial(t *testing.T) {
	compiled := continueConfig(t)
	calls := make(map[string]int)
	var bodies []byte
	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			calls[target.Provider]++
			bodies = request.Body
			ev := make(chan model.StreamEvent, 3)
			ev <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"continued"}}]}`), Meaningful: true}
			close(ev)
			return ev, nil
		},
	}
	runner := NewRunner(compiled, subsystem.NewCatalog(compiled), executor)
	request := model.ExecuteRequest{
		Kind: model.RequestChat,
		Body: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	}
	partial := &subsystem.PartialStreamOutput{Content: "let me think", Reasoning: "reasoning", GotText: true}
	// The broken provider "a" was already used; it must be excluded.
	selected, callErr := runner.ContinueStream(context.Background(), "standard", request, partial, []string{"a"})
	if callErr != nil {
		t.Fatalf("ContinueStream failed: %v", callErr)
	}
	if selected.Provider != "b" {
		t.Fatalf("continuation provider = %q, want b", selected.Provider)
	}
	if calls["a"] != 0 {
		t.Fatalf("broken provider a was re-contacted %d time(s); must be excluded", calls["a"])
	}
	if calls["b"] != 1 {
		t.Fatalf("provider b calls = %d, want 1", calls["b"])
	}
	if !strings.Contains(string(bodies), `"role":"assistant"`) || !strings.Contains(string(bodies), "let me think") {
		t.Fatalf("continuation body must carry reshared assistant message: %s", bodies)
	}
	if !strings.Contains(string(bodies), `"role":"user"`) {
		t.Fatalf("continuation body must preserve the original conversation: %s", bodies)
	}
}

func TestContinueStreamTerminalFailureAfterAllProvidersUsed(t *testing.T) {
	compiled := continueConfig(t)
	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			ev := make(chan model.StreamEvent, 1)
			ev <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"x"}}]}`), Meaningful: true}
			close(ev)
			return ev, nil
		},
	}
	runner := NewRunner(compiled, subsystem.NewCatalog(compiled), executor)
	request := model.ExecuteRequest{Kind: model.RequestChat, Body: []byte(`{"messages":[{"role":"user","content":"hi"}]}`)}
	partial := &subsystem.PartialStreamOutput{Content: "x", GotText: true}
	if _, callErr := runner.ContinueStream(context.Background(), "standard", request, partial, []string{"a", "b"}); callErr == nil {
		t.Fatal("ContinueStream must fail when every provider is already used")
	}
}

func TestRunnerChainRetryReracesWholePoolWithResharedPartial(t *testing.T) {
	compiled := continueConfig(t)
	var callsA, callsB int
	var bodies [][]byte
	var mu sync.Mutex
	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			mu.Lock()
			bodies = append(bodies, append([]byte(nil), request.Body...))
			mu.Unlock()
			ch := make(chan model.StreamEvent, 3)
			switch target.Provider {
			case "a":
				callsA++
				if callsA == 1 {
					ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"from-a-part"}}]}`), Meaningful: true}
					close(ch)
					return ch, nil
				}
				// Chain-retry pass: answers for real.
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"final-answer"}}]}`), Meaningful: true}
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`), Meaningful: false}
				close(ch)
				return ch, nil
			case "b":
				callsB++
				if callsB == 1 {
					go func() {
						<-ctx.Done()
						close(ch)
					}()
					return ch, nil
				}
				if callsB == 2 {
					ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"from-b-part"}}]}`), Meaningful: true}
					close(ch)
					return ch, nil
				}
				go func() {
					<-ctx.Done()
					close(ch)
				}()
				return ch, nil
			}
			panic("unknown provider")
		},
	}
	runner := NewRunner(compiled, subsystem.NewCatalog(compiled), executor)
	request := model.ExecuteRequest{Kind: model.RequestChat, Body: []byte(`{"messages":[{"role":"user","content":"hi"}]}`)}
	ctx := context.Background()
	partial := &subsystem.PartialStreamOutput{}

	// Wave 1: initial selection — a wins (from-a-part) then breaks.
	sel, callErr := runner.SelectStream(ctx, "standard", request)
	if callErr != nil {
		t.Fatalf("select failed: %v", callErr)
	}
	if sel.Provider != "a" {
		t.Fatalf("initial winner = %q, want a", sel.Provider)
	}
	for _, ev := range sel.Buffered {
		subsystem.AccumulatePartial(ev.Data, partial)
	}
	partial.GotText = true
	sel.Cancel()

	// Wave 2: takeover to the one remaining provider — b wins then breaks.
	next, callErr := runner.ContinueStream(ctx, "standard", request, partial, []string{"a"})
	if callErr != nil {
		t.Fatalf("takeover failed: %v", callErr)
	}
	if next.Provider != "b" {
		t.Fatalf("takeover provider = %q, want b", next.Provider)
	}
	for _, ev := range next.Buffered {
		subsystem.AccumulatePartial(ev.Data, partial)
	}
	next.Cancel()

	// Wave 3: the chain is exhausted — every provider is broken.
	if _, callErr := runner.ContinueStream(ctx, "standard", request, partial, []string{"a", "b"}); callErr == nil {
		t.Fatal("ContinueStream must fail once every provider is broken")
	}

	// Wave 4: the whole-chain retry re-dispatches with an empty broken set,
	// resharing the accumulated partial — a races again and answers.
	retry, callErr := runner.ContinueStream(ctx, "standard", request, partial, nil)
	if callErr != nil {
		t.Fatalf("chain retry failed: %v", callErr)
	}
	if retry.Provider != "a" {
		t.Fatalf("chain retry provider = %q, want a", retry.Provider)
	}
	if callsA != 2 || callsB != 3 {
		t.Fatalf("chain retry must re-race the whole pool: callsA=%d callsB=%d, want 2/3", callsA, callsB)
	}
	lastBody := string(bodies[len(bodies)-1])
	if !strings.Contains(lastBody, `"role":"assistant"`) || !strings.Contains(lastBody, "from-a-part") || !strings.Contains(lastBody, "from-b-part") {
		t.Fatalf("chain retry must reshape the accumulated partial into assistant context: %s", lastBody)
	}
}

func TestRunnerContinuationFleesCoolingProviderUntilCooldownExpires(t *testing.T) {
	compiled := continueConfig(t) // providers a, b; retries 0
	current := time.Unix(1_700_000_000, 0)
	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			ch := make(chan model.StreamEvent, 3)
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"` + target.Provider + `-answer"}}]}`), Meaningful: true}
			close(ch)
			return ch, nil
		},
	}
	runner := NewRunner(compiled, subsystem.NewCatalog(compiled), executor)
	runner.now = func() time.Time { return current }
	request := model.ExecuteRequest{Kind: model.RequestChat, Body: []byte(`{"messages":[{"role":"user","content":"hi"}]}`)}
	ctx := context.Background()
	partial := &subsystem.PartialStreamOutput{Content: "x", GotText: true}

	cooldown := compiled.Providers["a"].Cooldown.Duration
	runner.RecordStreamFailure(ctx, "standard", "a", "native-model",
		&model.CallError{Class: model.ErrorUpstream, Status: 502})
	current = current.Add(time.Second) // b breaks one second after a
	runner.now = func() time.Time { return current }
	runner.RecordStreamFailure(ctx, "standard", "b", "native-model",
		&model.CallError{Class: model.ErrorUpstream, Status: 502})
	if _, callErr := runner.ContinueStream(ctx, "standard", request, partial, nil); callErr == nil {
		t.Fatal("a continuation over an all-cooling pool must fail, not fail-open onto the breakers")
	}

	current = current.Add(cooldown - 500*time.Millisecond)
	runner.now = func() time.Time { return current }
	if got := runner.AvailableTargets([]model.Target{{Provider: "a", Model: "native-model"}}); len(got) != 1 {
		t.Fatalf("a must be eligible again once its cooldown expires, available: %#v", got)
	}
	if got := runner.AvailableTargets([]model.Target{{Provider: "b", Model: "native-model"}}); len(got) != 0 {
		t.Fatalf("b must still be cooling, available: %#v", got)
	}
	recovered, callErr := runner.ContinueStream(ctx, "standard", request, partial, nil)
	if callErr != nil {
		t.Fatalf("continuation after cooldown expiry failed: %v", callErr)
	}
	if recovered.Provider != "a" {
		t.Fatalf("the recovered provider must be re-raceable once its cooldown expires, got %q", recovered.Provider)
	}
	recovered.Cancel()
}
