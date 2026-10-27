package server

import (
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"

	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

// TestServerHoldKeepsStreamOpenUntilRecovery is the headline contract for
// "never surface an error while the provider pool recovers": when a winner
// breaks mid-stream and the whole chain is exhausted (every provider broke or
// is cooling), the gateway does NOT surface a terminal error immediately.
// Instead it holds the relayed stream open, sends SSE keep-alives, and
// re-attempts the continuation until a provider recovers; then it hands the
// stream back and completes with [DONE]. The client never sees an event:
// error frame on a recoverable outage.
func TestServerHoldKeepsStreamOpenUntilRecovery(t *testing.T) {
	oldRecheck := policy.ContinueRecoveryRecheck
	oldKeepAlive := policy.ContinueRecoveryKeepAlive
	policy.ContinueRecoveryRecheck = 10 * time.Millisecond
	policy.ContinueRecoveryKeepAlive = 10 * time.Millisecond
	t.Cleanup(func() {
		policy.ContinueRecoveryRecheck = oldRecheck
		policy.ContinueRecoveryKeepAlive = oldKeepAlive
	})

	// Provider a: on its initial race (call #1) relays a partial and then
	// closes without finish_reason, so it wins and breaks. On any later call
	// it fails at selection (returns a model.CallError) so it can never win the
	// hold's re-races. Provider b: loses the initial race silently (call #1),
	// breaks on the takeover attempt (call #2) — exhausting the chain so the
	// gateway enters the hold — then recovers (call #3) when the hold's
	// recheck re-races the pool. The client sees partial + recovered answer +
	// [DONE], never an error.
	var aCalls, bCalls int
	executor := &fakeExecutor{stream: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		ch := make(chan model.StreamEvent, 4)
		switch target.Provider {
		case "a":
			aCalls++
			if aCalls == 1 {
				// Initial race winner: relays a partial, then breaks.
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"partial-from-a"}}]}`), Meaningful: true}
				close(ch)
				return ch, nil
			}
			// Later races: fail at selection so a never wins again.
			return nil, &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: errors.New("a refuses to recover")}
		case "b":
			bCalls++
			if bCalls == 1 {
				// Initial race loser: silent until cancelled.
				go func() {
					<-ctx.Done()
					close(ch)
				}()
				return ch, nil
			}
			if bCalls == 2 {
				// Takeover attempt (after a breaks): breaks too, so the chain
				// exhausts and the gateway enters the hold.
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"partial-from-b"}}]}`), Meaningful: true}
				close(ch)
				return ch, nil
			}
			// Continuation during the hold: recovers and completes.
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"recovered-from-b"}}]}`), Meaningful: true}
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`), Meaningful: false}
			close(ch)
			return ch, nil
		}
		close(ch)
		return ch, nil
	}}
	compiled := continueConfigHold(t, 0, 2*time.Second, 300*time.Millisecond)
	api, _ := streamTestServer(t, compiled, executor)

	body := readStreamBody(t, api.URL, 5*time.Second)

	// The pre-break partials and the recovered answer must all be relayed,
	// and a recovery within the horizon completes without any error frame.
	if !strings.Contains(body, "partial-from-a") {
		t.Errorf("the pre-break partial must be preserved on the held stream: %s", body)
	}
	if !strings.Contains(body, "recovered-from-b") {
		t.Errorf("the recovered continuation must be relayed on the held stream: %s", body)
	}
	// A recoverable outage must never surface a client-facing error.
	if strings.Contains(body, "event: error") || strings.Contains(body, "upstream stream failed") {
		t.Errorf("a recoverable outage must not surface a terminal error: %s", body)
	}
	// The stream must end with the completion marker, not an error.
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Errorf("a recovered held stream must end with [DONE]: %s", body)
	}
	// The continuation must have been re-attempted during the hold (recovery
	// on the recheck) — not merely a single-provider takeover.
	if bCalls < 3 {
		t.Errorf("the hold must re-contact provider b (got %d calls, want >= 3)", bCalls)
	}
}

// TestServerHoldSurfacesTerminalErrorAfterHorizon pins the bounded horizon:
// when the pool stays fully down past the continue.wait horizon, the gateway
// finally surfaces the terminal error as a last resort (the client can then
// retry from scratch). The keep-alives must be present during the hold, and
// the terminal error must carry the stable "upstream stream failed" text.
func TestServerHoldSurfacesTerminalErrorAfterHorizon(t *testing.T) {
	oldKeepAlive := policy.ContinueRecoveryKeepAlive
	policy.ContinueRecoveryKeepAlive = 5 * time.Millisecond
	t.Cleanup(func() { policy.ContinueRecoveryKeepAlive = oldKeepAlive })

	// Every provider always breaks after a partial, so the pool never
	// recovers within the (short) hold horizon.
	breakAll := func(ctx context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		ch := make(chan model.StreamEvent, 3)
		ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"partial"}}]}`), Meaningful: true}
		close(ch)
		return ch, nil
	}
	compiled := continueConfigWait(t, 0, 80*time.Millisecond)
	api, _ := streamTestServer(t, compiled, &fakeExecutor{stream: breakAll})

	body := readStreamBody(t, api.URL, 5*time.Second)

	// Keep-alives were emitted while the stream was held open.
	if !strings.Contains(body, ": ping") {
		t.Errorf("a held stream must emit keep-alives before the horizon expires: %s", body)
	}
	// After the horizon the terminal error appears (last resort), with the
	// unchanged stable message text.
	if !strings.Contains(body, "event: error") || !strings.Contains(body, "upstream stream failed") {
		t.Errorf("after the hold horizon the terminal error must surface: %s", body)
	}
	// And the stream must NOT be marked done.
	if strings.Contains(body, "data: [DONE]") {
		t.Errorf("an errored held stream must not end with [DONE]: %s", body)
	}
}

// TestServerHoldStopsOnClientDisconnect pins that holding a stream open never
// outlives the client: when the client disconnects while the pool is down,
// the gateway stops waiting and writes nothing further (the request context
// is cancelled). It must not emit an error to a client that is already gone.
func TestServerHoldStopsOnClientDisconnect(t *testing.T) {
	oldRecheck := policy.ContinueRecoveryRecheck
	policy.ContinueRecoveryRecheck = 10 * time.Millisecond
	t.Cleanup(func() { policy.ContinueRecoveryRecheck = oldRecheck })

	breakAll := func(ctx context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		ch := make(chan model.StreamEvent, 3)
		ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"partial"}}]}`), Meaningful: true}
		close(ch)
		return ch, nil
	}
	compiled := continueConfigWait(t, 0, time.Hour) // effectively unbounded
	api, _ := streamTestServer(t, compiled, &fakeExecutor{stream: breakAll})

	// Open the request, read a little, then cancel the client context.
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"standard","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Read the prelude (the partial) so the stream is held, then cancel.
	br := bufio.NewReader(resp.Body)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("read prelude: %v", err)
	}
	cancel()
	// Wait briefly; the gateway should stop without writing an error frame.
	time.Sleep(50 * time.Millisecond)
	// Reading must now yield EOF or a connection error, never an error frame.
	var b strings.Builder
	_, _ = io.CopyN(&b, br, 1<<20)
	if strings.Contains(b.String(), "event: error") {
		t.Errorf("a client that disconnected during the hold must not receive an error frame: %q", b.String())
	}
}

// TestServerHoldRecoveryBoundedWhenProviderRepeatedlyBreaks ensures a single
// provider whose cooldown expires inside the hold window cannot loop forever
// when it keeps emitting a partial and breaking on every recovered attempt.
func TestServerHoldRecoveryBoundedWhenProviderRepeatedlyBreaks(t *testing.T) {
	oldRecheck := policy.ContinueRecoveryRecheck
	oldKeepAlive := policy.ContinueRecoveryKeepAlive
	policy.ContinueRecoveryRecheck = 5 * time.Millisecond
	policy.ContinueRecoveryKeepAlive = 5 * time.Millisecond
	t.Cleanup(func() {
		policy.ContinueRecoveryRecheck = oldRecheck
		policy.ContinueRecoveryKeepAlive = oldKeepAlive
	})

	var calls int
	executor := &fakeExecutor{stream: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		calls++
		ch := make(chan model.StreamEvent, 2)
		ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"reasoning":"thinking..."}}]}`), Meaningful: true}
		close(ch)
		return ch, nil
	}}
	cfg := configtest.TestConfig()
	cfg.Providers = cfg.Providers[:1]
	cfg.Providers[0].Cooldown = policy.Duration{Duration: 10 * time.Millisecond}
	cfg.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a"),
		policytest.MapRuleHelper("standard", "native-model"),
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 1),
		&policy.RetryRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionRetry}, Attempts: 2},
		&policy.TimeoutRule{
			RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout},
			Duration: policy.Duration{Duration: time.Second},
			Idle:     policy.Duration{Duration: 5 * time.Second},
		},
		&policy.ContinueRule{
			RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionContinue},
			Retries:  2,
			Wait:     policy.Duration{Duration: time.Second},
		},
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	api, _ := streamTestServer(t, compiled, executor)

	body := readStreamBody(t, api.URL, 2*time.Second)

	if !strings.Contains(body, "event: error") {
		t.Fatalf("repeatedly breaking single provider must eventually surface error instead of looping forever: %s", body)
	}
	if calls != 3 {
		t.Fatalf("expected 1 initial + 2 hold-recovery calls, got %d", calls)
	}
}

func TestServerEmitsParsedKeepAliveDeltasDuringSlowSelectAndToolGeneration(t *testing.T) {
	oldKeepAlive := policy.ContinueRecoveryKeepAlive
	policy.ContinueRecoveryKeepAlive = 10 * time.Millisecond
	t.Cleanup(func() { policy.ContinueRecoveryKeepAlive = oldKeepAlive })

	executor := &fakeExecutor{stream: func(ctx context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		ch := make(chan model.StreamEvent, 4)
		// Slow prefill before first chunk (triggers SelectStream keep-alive).
		time.Sleep(25 * time.Millisecond)
		ch <- model.StreamEvent{Data: []byte(`{"id":"chat-slow","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"bash","arguments":"{\"command\":\"ls\"}"}}]}}]}`), Meaningful: true}
		go func() {
			// Slow second phase after tool_calls (triggers mid-stream tool_calls keep-alive).
			time.Sleep(25 * time.Millisecond)
			ch <- model.StreamEvent{Data: []byte(`{"id":"chat-slow","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`), Meaningful: false}
			close(ch)
		}()
		return ch, nil
	}}
	compiled := continueConfigWait(t, 1, time.Second)
	api, _ := streamTestServer(t, compiled, executor)

	body := readStreamBody(t, api.URL, 2*time.Second)

	if !strings.Contains(body, `"reasoning_content":" "`) {
		t.Fatalf("expected reasoning_content keep-alive chunk during slow pre-selection wait: %s", body)
	}
	if !strings.Contains(body, `"arguments":""`) {
		t.Fatalf("expected empty tool_calls arguments keep-alive chunk during slow post-tool wait: %s", body)
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Fatalf("expected completed stream ending with [DONE]: %s", body)
	}
}

func continueConfigRetriesWait(t *testing.T, retries int, wait time.Duration) *config.CompiledConfig {
	t.Helper()
	cfg := configtest.TestConfig()
	cfg.Providers = cfg.Providers[:2] // a, b
	cfg.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a", "b"),
		policytest.MapRuleHelper("standard", "native-model"),
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 2),
		policytest.ContinueRuleWait("standard", 90*time.Second, "full", retries, wait),
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

// continueConfigWait is a continueConfigRetries variant with an explicit hold
// horizon (wait) on the continue rule.
func continueConfigWait(t *testing.T, retries int, wait time.Duration) *config.CompiledConfig {
	t.Helper()
	return continueConfigRetriesWait(t, retries, wait)
}

// continueConfigHold is continueConfigWait for hold tests that expect a
// provider to RECOVER inside the horizon: it sets the provider cooldowns
// below the hold wait, so the hold's strict-availability recheck can re-race
// a provider once its cooldown expires within the wait window. With the
// production-default 15s cooldown a short test horizon would expire before
// any provider became re-eligible, and the test could never exercise
// recovery.
func continueConfigHold(t *testing.T, retries int, wait, cooldown time.Duration) *config.CompiledConfig {
	t.Helper()
	cfg := configtest.TestConfig()
	for i := range cfg.Providers {
		cfg.Providers[i].Cooldown = policy.Duration{Duration: cooldown}
	}
	cfg.Providers = cfg.Providers[:2] // a, b
	cfg.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a", "b"),
		policytest.MapRuleHelper("standard", "native-model"),
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 2),
		policytest.ContinueRuleWait("standard", 90*time.Second, "full", retries, wait),
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

// readStreamBody POSTs a streaming chat request and returns the full body. It
// is the streaming-aware counterpart of postStream: it reads until EOF and
// guards against the gateway never terminating the body (a held stream) by
// bounding the read with a timeout after which remaining frames are discarded
// and the body-so-far returned.
func readStreamBody(t *testing.T, url string, readTimeout time.Duration) string {
	t.Helper()
	resp, err := http.Post(url+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"standard","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("stream request status %d: %s", resp.StatusCode, b)
	}
	var body strings.Builder
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(&body, resp.Body)
	}()
	select {
	case <-done:
	case <-time.After(readTimeout):
		// The stream is still held open; return what we received so far.
		resp.Body.Close()
		<-done
	}
	return body.String()
}
