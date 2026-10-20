package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

func continueConfigRetries(t *testing.T, retries int) *policy.CompiledConfig {
	t.Helper()
	return continueConfigRetriesWait(t, retries, 50*time.Millisecond)
}

func continueConfig(t *testing.T) *policy.CompiledConfig {
	t.Helper()
	return continueConfigRetries(t, 0)
}

func TestServerContinueTakeoverOnFinishReasonlessClose(t *testing.T) {
	compiled := continueConfig(t)
	var bCalls int
	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			ch := make(chan model.StreamEvent, 3)
			if target.Provider == "a" {
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"from-a"}}]}`), Meaningful: true}
				close(ch)
				return ch, nil
			}
			bCalls++
			if bCalls == 1 {
				go func() {
					<-ctx.Done()
					close(ch)
				}()
				return ch, nil
			}
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"from-b"}}]}`), Meaningful: true}
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`), Meaningful: false}
			close(ch)
			return ch, nil
		},
	}
	api, runner := streamTestServer(t, compiled, executor)

	body := postStream(t, api.URL)

	if !strings.Contains(body, "from-a") {
		t.Fatalf("takeover must relay the original partial: %s", body)
	}
	if !strings.Contains(body, "from-b") {
		t.Fatalf("takeover must relay the continuation: %s", body)
	}
	if strings.Contains(body, "event: error") {
		t.Fatalf("takeover must not surface a client-facing error: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("takeover must complete the turn with [DONE]: %s", body)
	}
	if bCalls != 2 {
		t.Fatalf("provider b must be contacted exactly twice (race + continuation), got %d", bCalls)
	}
	if got := runner.AvailableTargets([]model.Target{{Provider: "a", Model: "native-model"}}); len(got) != 0 {
		t.Fatalf("provider a must cool down after its break, available: %#v", got)
	}
	if got := runner.AvailableTargets([]model.Target{{Provider: "b", Model: "native-model"}}); len(got) != 1 {
		t.Fatalf("provider b must stay healthy after the takeover, available: %#v", got)
	}
}

func TestServerContinueTakeoverOnEmptyCompletion(t *testing.T) {
	compiled := continueConfig(t)
	var bCalls int
	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			ch := make(chan model.StreamEvent, 3)
			if target.Provider == "a" {
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"reasoning_content":"leaking thought"}}]}`), Meaningful: true}
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`), Meaningful: false}
				close(ch)
				return ch, nil
			}
			bCalls++
			if bCalls == 1 {
				go func() {
					<-ctx.Done()
					close(ch)
				}()
				return ch, nil
			}
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"real-answer"}}]}`), Meaningful: true}
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`), Meaningful: false}
			close(ch)
			return ch, nil
		},
	}
	api, runner := streamTestServer(t, compiled, executor)

	body := postStream(t, api.URL)

	if strings.Contains(body, "data: [DONE]\n") && !strings.Contains(body, "real-answer") {
		t.Fatalf("empty winner must not end the turn with [DONE] without an answer: %s", body)
	}
	if !strings.Contains(body, "real-answer") {
		t.Fatalf("takeover after empty completion must relay the real answer: %s", body)
	}
	if strings.Contains(body, "event: error") {
		t.Fatalf("takeover after empty completion must not surface a client-facing error: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("takeover must complete the turn with a final [DONE]: %s", body)
	}
	if bCalls != 2 {
		t.Fatalf("provider b must be contacted exactly twice (race + continuation), got %d", bCalls)
	}
	if got := runner.AvailableTargets([]model.Target{{Provider: "a", Model: "native-model"}}); len(got) != 0 {
		t.Fatalf("provider a must cool down after an empty completion, available: %#v", got)
	}
	if got := runner.AvailableTargets([]model.Target{{Provider: "b", Model: "native-model"}}); len(got) != 1 {
		t.Fatalf("provider b must stay healthy after the takeover, available: %#v", got)
	}
}

func TestServerContinueTakeoverExcludesJustBrokenProvider(t *testing.T) {
	cfg := policy.TestConfig()
	cfg.Providers = cfg.Providers[:2] // a, b
	cfg.RoutingRules = []policy.Rule{
		policy.FilterModel("standard", "standard"),
		policy.FilterProvider("standard", "a", "b"),
		policy.MapRuleHelper("standard", "native-model"),
		policy.RankRuleHelper("standard"),
		policy.RaceRuleHelper("standard", 2),
		policy.ContinueRuleHelper("standard", 90*time.Second, "full"),
	}
	compiled, err := policy.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var aCalls, bCalls int
	winnerBreak := &model.CallError{Class: model.ErrorModelNotFound, Status: http.StatusNotFound}
	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			ch := make(chan model.StreamEvent, 3)
			if target.Provider == "a" {
				aCalls++
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"from-a"}}]}`), Meaningful: true}
				ch <- model.StreamEvent{Err: winnerBreak}
				close(ch)
				return ch, nil
			}
			bCalls++
			if bCalls == 1 {
				go func() {
					<-ctx.Done()
					close(ch)
				}()
				return ch, nil
			}
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"from-b"}}]}`), Meaningful: true}
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`), Meaningful: false}
			close(ch)
			return ch, nil
		},
	}
	api, _ := streamTestServer(t, compiled, executor)

	body := postStream(t, api.URL)

	if !strings.Contains(body, "from-b") {
		t.Fatalf("takeover must continue on the other provider (b): %s", body)
	}
	if strings.Contains(body, "event: error") {
		t.Fatalf("takeover must not surface a client-facing error: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("takeover must complete the turn with [DONE]: %s", body)
	}
	if aCalls != 1 {
		t.Fatalf("broken provider a must not be re-contacted by the takeover: calls=%d, want 1", aCalls)
	}
	if bCalls != 2 {
		t.Fatalf("provider b must be contacted for the race and the continuation: calls=%d, want 2", bCalls)
	}
}

func TestServerEmptyCompletionSurfacesErrorWithoutContinue(t *testing.T) {
	compiled := singleProviderStreamConfig(t)
	events := make(chan model.StreamEvent, 3)
	events <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"reasoning_content":"thinking"}}]}`), Meaningful: true}
	events <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`), Meaningful: false}
	close(events)
	executor := &fakeExecutor{stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		return events, nil
	}}
	api, runner := streamTestServer(t, compiled, executor)

	body := postStream(t, api.URL)

	var payload struct {
		Error struct {
			Message    string `json:"message"`
			Type       string `json:"type"`
			StatusCode int    `json:"status_code"`
			Retryable  bool   `json:"retryable"`
			Partial    bool   `json:"partial"`
		} `json:"error"`
	}
	if err := json.Unmarshal(sseErrorData(t, body), &payload); err != nil {
		t.Fatalf("structured error payload: %v\n%s", err, body)
	}
	if payload.Error.Type != "5xx" || payload.Error.StatusCode != 502 {
		t.Errorf("empty completion must classify as 5xx: %+v", payload.Error)
	}
	if !payload.Error.Retryable || !payload.Error.Partial {
		t.Errorf("empty completion must be retryable+partial: %+v", payload.Error)
	}
	if strings.Contains(body, "data: [DONE]\n") {
		t.Fatalf("empty completion without continue must not end with [DONE]: %s", body)
	}
	if got := runner.AvailableTargets([]model.Target{{Provider: "a", Model: "native-model"}}); len(got) != 0 {
		t.Fatalf("provider a must cool down after an empty completion, available: %#v", got)
	}
}

func TestServerEmptyCompletionIsNotToolCallTurn(t *testing.T) {
	compiled := singleProviderStreamConfig(t)
	events := make(chan model.StreamEvent, 3)
	events <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"bash","arguments":"{}"}}]}}]}`), Meaningful: true}
	events <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`), Meaningful: false}
	close(events)
	executor := &fakeExecutor{stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		return events, nil
	}}
	api, _ := streamTestServer(t, compiled, executor)

	body := postStream(t, api.URL)

	if strings.Contains(body, "event: error") {
		t.Fatalf("a tool-call completion must not be treated as empty: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("a tool-call completion must end with [DONE]: %s", body)
	}
	if !strings.Contains(body, `"tool_calls"`) || !strings.Contains(body, `"id":"call_1"`) {
		t.Fatalf("the tool-call delta must be relayed to the client: %s", body)
	}
}

func TestServerContinueNeverCrossesToolCallBoundary(t *testing.T) {
	compiled := continueConfig(t)
	var bCalls int
	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			ch := make(chan model.StreamEvent, 3)
			if target.Provider == "a" {
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"write","arguments":"{\"path\":\"/tmp/x.go\",\""}}]}}]}`), Meaningful: true}
				close(ch)
				return ch, nil
			}
			bCalls++
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch, nil
		},
	}
	api, runner := streamTestServer(t, compiled, executor)

	body := postStream(t, api.URL)

	if !strings.Contains(body, `"tool_calls"`) {
		t.Fatalf("the tool-call delta must be relayed: %s", body)
	}
	if strings.Contains(body, "data: [DONE]\n") {
		t.Fatalf("a truncated tool-call stream must not end with [DONE]: %s", body)
	}
	var payload struct {
		Error struct {
			Type       string `json:"type"`
			StatusCode int    `json:"status_code"`
			Retryable  bool   `json:"retryable"`
			Partial    bool   `json:"partial"`
		} `json:"error"`
	}
	if err := json.Unmarshal(sseErrorData(t, body), &payload); err != nil {
		t.Fatalf("structured error payload: %v\n%s", err, body)
	}
	if payload.Error.Type != "5xx" || payload.Error.StatusCode != 502 {
		t.Errorf("truncated tool-call must classify as 5xx: %+v", payload.Error)
	}
	if !payload.Error.Retryable || !payload.Error.Partial {
		t.Errorf("truncated tool-call must be retryable+partial: %+v", payload.Error)
	}
	if bCalls != 1 {
		t.Fatalf("takeover must not cross the tool-call boundary; provider b calls = %d, want 1 (race only)", bCalls)
	}
	if got := runner.AvailableTargets([]model.Target{{Provider: "a", Model: "native-model"}}); len(got) != 0 {
		t.Fatalf("provider a must cool down after the truncated tool-call break, available: %#v", got)
	}
}

func TestServerChainRetryFleesCoolingProvidersOnExhaustedChain(t *testing.T) {
	compiled := continueConfigRetries(t, 1)
	var callsA, callsB int
	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			ch := make(chan model.StreamEvent, 3)
			switch target.Provider {
			case "a":
				callsA++
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"from-a-part"}}]}`), Meaningful: true}
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
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"from-b-part"}}]}`), Meaningful: true}
				close(ch)
				return ch, nil
			}
			panic("unknown provider")
		},
	}
	api, runner := streamTestServer(t, compiled, executor)

	body := postStream(t, api.URL)

	if !strings.Contains(body, "from-a-part") || !strings.Contains(body, "from-b-part") {
		t.Fatalf("both provider partials must be relayed before the error: %s", body)
	}
	var payload struct {
		Error struct {
			Message    string `json:"message"`
			Type       string `json:"type"`
			StatusCode int    `json:"status_code"`
			Retryable  bool   `json:"retryable"`
			Partial    bool   `json:"partial"`
		} `json:"error"`
	}
	if err := json.Unmarshal(sseErrorData(t, body), &payload); err != nil {
		t.Fatalf("structured error payload: %v\n%s", err, body)
	}
	if !payload.Error.Retryable || !payload.Error.Partial || payload.Error.StatusCode != 502 {
		t.Fatalf("exhausted all-cooling chain must surface retryable+partial 5xx: %+v", payload.Error)
	}
	if strings.Contains(body, "data: [DONE]") {
		t.Fatalf("an exhausted all-cooling chain must not end with [DONE]: %s", body)
	}
	if callsA != 1 {
		t.Fatalf("provider a must not be re-contacted once it broke: calls=%d, want 1", callsA)
	}
	if callsB != 2 {
		t.Fatalf("provider b must be contacted for the race and the takeover only: calls=%d, want 2", callsB)
	}
	met := scrape(t, runner.Metrics())
	if !strings.Contains(met, `llm_chain_retries_total{status="started"} 1`) {
		t.Errorf("the whole-chain retry must be recorded as started:\n%s", met)
	}
	if !strings.Contains(met, `llm_chain_retries_total{status="exhausted"} 1`) {
		t.Errorf("the all-cooling retry pass must be recorded as exhausted:\n%s", met)
	}
	if strings.Contains(met, `llm_chain_retries_total{status="completed"}`) {
		t.Errorf("no winner was selected, so completed must stay absent:\n%s", met)
	}
}

func TestServerChainRetryBudgetExhausted(t *testing.T) {
	compiled := continueConfigRetries(t, 1)
	breakAll := func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		ch := make(chan model.StreamEvent, 3)
		ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"partial"}}]}`), Meaningful: true}
		close(ch)
		return ch, nil
	}
	api, runner := streamTestServer(t, compiled, &fakeExecutor{stream: breakAll})

	body := postStream(t, api.URL)

	if strings.Contains(body, "data: [DONE]") {
		t.Fatalf("an exhausted chain with a spent budget must not end with [DONE]: %s", body)
	}
	var payload struct {
		Error struct {
			Message    string `json:"message"`
			Type       string `json:"type"`
			StatusCode int    `json:"status_code"`
			Retryable  bool   `json:"retryable"`
			Partial    bool   `json:"partial"`
		} `json:"error"`
	}
	if err := json.Unmarshal(sseErrorData(t, body), &payload); err != nil {
		t.Fatalf("structured error payload: %v\n%s", err, body)
	}
	if !payload.Error.Retryable || !payload.Error.Partial {
		t.Errorf("exhausted chain with spent budget must stay retryable+partial: %+v", payload.Error)
	}
	if payload.Error.StatusCode != 502 {
		t.Errorf("exhausted chain must surface as 5xx: %+v", payload.Error)
	}
	met := scrape(t, runner.Metrics())
	if !strings.Contains(met, `llm_chain_retries_total{status="started"} 1`) {
		t.Errorf("exactly one whole-chain retry must be recorded as started:\n%s", met)
	}
	if !strings.Contains(met, `llm_chain_retries_total{status="exhausted"} 1`) {
		t.Errorf("the retry pass over an all-cooling pool must be recorded as exhausted:\n%s", met)
	}
	if strings.Contains(met, `llm_chain_retries_total{status="completed"}`) {
		t.Errorf("no winner was selected (all providers cooling), so completed must stay absent:\n%s", met)
	}
}

func TestServerChainRetryBudgetZeroKeepsOldBehavior(t *testing.T) {
	compiled := continueConfig(t) // retries 0
	breakAll := func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		ch := make(chan model.StreamEvent, 3)
		ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"partial"}}]}`), Meaningful: true}
		close(ch)
		return ch, nil
	}
	api, runner := streamTestServer(t, compiled, &fakeExecutor{stream: breakAll})

	body := postStream(t, api.URL)

	if strings.Contains(body, "data: [DONE]") {
		t.Fatalf("retries=0 must keep surfacing terminal errors, not [DONE]: %s", body)
	}
	sseErrorData(t, body)
	if got := scrape(t, runner.Metrics()); strings.Contains(got, "llm_chain_retries_total") {
		t.Errorf("retries=0 must not emit any whole-chain retry series:\n%s", got)
	}
}

func TestServerContinueFinalizesCompleteToolCallOnPrematureClose(t *testing.T) {
	compiled := continueConfig(t)
	var bCalls int
	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			ch := make(chan model.StreamEvent, 4)
			if target.Provider == "a" {
				ch <- model.StreamEvent{Data: []byte(`{"id":"chatcmpl-vertex","choices":[{"index":0,"delta":{"reasoning_content":"thinking..."}}]}`), Meaningful: true}
				ch <- model.StreamEvent{Data: []byte(`{"id":"chatcmpl-vertex","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"bash","arguments":"{\"command\":\"git status\"}"}}]}}]}`), Meaningful: true}
				close(ch)
				return ch, nil
			}
			bCalls++
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch, nil
		},
	}
	api, runner := streamTestServer(t, compiled, executor)

	body := postStream(t, api.URL)

	if strings.Contains(body, "event: error") || strings.Contains(body, "upstream stream failed") {
		t.Fatalf("a complete tool-call stream under continue must finalize cleanly, not surface an error: %s", body)
	}
	if !strings.Contains(body, `"finish_reason":"tool_calls"`) {
		t.Fatalf("a complete tool-call stream must emit synthetic finish_reason=tool_calls: %s", body)
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Fatalf("a finalized tool-call stream must end with [DONE]: %s", body)
	}
	if bCalls != 1 {
		t.Fatalf("finalizing an already-complete tool call must not re-dispatch to provider b: calls=%d, want 1", bCalls)
	}
	if got := runner.AvailableTargets([]model.Target{{Provider: "a", Model: "native-model"}}); len(got) != 0 {
		t.Fatalf("provider a must still cool down after dropping the stream before finish_reason, available: %#v", got)
	}
}

func TestServerStreamErrorAfterFinishReasonCompletesCleanly(t *testing.T) {
	compiled := singleProviderStreamConfig(t)
	events := make(chan model.StreamEvent, 4)
	events <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"done answer"}}]}`), Meaningful: true}
	events <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`), Meaningful: false}
	events <- model.StreamEvent{Err: &model.CallError{Class: model.ErrorConnection, Status: 502}}
	close(events)
	executor := &fakeExecutor{stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		return events, nil
	}}
	api, _ := streamTestServer(t, compiled, executor)

	body := postStream(t, api.URL)

	if strings.Contains(body, "event: error") {
		t.Fatalf("stream error after finish_reason must not emit event: error: %s", body)
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Fatalf("stream error after finish_reason must end with [DONE]: %s", body)
	}
}
