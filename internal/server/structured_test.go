package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/scheduler"
	"github.com/mytecor/llm-gateway/internal/structured"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"
)

const strictFormat = `"response_format":{"type":"json_schema","json_schema":{"name":"answer","strict":true,"schema":{"type":"object","properties":{"value":{"type":"integer","minimum":0}},"required":["value"],"additionalProperties":false}}}`

func strictRequest(stream bool) string {
	flag := "false"
	if stream {
		flag = "true"
	}
	return `{"model":"standard","messages":[{"role":"user","content":"original"}],"stream":` + flag + `,"stream_options":{"include_usage":true},` + strictFormat + `}`
}
func strictCompletion(text string) []byte {
	data, _ := json.Marshal(map[string]any{"id": "c", "model": "native-model", "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}})
	return data
}
func strictChunk(delta map[string]any, finish any) model.StreamEvent {
	data, _ := json.Marshal(map[string]any{"id": "c", "model": "native-model", "object": "chat.completion.chunk", "choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}})
	return model.StreamEvent{Data: data, Meaningful: true}
}
func strictConfig() config.Config {
	cfg := configtest.TestConfig()
	cfg.Providers = cfg.Providers[:1]
	cfg.Rules = policytest.EntryRules([]string{"a"}, 1)
	return cfg
}

func TestStrictCorrectionsShareRequestBudget(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, tc := range []struct {
			name                          string
			calls, perProvider, wantCalls int
			validSecond                   bool
		}{
			{"total", 1, 3, 1, true},
			{"per-provider", 3, 1, 1, true},
			{"successful-correction", 2, 2, 2, true},
			{"exhausted-corrections", 2, 3, 2, false},
		} {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, tc.name), func(t *testing.T) {
				cfg := strictConfig()
				cfg.Rules = nil
				zero := 0
				cfg.Models = map[string]policy.ModelSpec{"standard": {
					Budget: &policy.BudgetSpec{MaxCalls: tc.calls, MaxInFlight: 1, MaxCallsPerProvider: tc.perProvider, MaxHedges: &zero},
					Tiers:  []policy.TierSpec{{Targets: []policy.TargetSpec{{Provider: "a", Native: "native-model"}}}},
				}}
				var calls atomic.Int32
				answer := func() string {
					if calls.Add(1) > 1 && tc.validSecond {
						return `{"value":1}`
					}
					return `{"value":-1}`
				}
				exec := &fakeExecutor{
					do: func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError) {
						return strictCompletion(answer()), nil
					},
					stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
						ch := make(chan model.StreamEvent, 2)
						ch <- strictChunk(map[string]any{"content": answer()}, nil)
						ch <- strictChunk(map[string]any{}, "stop")
						close(ch)
						return ch, nil
					},
				}
				api, read := eventsTestServer(t, cfg, exec)
				resp, body := postCompletion(t, api.URL, strictRequest(stream))
				if calls.Load() != int32(tc.wantCalls) {
					t.Fatalf("calls=%d, want %d, body=%s", calls.Load(), tc.wantCalls, body)
				}
				success := tc.validSecond && tc.wantCalls == 2
				if (resp.StatusCode == 200) != success {
					t.Fatalf("status=%d, want success=%t, body=%s", resp.StatusCode, success, body)
				}
				if success {
					for _, event := range read() {
						if event["event"] == "request_completed" && event["attempts"] != float64(2) {
							t.Fatalf("double-counted generation attempts: %v", event)
						}
					}
				}
			})
		}
	}
}

func TestStrictHTTPReRoutesAndAccountsOnce(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			cfg := configtest.TestConfig()
			cfg.Providers = cfg.Providers[:2]
			cfg.Rules = policy.Rules{policytest.FilterModel("standard", "standard"), policytest.FilterProvider("standard", "a", "b"), policytest.MapRuleHelper("standard", "native-model"), policytest.RankRuleHelper("standard"), policytest.BalanceRuleHelper("standard", "round_robin"), policytest.RaceRuleHelper("standard", 1), policytest.AdmissionRuleHelper("standard", 1, 0, time.Second)}
			var calls atomic.Int32
			var firstProvider string
			response := func(target model.Target, req model.ExecuteRequest) string {
				n := calls.Add(1)
				var wire struct {
					Model    string
					Messages []struct{ Role, Content string }
					Format   json.RawMessage `json:"response_format"`
				}
				if json.Unmarshal(req.Body, &wire) != nil || wire.Model != "standard" || !strings.Contains(string(wire.Format), `"strict":true`) {
					t.Error("changed request contract")
				}
				if n == 1 {
					firstProvider = target.Provider
					return `{"value":-1}`
				}
				if target.Provider == firstProvider || len(wire.Messages) != 3 || wire.Messages[1].Role != "assistant" || !strings.Contains(wire.Messages[2].Content, "minimum") {
					t.Error("correction did not reroute with feedback")
				}
				return `{"value":1}`
			}
			executor := &fakeExecutor{}
			executor.do = func(_ context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
				return strictCompletion(response(target, req)), nil
			}
			executor.stream = func(_ context.Context, target model.Target, req model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
				text := response(target, req)
				ch := make(chan model.StreamEvent, 4)
				ch <- strictChunk(map[string]any{"reasoning_content": "PRIVATE_REJECTED_REASONING"}, nil)
				if calls.Load() > 1 {
					<-ch
				}
				ch <- strictChunk(map[string]any{"content": text}, nil)
				ch <- strictChunk(map[string]any{}, "stop")
				ch <- model.StreamEvent{Data: []byte(`{"id":"c","object":"chat.completion.chunk","model":"native-model","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`)}
				close(ch)
				return ch, nil
			}
			api, read := eventsTestServer(t, cfg, executor)
			resp, body := postCompletion(t, api.URL, strictRequest(stream))
			if resp.StatusCode != 200 || calls.Load() != 2 || strings.Contains(body, `-1`) || strings.Contains(body, "PRIVATE_REJECTED") || strings.Contains(body, "native-model") {
				t.Fatalf("status=%d calls=%d body=%s", resp.StatusCode, calls.Load(), body)
			}
			if !strings.Contains(body, `"prompt_tokens":6`) || !strings.Contains(body, `"completion_tokens":4`) {
				t.Fatal("missing aggregate usage", body)
			}
			completed := 0
			for _, event := range read() {
				if event["event"] == "request_completed" {
					completed++
					if event["input_tokens"] != float64(6) {
						t.Fatal(event)
					}
				}
			}
			if completed != 1 {
				t.Fatalf("completion observed %d times", completed)
			}
			var metrics bytes.Buffer
			if err := api.Config.Handler.(*Server).metrics.WriteExposition(&metrics); err != nil {
				t.Fatal(err)
			}
			text := metrics.String()
			for _, provider := range []string{"a", "b"} {
				if !strings.Contains(text, `llm_input_tokens_total{api_key="",model="standard",provider="`+provider+`",native_model="native-model"} 3`) {
					t.Fatalf("provider usage lost: %s", text)
				}
			}
			if !strings.Contains(text, `llm_structured_corrections_total{model="standard"} 1`) || !strings.Contains(text, `status="success"} 1`) || strings.Contains(text, "PRIVATE_REJECTED") {
				t.Fatalf("incorrect request metrics: %s", text)
			}
		})
	}
}

func TestStrictExhaustionRefusalToolsAndPassThrough(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			var calls atomic.Int32
			executor := &fakeExecutor{do: func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError) {
				calls.Add(1)
				return strictCompletion("PRIVATE_INVALID"), nil
			}, stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
				calls.Add(1)
				ch := make(chan model.StreamEvent, 2)
				ch <- strictChunk(map[string]any{"content": "PRIVATE_INVALID"}, nil)
				ch <- strictChunk(map[string]any{}, "stop")
				close(ch)
				return ch, nil
			}}
			api, read := eventsTestServer(t, strictConfig(), executor)
			resp, body := postCompletion(t, api.URL, strictRequest(stream))
			if resp.StatusCode != 502 || !strings.Contains(body, "structured_output_validation_failed") || strings.Contains(body, "PRIVATE_INVALID") || calls.Load() != 3 {
				t.Fatalf("status=%d calls=%d body=%s", resp.StatusCode, calls.Load(), body)
			}
			failures := 0
			for _, event := range read() {
				if event["event"] == "request_failed" {
					failures++
				}
			}
			if failures != 1 {
				t.Fatalf("failed observed %d times", failures)
			}
			// Tool/schema/n validation happens before upstream dispatch.
			for _, bad := range []string{strings.Replace(strictRequest(stream), `"model":`, `"tools":[{}],"model":`, 1), strings.Replace(strictRequest(stream), `"model":`, `"n":2,"model":`, 1), strings.Replace(strictRequest(stream), `"type":"object"`, `"type":42`, 1)} {
				resp, _ = postCompletion(t, api.URL, bad)
				if resp.StatusCode != 400 || calls.Load() != 3 {
					t.Fatal("invalid request dispatched")
				}
			}
			// Ordinary streaming requests still relay their first provider response.
			plain := strings.Replace(strictRequest(stream), `"strict":true`, `"strict":false`, 1)
			resp, body = postCompletion(t, api.URL, plain)
			if resp.StatusCode != 200 || calls.Load() != 4 || !strings.Contains(body, "PRIVATE_INVALID") {
				t.Fatalf("pass-through changed: %s", body)
			}
		})
	}
}

func TestStrictRefusalAndUnexpectedTools(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, refusal := range []bool{false, true} {
			t.Run(map[bool]string{false: "json", true: "sse"}[stream]+map[bool]string{false: "tools", true: "refusal"}[refusal], func(t *testing.T) {
				calls := 0
				executor := &fakeExecutor{}
				executor.do = func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError) {
					calls++
					body := `{"choices":[{"index":0,"message":{"content":null,"tool_calls":[{}]},"finish_reason":"tool_calls"}]}`
					if refusal {
						body = `{"choices":[{"index":0,"message":{"content":null,"refusal":"declined"},"finish_reason":"stop"}]}`
					}
					return []byte(body), nil
				}
				executor.stream = func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
					calls++
					ch := make(chan model.StreamEvent, 2)
					delta := map[string]any{"tool_calls": []any{map[string]any{"index": 0}}}
					finish := "tool_calls"
					if refusal {
						delta = map[string]any{"refusal": "declined"}
						finish = "stop"
					}
					ch <- strictChunk(delta, nil)
					ch <- strictChunk(map[string]any{}, finish)
					close(ch)
					return ch, nil
				}
				api, _ := eventsTestServer(t, strictConfig(), executor)
				resp, body := postCompletion(t, api.URL, strictRequest(stream))
				expected := 502
				if refusal {
					expected = 200
				}
				if calls != 1 || resp.StatusCode != expected || (refusal && !strings.Contains(body, "declined")) {
					t.Fatalf("calls=%d status=%d body=%s", calls, resp.StatusCode, body)
				}
			})
		}
	}
}

func TestStrictDeadlineAndDisconnectReleaseGeneration(t *testing.T) {
	var active atomic.Int32
	executor := &fakeExecutor{stream: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		active.Add(1)
		ch := make(chan model.StreamEvent)
		go func() {
			defer active.Add(-1)
			defer close(ch)
			select {
			case ch <- strictChunk(map[string]any{"content": "PRIVATE_PARTIAL"}, nil):
			case <-ctx.Done():
				return
			}
			<-ctx.Done()
		}()
		return ch, nil
	}}
	cfg := strictConfig()
	cfg.Rules = append(cfg.Rules, policytest.TimeoutRuleHelper("standard", 50*time.Millisecond))
	api, read := eventsTestServer(t, cfg, executor)
	resp, body := postCompletion(t, api.URL, strictRequest(true))
	if resp.StatusCode != 504 || strings.Contains(body, "PRIVATE_PARTIAL") {
		t.Fatalf("deadline failed: %d %s", resp.StatusCode, body)
	}
	if active.Load() != 0 {
		t.Fatal("generation remains active")
	}
	for _, event := range read() {
		if event["event"] == "request_completed" {
			t.Fatal("deadline reported success")
		}
	}
	// A cancellation during an attached stream also releases all upstream work.
	cfg = strictConfig()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(strictRequest(true))).WithContext(ctx)
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	catalog := subsystem.NewCatalog(compiled)
	handler := NewServer(compiled, catalog, scheduler.NewRunner(compiled.RoutingPlan(), catalog, executor))
	done := make(chan struct{})
	go func() { handler.ServeHTTP(httptest.NewRecorder(), req); close(done) }()
	deadline := time.After(time.Second)
	for active.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("no generation")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancel did not stop request")
	}
	for active.Load() != 0 {
		select {
		case <-deadline:
			t.Fatal("upstream not cancelled")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestStrictSSEHeartbeatAndErrorHasNoPartial(t *testing.T) {
	writer := httptest.NewRecorder()
	sink := &sseStreamSink{writer: writer, flusher: writer, logical: "standard", kind: model.RequestChat, requestID: "r", strict: true}
	if !sink.KeepAlive(scheduler.StreamHeartbeat{}) {
		t.Fatal("heartbeat failed")
	}
	sink.Fail(structured.Error(502, "structured_output_validation_failed"), false)
	body := writer.Body.String()
	if !strings.Contains(body, `"delta":{}`) || !strings.Contains(body, `"partial":false`) || strings.Contains(body, "reasoning_content") || strings.Contains(body, "[DONE]") {
		t.Fatal(body)
	}
}

func TestStrictBufferOverflowFailsClosed(t *testing.T) {
	executor := &fakeExecutor{stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		ch := make(chan model.StreamEvent, 1)
		ch <- strictChunk(map[string]any{"content": strings.Repeat("x", structured.MaxBufferBytes)}, nil)
		close(ch)
		return ch, nil
	}}
	api, _ := eventsTestServer(t, strictConfig(), executor)
	resp, body := postCompletion(t, api.URL, strictRequest(true))
	if resp.StatusCode != 502 || !strings.Contains(body, "structured_output_buffer_exceeded") || len(body) > 1024 {
		t.Fatalf("status=%d body bytes=%d", resp.StatusCode, len(body))
	}
}

func strictLengthCompletion(text string) []byte {
	data, _ := json.Marshal(map[string]any{"id": "c", "model": "native-model", "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text}, "finish_reason": "length"}}, "usage": map[string]any{"prompt_tokens": 3, "completion_tokens": 128, "total_tokens": 131}})
	return data
}

func TestStrictIncompleteOutputFailsOverAcrossPoolAndTiers(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, failSecondTierZero := range []bool{false, true} {
			name := fmt.Sprintf("stream=%t/cross-tier=%t", stream, failSecondTierZero)
			t.Run(name, func(t *testing.T) {
				cfg := configtest.TestConfig()
				cfg.Rules = nil
				zero := 0
				cfg.Models = map[string]policy.ModelSpec{"standard": {
					Profile: "balanced",
					Budget:  &policy.BudgetSpec{MaxCalls: 4, MaxInFlight: 1, MaxCallsPerProvider: 1, MaxHedges: &zero},
					Tiers: []policy.TierSpec{
						{Targets: []policy.TargetSpec{{Provider: "a", Native: "native-model"}, {Provider: "b", Native: "native-model"}}},
						{Targets: []policy.TargetSpec{{Provider: "c", Native: "native-model"}}},
					},
				}}
				var calls []string
				var firstTier0 atomic.Int32
				exec := &fakeExecutor{
					do: func(_ context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
						calls = append(calls, target.Provider)
						if bytes.Contains(req.Body, []byte("invalid JSON Schema")) {
							t.Fatalf("unexpected validation correction message on incomplete failover: %s", req.Body)
						}
						if target.Provider != "c" && (failSecondTierZero || firstTier0.Add(1) == 1) {
							return strictLengthCompletion(`{"value":`), nil
						}
						return strictCompletion(`{"value":42}`), nil
					},
					stream: func(_ context.Context, target model.Target, req model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
						calls = append(calls, target.Provider)
						if bytes.Contains(req.Body, []byte("invalid JSON Schema")) {
							t.Fatalf("unexpected validation correction message on incomplete failover: %s", req.Body)
						}
						ch := make(chan model.StreamEvent, 2)
						if target.Provider != "c" && (failSecondTierZero || firstTier0.Add(1) == 1) {
							ch <- strictChunk(map[string]any{"content": `{"value":`}, nil)
							ch <- strictChunk(map[string]any{}, "length")
						} else {
							ch <- strictChunk(map[string]any{"content": `{"value":42}`}, nil)
							ch <- strictChunk(map[string]any{}, "stop")
						}
						close(ch)
						return ch, nil
					},
				}
				api, _ := eventsTestServer(t, cfg, exec)
				resp, body := postCompletion(t, api.URL, strictRequest(stream))
				if resp.StatusCode != 200 || !strings.Contains(body, `42`) {
					t.Fatalf("status=%d body=%s calls=%v", resp.StatusCode, body, calls)
				}
				wantCalls := 2
				if failSecondTierZero {
					wantCalls = 3
				}
				if len(calls) != wantCalls {
					t.Fatalf("calls=%v, want %d", calls, wantCalls)
				}
				var metrics bytes.Buffer
				if err := api.Config.Handler.(*Server).metrics.WriteExposition(&metrics); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(metrics.String(), `llm_structured_corrections_total{model="standard"}`) {
					t.Fatalf("clean failover should not increment correction metric: %s", metrics.String())
				}
			})
		}
	}
}

func TestStrictIncompleteOutputDownweightsProviderInExpectedTTFT(t *testing.T) {
	cfg := configtest.TestConfig()
	cfg.Providers = cfg.Providers[:2]
	cfg.Providers[0].Priority = 100
	cfg.Providers[1].Priority = 50
	cfg.Rules = nil
	zero := 0
	cfg.Models = map[string]policy.ModelSpec{"standard": {
		Profile: "balanced",
		Budget:  &policy.BudgetSpec{MaxCalls: 3, MaxInFlight: 1, MaxCallsPerProvider: 1, MaxHedges: &zero},
		Tiers: []policy.TierSpec{
			{Targets: []policy.TargetSpec{{Provider: "a", Native: "native-model"}, {Provider: "b", Native: "native-model"}}},
		},
	}}
	var aCalls, bCalls atomic.Int32
	exec := &fakeExecutor{
		do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
			if target.Provider == "a" {
				aCalls.Add(1)
				return strictLengthCompletion(`{`), nil
			}
			time.Sleep(25 * time.Millisecond)
			bCalls.Add(1)
			return strictCompletion(`{"value":7}`), nil
		},
	}
	api, _ := eventsTestServer(t, cfg, exec)
	for i := 0; i < 6; i++ {
		resp, body := postCompletion(t, api.URL, strictRequest(false))
		if resp.StatusCode != 200 || !strings.Contains(body, `7`) {
			t.Fatalf("iter %d: status=%d body=%s", i, resp.StatusCode, body)
		}
	}
	// Request 1 hits "a" (truncated) then fails over to "b", tripping "a"'s error
	// budget so later requests pick "b" directly without hitting "a" every time.
	if aCalls.Load() >= 4 || bCalls.Load() != 6 {
		t.Fatalf("expected expected-ttft health penalty to steer away from broken a: aCalls=%d bCalls=%d", aCalls.Load(), bCalls.Load())
	}
}

func TestStrictValidationErrorFailoverMovesCorrectionToPeer(t *testing.T) {
	cfg := configtest.TestConfig()
	cfg.Providers = cfg.Providers[:2]
	cfg.Rules = nil
	zero := 0
	cfg.Models = map[string]policy.ModelSpec{"standard": {
		Profile: "balanced",
		Budget:  &policy.BudgetSpec{MaxCalls: 3, MaxInFlight: 1, MaxCallsPerProvider: 1, MaxHedges: &zero},
		Tiers: []policy.TierSpec{
			{Targets: []policy.TargetSpec{{Provider: "a", Native: "native-model"}, {Provider: "b", Native: "native-model"}}},
		},
	}}
	var calls []string
	exec := &fakeExecutor{
		do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
			calls = append(calls, target.Provider)
			if target.Provider == "a" {
				return strictCompletion(`{"value":-1}`), nil
			}
			return strictCompletion(`{"value":5}`), nil
		},
	}
	api, _ := eventsTestServer(t, cfg, exec)
	resp, body := postCompletion(t, api.URL, strictRequest(false))
	if resp.StatusCode != 200 || !strings.Contains(body, `5`) {
		t.Fatalf("status=%d body=%s calls=%v", resp.StatusCode, body, calls)
	}
	if len(calls) != 2 || calls[0] != "a" || calls[1] != "b" {
		t.Fatalf("expected initial attempt on a and correction on b, got %v", calls)
	}
	var metrics bytes.Buffer
	if err := api.Config.Handler.(*Server).metrics.WriteExposition(&metrics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics.String(), `llm_structured_corrections_total{model="standard"} 1`) {
		t.Fatalf("missing correction metric: %s", metrics.String())
	}
}

func TestStrictIncompleteOutputRollsPeerWhenCoolingWithPerProviderBudget(t *testing.T) {
	cfg := configtest.TestConfig()
	cfg.Providers = cfg.Providers[:2]
	cfg.Rules = nil
	zero := 0
	cfg.Models = map[string]policy.ModelSpec{"standard": {
		Profile: "balanced",
		Budget:  &policy.BudgetSpec{MaxCalls: 4, MaxInFlight: 1, MaxCallsPerProvider: 2, MaxHedges: &zero},
		Tiers: []policy.TierSpec{
			{Targets: []policy.TargetSpec{{Provider: "a", Native: "native-model"}, {Provider: "b", Native: "native-model"}}},
		},
	}}
	var aCalls, bCalls atomic.Int32
	exec := &fakeExecutor{
		do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
			if target.Provider == "b" {
				bCalls.Add(1)
				return nil, &model.CallError{Class: model.ErrorTimeout, Status: 504, Scope: model.FailureScopeProvider, Replay: model.ReplayNotSent}
			}
			if aCalls.Add(1) == 1 {
				return strictLengthCompletion(`{`), nil
			}
			return strictCompletion(`{"value":99}`), nil
		},
	}
	api, _ := eventsTestServer(t, cfg, exec)
	resp, body := postCompletion(t, api.URL, strictRequest(false))
	if resp.StatusCode != 200 || !strings.Contains(body, `99`) {
		t.Fatalf("status=%d body=%s aCalls=%d bCalls=%d", resp.StatusCode, body, aCalls.Load(), bCalls.Load())
	}
	if aCalls.Load() != 2 || bCalls.Load() != 1 {
		t.Fatalf("expected second roll on a after b cooled, got aCalls=%d bCalls=%d", aCalls.Load(), bCalls.Load())
	}
}
