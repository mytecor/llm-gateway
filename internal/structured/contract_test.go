package structured

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/scheduler"
)

func strictBody(schema string) []byte {
	return []byte(`{"model":"standard","messages":[{"role":"user","content":"original"}],"response_format":{"type":"json_schema","json_schema":{"name":"test","strict":true,"schema":` + schema + `}}}`)
}
func contract(t *testing.T, schema string) *Contract {
	t.Helper()
	c, err := Parse(strictBody(schema))
	if err != nil || c == nil {
		t.Fatalf("compile: %v", err)
	}
	return c
}
func completion(text string) []byte {
	b, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text}, "finish_reason": "stop"}}})
	return b
}

func TestContractActivationAndEarlyRejections(t *testing.T) {
	for _, body := range []string{`{}`, `{"tools":"opaque"}`, `{"response_format":{"type":"json_object"}}`, `{"response_format":{"type":"json_schema","json_schema":{"strict":false,"schema":42}}}`} {
		c, err := Parse([]byte(body))
		if c != nil || err != nil {
			t.Fatalf("changed pass-through: %s", body)
		}
	}
	for _, test := range []struct{ body, code string }{
		{string(strictBody(`{"type":42}`)), "invalid_json_schema"},
		{string(strictBody(`{"$ref":"https://unreachable.invalid/schema"}`)), "invalid_json_schema"},
		{string(strictBody(`{"$ref":"file:///etc/passwd"}`)), "invalid_json_schema"},
		{strings.Replace(string(strictBody(`{}`)), `"model":`, `"n":2,"model":`, 1), "structured_output_n_unsupported"},
		{strings.Replace(string(strictBody(`{}`)), `"model":`, `"tools":[{}],"model":`, 1), "structured_output_tools_unsupported"},
	} {
		c, err := Parse([]byte(test.body))
		if c != nil || err == nil || err.Status != 400 || err.Code != test.code {
			t.Fatalf("%s: %#v", test.code, err)
		}
	}
}

func TestExactJSONPrecisionAndLocalReferences(t *testing.T) {
	c := contract(t, `{"$defs":{"id":{"type":"integer","const":9007199254740993}},"type":"object","properties":{"id":{"$ref":"#/$defs/id"}},"required":["id"],"additionalProperties":false}`)
	if err := c.Validate(`{"id":9007199254740993}`); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{`{"id":9007199254740992}`, `{"id":9007199254740993} {}`, "```json\n{}\n```", `{"id":9007199254740993,"extra":true}`, `null`, `{"id":1`} {
		if c.Validate(bad) == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}

func TestFeedbackPointersAndBounds(t *testing.T) {
	c := contract(t, `{"type":"object","properties":{"a/b~c":{"type":"number","maximum":1}}}`)
	feedback := Feedback(c.Validate(`{"a/b~c":2}`))
	if !strings.Contains(feedback, "/a~1b~0c") || !strings.Contains(feedback, "maximum") || !strings.Contains(feedback, "Do not repeat") {
		t.Fatal(feedback)
	}
	properties := map[string]any{}
	value := map[string]any{}
	for i := 0; i < 20; i++ {
		key := strings.Repeat("字", 1000) + string(rune('a'+i))
		properties[key] = map[string]any{"type": "number"}
		value[key] = "bad"
	}
	schema, _ := json.Marshal(map[string]any{"properties": properties})
	body, _ := json.Marshal(value)
	feedback = Feedback(contract(t, string(schema)).Validate(string(body)))
	if len(feedback) > maxFeedbackBytes || !utf8.ValidString(feedback) || strings.Count(feedback, "\n- ") > maxFeedbackErrors {
		t.Fatal("feedback bounds")
	}
}

func TestCorrectionHistoryBudgetAndUsage(t *testing.T) {
	for _, validAt := range []int{0, 1, 2, 3} {
		t.Run(string(rune('0'+validAt)), func(t *testing.T) {
			body := strictBody(`{"type":"integer","minimum":0}`)
			original := string(body)
			calls := 0
			result, err := Run(context.Background(), contract(t, `{"type":"integer","minimum":0}`), model.ExecuteRequest{Kind: model.RequestChat, Body: body}, func(_ context.Context, req model.ExecuteRequest) (*Generation, *model.CallError) {
				var wire struct {
					Model          string
					Messages       []struct{ Role, Content string }
					ResponseFormat json.RawMessage `json:"response_format"`
				}
				_ = json.Unmarshal(req.Body, &wire)
				if wire.Model != "standard" || len(wire.Messages) != 1+2*calls || !strings.Contains(string(wire.ResponseFormat), `"strict":true`) {
					t.Fatalf("lost context: %s", req.Body)
				}
				if calls > 0 && (wire.Messages[len(wire.Messages)-2].Role != "assistant" || !strings.Contains(wire.Messages[len(wire.Messages)-1].Content, "minimum")) {
					t.Fatal("missing feedback")
				}
				content := "-1"
				if calls == validAt {
					content = "1"
				}
				calls++
				return &Generation{Outcome: model.RunOutcome{Body: completion(content), Provider: string(rune('a' + calls)), Attempts: 2, Usage: model.UsageSummary{Input: 3, Output: 1}, HasUsage: true}}, nil
			}, nil)
			expected := min(validAt+1, 3)
			if calls != expected || string(body) != original {
				t.Fatalf("calls=%d original changed=%v", calls, string(body) != original)
			}
			if validAt == 3 {
				if err == nil || err.Code != "structured_output_validation_failed" {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if result.Outcome.Usage.Input != int64(expected*3) || result.Outcome.Attempts != expected*2 {
				t.Fatalf("lost accounting: %+v", result.Outcome)
			}
		})
	}
}

func TestRefusalAndToolsAreTerminal(t *testing.T) {
	for _, test := range []struct {
		body string
		code string
	}{
		{`{"choices":[{"index":0,"message":{"content":null,"refusal":"declined"},"finish_reason":"stop"}]}`, ""},
		{`{"choices":[{"index":0,"message":{"content":null,"tool_calls":[{}]},"finish_reason":"tool_calls"}]}`, "structured_output_unexpected_tool_calls"},
	} {
		calls := 0
		_, err := Run(context.Background(), contract(t, `{"type":"object"}`), model.ExecuteRequest{}, func(context.Context, model.ExecuteRequest) (*Generation, *model.CallError) {
			calls++
			return &Generation{Outcome: model.RunOutcome{Body: []byte(test.body)}}, nil
		}, nil)
		if calls != 1 || (test.code == "" && err != nil) || (test.code != "" && (err == nil || err.Code != test.code)) {
			t.Fatalf("calls=%d err=%v", calls, err)
		}
	}
}

func TestDeadlineNeverExtendsBudgets(t *testing.T) {
	start := time.Now()
	for _, test := range []struct{ client, route, want time.Duration }{{0, 0, 30 * time.Minute}, {time.Minute, 0, time.Minute}, {time.Minute, 2 * time.Minute, time.Minute}, {time.Minute, time.Second, time.Second}} {
		ctx := context.Background()
		cancel := func() {}
		if test.client > 0 {
			ctx, cancel = context.WithDeadline(ctx, start.Add(test.client))
		}
		outer, stop := Deadline(ctx, start, test.route)
		deadline, _ := outer.Deadline()
		if !deadline.Equal(start.Add(test.want)) {
			t.Fatalf("deadline=%s", deadline)
		}
		stop()
		cancel()
	}
}

func TestBufferRejectsOverflowAndDoesNotLeakGenerations(t *testing.T) {
	b := &Buffer{}
	chunk := func(text string) model.StreamEvent {
		raw, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"content": text}}}})
		return model.StreamEvent{Data: raw}
	}
	if !b.Write(chunk(`{"id":`)) || !b.Write(chunk(`1}`)) {
		t.Fatal(b.Err)
	}
	if text, _, err := Answer(b.Body()); err != nil || text != `{"id":1}` {
		t.Fatal(text, err)
	}
	if b.Write(chunk(strings.Repeat("x", MaxBufferBytes))) || b.Err == nil || b.Err.Code != "structured_output_buffer_exceeded" {
		t.Fatal("overflow accepted")
	}
	calls := 0
	b = &Buffer{KeepWaiting: func() bool { calls++; return false }}
	if b.KeepAlive(scheduler.StreamHeartbeat{}) || calls != 1 {
		t.Fatal("disconnect ignored")
	}
}

func TestInfrastructureFailureDoesNotStartSchemaCorrections(t *testing.T) {
	calls := 0
	expected := &model.CallError{Class: model.ErrorRateLimit, Status: 429}
	_, err := Run(context.Background(), contract(t, `{"type":"integer"}`), model.ExecuteRequest{}, func(context.Context, model.ExecuteRequest) (*Generation, *model.CallError) {
		calls++
		return nil, expected
	}, nil)
	if err != expected || calls != 1 {
		t.Fatalf("infrastructure failure became correction: calls=%d err=%v", calls, err)
	}
}

func TestMissingUsageAndStreamAggregate(t *testing.T) {
	b := &Buffer{}
	for _, raw := range []string{
		`{"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"1"},"finish_reason":null}],"usage":{"prompt_tokens":2}}`,
		`{"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		`{"id":"c","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1}}`,
	} {
		if !b.Write(model.StreamEvent{Data: []byte(raw)}) {
			t.Fatal(b.Err)
		}
	}
	for _, include := range []bool{false, true} {
		events := PublicEvents(b.Events, model.UsageSummary{Input: 5, Output: 3}, true, include)
		count := 0
		for _, event := range events {
			if usage, ok := model.ExtractUsageFull(event.Data); ok {
				count++
				if usage.Input != 5 || usage.Output != 3 {
					t.Fatal(usage)
				}
			}
		}
		expected := 0
		if include {
			expected = 1
		}
		if count != expected {
			t.Fatalf("usage repeated %d times", count)
		}
	}
	result, err := Run(context.Background(), contract(t, `{"type":"integer"}`), model.ExecuteRequest{}, func(context.Context, model.ExecuteRequest) (*Generation, *model.CallError) {
		return &Generation{Outcome: model.RunOutcome{Body: completion("1")}}, nil
	}, nil)
	if err != nil || result.Outcome.HasUsage || strings.Contains(string(result.Outcome.Body), `"usage"`) {
		t.Fatal("invented missing usage")
	}
}
