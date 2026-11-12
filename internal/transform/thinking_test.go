package transform

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/testsupport/executortest"
)

func response(content, finish string) []byte {
	data, _ := json.Marshal(map[string]any{"id": "response", "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": content}, "finish_reason": finish}}, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 2}})
	return data
}
func chunk(content, finish string) model.StreamEvent {
	data, _ := json.Marshal(map[string]any{"id": "stream", "choices": []any{map[string]any{"index": 0, "delta": map[string]any{"content": content}, "finish_reason": finish}}})
	return model.StreamEvent{Data: data, Meaningful: content != ""}
}

func TestThinkingSplitterEveryBoundaryAndLiterals(t *testing.T) {
	for _, test := range []struct{ input, content, reason string }{
		{`<think>plan</think>{"ok":true}`, `{"ok":true}`, "plan"},
		{" \n<think>Сначала 🌏, затем мир</think>\n{\"ok\":true}", " \n\n{\"ok\":true}", "Сначала 🌏, затем мир"},
		{`{"text":"<think>literal</think>"}`, `{"text":"<think>literal</think>"}`, ""},
		{`"<think>literal</think>"`, `"<think>literal</think>"`, ""},
		{`prefix <think>literal</think>`, `prefix <think>literal</think>`, ""},
		{`<thi`, `<thi`, ""},
		{`<think></think>{}`, `{}`, ""},
	} {
		for index := 0; index <= len(test.input); index++ {
			if !utf8.ValidString(test.input[:index]) {
				continue
			}
			parser := &thinkingSplitter{}
			content, reason := "", ""
			for _, part := range []string{test.input[:index], test.input[index:]} {
				c, r := parser.feed(part)
				content += c
				reason += r
				if len(parser.pending) >= len(closeThinking) || !utf8.ValidString(c+r) {
					t.Fatalf("unbounded/invalid UTF8 pending at %d", index)
				}
			}
			c, r, err := parser.finish(false)
			content += c
			reason += r
			if err != nil || content != test.content || reason != test.reason {
				t.Fatalf("input=%q at %d: %q %q %v", test.input, index, content, reason, err)
			}
		}
	}
	parser := &thinkingSplitter{}
	for _, rune := range "<think>思考🌏</think>{\"ok\":true}" {
		parser.feed(string(rune))
	}
	if _, _, err := parser.finish(false); err != nil {
		t.Fatal(err)
	}
}

func TestResponseTransformPreservesNativeMetadataAndIsOptIn(t *testing.T) {
	original := []byte(`{"id":"response","custom":{"keep":9007199254740993},"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"<think>added</think>{\"ok\":true}","reasoning_content":"native:","reasoning_details":[{"type":"reasoning.encrypted","signature":"opaque"}]}}],"usage":{"prompt_tokens":4,"completion_tokens":7}}`)
	upstream := &executortest.FakeExecutor{DoFunc: func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError) {
		return original, nil
	}}
	request := model.ExecuteRequest{Kind: model.RequestChat}
	unchanged, err := Wrap(upstream, nil).Do(context.Background(), model.Target{}, request)
	if err != nil || string(unchanged) != string(original) {
		t.Fatal("transform implicitly enabled")
	}
	normalized, err := Wrap(upstream, []Kind{SplitThinking}).Do(context.Background(), model.Target{}, request)
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Choices []struct{ Message map[string]json.RawMessage }
		Custom  json.RawMessage
		Usage   json.RawMessage
	}
	if json.Unmarshal(normalized, &root) != nil {
		t.Fatal("invalid response")
	}
	var content, reason string
	_ = json.Unmarshal(root.Choices[0].Message["content"], &content)
	_ = json.Unmarshal(root.Choices[0].Message["reasoning_content"], &reason)
	if content != `{"ok":true}` || reason != "native:added" || !strings.Contains(string(root.Choices[0].Message["reasoning_details"]), "opaque") || !strings.Contains(string(root.Custom), "9007199254740993") {
		t.Fatal(string(normalized))
	}
	if string(original) != string(unchanged) {
		t.Fatal("mutated upstream bytes")
	}
	request.Kind = model.RequestResponses
	unchanged, err = Wrap(upstream, []Kind{SplitThinking}).Do(context.Background(), model.Target{}, request)
	if err != nil || string(unchanged) != string(original) {
		t.Fatal("changed Responses payload")
	}
}

func TestStreamMatchesResponseAtAllDelimiterBoundaries(t *testing.T) {
	input := `<think>analysis</think>{"raw":"<think>literal</think>"}`
	for index := 0; index <= len(input); index++ {
		decoder := newStreamDecoder()
		content, reason := "", ""
		for i, event := range []model.StreamEvent{chunk(input[:index], ""), chunk(input[index:], ""), chunk("", "stop")} {
			normalized, err := decoder.apply(event)
			if err != nil {
				t.Fatal(err)
			}
			delta, err2 := model.DecodeStreamDelta(normalized.Data)
			if err2 != nil {
				t.Fatal(err2)
			}
			if len(delta.Choices) > 0 {
				content += delta.Choices[0].Delta.Content
				reason += delta.Choices[0].Delta.ReasoningContent
			}
			if index > 0 && index < len(openThinking) && i == 0 && normalized.Meaningful {
				t.Fatal("partial opening tag won a race")
			}
		}
		if err := decoder.end(); err != nil || content != `{"raw":"<think>literal</think>"}` || reason != "analysis" {
			t.Fatalf("boundary=%d content=%q reason=%q err=%v", index, content, reason, err)
		}
	}
}

func TestUnclosedReasoningAndLengthFinish(t *testing.T) {
	for _, finish := range []string{"stop", "length"} {
		body, err := splitResponse(response(`<think>unfinished</thi`, finish))
		if finish == "stop" {
			if err == nil || err.Code != "incomplete_thinking" {
				t.Fatal(err)
			}
			continue
		}
		if err != nil || strings.Contains(string(body), `"content":"<think>`) || !strings.Contains(string(body), `"finish_reason":"length"`) {
			t.Fatalf("length result: %s %v", body, err)
		}
		decoder := newStreamDecoder()
		_, err = decoder.apply(chunk(`<think>unfinished</thi`, ""))
		if err != nil {
			t.Fatal(err)
		}
		event, err := decoder.apply(chunk("", finish))
		delta, decodeErr := model.DecodeStreamDelta(event.Data)
		if err != nil || decodeErr != nil || delta.Choices[0].Delta.ReasoningContent != "</thi" {
			t.Fatalf("truncated end: %s %v", event.Data, err)
		}
	}
	decoder := newStreamDecoder()
	_, _ = decoder.apply(chunk("<think>unfinished", ""))
	if err := decoder.end(); err == nil || err.Code != "incomplete_thinking" {
		t.Fatal("unexpected successful EOF")
	}
}

func TestSeparateReasoningSignaturesUsageAndChoicesRemainIntact(t *testing.T) {
	decoder := newStreamDecoder()
	events := []model.StreamEvent{
		{Data: []byte(`{"choices":[{"index":0,"delta":{"reasoning":"native","reasoning_content":"native","reasoning_details":[{"type":"reasoning.encrypted","signature":"opaque"}]}},{"index":1,"delta":{"content":"{\"other\":true}"}}]}`), Meaningful: true},
		{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"{\"ok\":true}"},"finish_reason":"stop"},{"index":1,"delta":{},"finish_reason":"stop"}]}`), Meaningful: true},
		{Data: []byte(`{"choices":[],"usage":{"completion_tokens":10,"completion_tokens_details":{"reasoning_tokens":7}}}`)},
	}
	for _, event := range events {
		got, err := decoder.apply(event)
		if err != nil || !reflect.DeepEqual(got, event) {
			t.Fatalf("changed native event: %s %v", got.Data, err)
		}
	}
}

func TestStreamCancellationAndErrorsReleaseUpstream(t *testing.T) {
	stopped := make(chan struct{})
	upstream := &executortest.FakeExecutor{StreamFunc: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		events := make(chan model.StreamEvent)
		go func() {
			defer close(stopped)
			defer close(events)
			select {
			case events <- chunk("<think>reason", ""):
			case <-ctx.Done():
				return
			}
			<-ctx.Done()
		}()
		return events, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	events, err := Wrap(upstream, []Kind{SplitThinking}).Stream(ctx, model.Target{}, model.ExecuteRequest{Kind: model.RequestChat})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	for range events {
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("upstream remained active")
	}
}
