package model

import "testing"

func TestStreamDeltaSupportsChatAndResponsesUsage(t *testing.T) {
	for _, body := range []string{
		`{"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":8,"cached_tokens":2}}`,
		`{"type":"response.completed","response":{"usage":{"input_tokens":5,"output_tokens":8,"cached_tokens":2}}}`,
	} {
		delta, err := DecodeStreamDelta([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		usage, ok := delta.UsageSummary()
		if !ok || usage.Input != 5 || usage.Output != 8 || usage.CachedTokens != 2 {
			t.Fatalf("lost usage: %+v %v", usage, ok)
		}
	}
	for _, body := range []string{`{}`, `{"usage":null}`} {
		delta, err := DecodeStreamDelta([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := delta.UsageSummary(); ok {
			t.Fatal("missing usage reported as zero")
		}
	}
}

func TestStreamDeltaKeepsTextToolsAndFinishSemantics(t *testing.T) {
	delta, err := DecodeStreamDelta([]byte(`{"id":"chunk","choices":[{"delta":{"content":"answer","reasoning":"thought","reasoning_content":"duplicate","tool_calls":[{"index":2,"id":"call","function":{"name":"tool","arguments":"{}"}}]}},{"delta":{},"finish_reason":"tool_calls"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if delta.ID != "chunk" || delta.Text() != "answerthought" || !delta.Finished() {
		t.Fatalf("inconsistent chunk semantics: %+v", delta)
	}
	call := delta.Choices[0].Delta.ToolCalls[0]
	if call.Index != 2 || call.ID != "call" || call.Function.Arguments != "{}" {
		t.Fatalf("lost tool call: %+v", call)
	}
}
