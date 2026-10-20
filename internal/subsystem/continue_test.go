package subsystem

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAccumulatePartialCollectsContentAndReasoning(t *testing.T) {
	out := &PartialStreamOutput{}
	AccumulatePartial([]byte(`{"choices":[{"index":0,"delta":{"content":"Hel"}}]}`), out)
	AccumulatePartial([]byte(`{"choices":[{"index":0,"delta":{"content":"lo"}}]}`), out)
	AccumulatePartial([]byte(`{"choices":[{"index":0,"delta":{"reasoning":"think"}}]}`), out)
	AccumulatePartial([]byte(`{"choices":[{"index":0,"delta":{"reasoning":" more"}}]}`), out)
	if out.Content != "Hello" {
		t.Fatalf("content = %q, want Hello", out.Content)
	}
	if out.Reasoning != "think more" {
		t.Fatalf("reasoning = %q, want 'think more'", out.Reasoning)
	}
	if !out.GotText {
		t.Fatal("GotText must be set after a content delta")
	}
}

func TestAccumulatePartialIgnoresEmptyAndUsages(t *testing.T) {
	out := &PartialStreamOutput{}
	AccumulatePartial([]byte(`{"choices":[{"index":0,"delta":{}}]}`), out)
	AccumulatePartial([]byte(`{"choices":[]}`), out)
	AccumulatePartial([]byte(`{}`), out)
	AccumulatePartial([]byte(`{"usage":{"prompt_tokens":5}}`), out)
	if out.GotText || out.Content != "" {
		t.Fatalf("empty chunks must not count as text: %+v", out)
	}
}

func TestAccumulatePartialTracksToolCalls(t *testing.T) {
	out := &PartialStreamOutput{}
	AccumulatePartial([]byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"bash","arguments":""}}]}}]}`), out)
	if !out.GotToolCalls {
		t.Fatal("GotToolCalls must be set after a tool-call delta")
	}
	if out.GotText || out.Content != "" {
		t.Fatalf("a tool-call delta must not count as text: %+v", out)
	}

	empty := &PartialStreamOutput{}
	AccumulatePartial([]byte(`{"choices":[{"index":0,"delta":{"tool_calls":[]}}]}`), empty)
	if empty.GotToolCalls {
		t.Fatal("an empty tool_calls array must not set GotToolCalls")
	}
}

func TestAccumulatePartialReasoningOnlyIsNotText(t *testing.T) {
	out := &PartialStreamOutput{}
	AccumulatePartial([]byte(`{"choices":[{"index":0,"delta":{"reasoning_content":"let me think"}}]}`), out)
	AccumulatePartial([]byte(`{"choices":[{"index":0,"delta":{"reasoning":"and more"}}]}`), out)
	if out.Reasoning != "let me thinkand more" {
		t.Fatalf("reasoning = %q", out.Reasoning)
	}
	if out.GotText || out.GotToolCalls || out.Content != "" {
		t.Fatalf("reasoning-only stream must not count as a non-empty completion: %+v", out)
	}
}

func TestAppendPartialChatHistoryAppendsAssistantMessage(t *testing.T) {
	original := []byte(`{"model":"native","messages":[{"role":"user","content":"hi"}]}`)
	partial := &PartialStreamOutput{Content: "Hello there", Reasoning: "thinking", GotText: true}
	rewritten, err := AppendPartialChatHistory(original, partial)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(rewritten, &body); err != nil {
		t.Fatalf("rewritten body invalid: %v (%s)", err, rewritten)
	}
	if len(body.Messages) != 2 {
		t.Fatalf("messages len = %d, want 2: %s", len(body.Messages), rewritten)
	}
	last := body.Messages[1]
	if last["role"] != "assistant" || last["content"] != "Hello there" || last["reasoning_content"] != "thinking" {
		t.Fatalf("appended assistant message = %#v", last)
	}
	if body.Messages[0]["role"] != "user" || body.Messages[0]["content"] != "hi" {
		t.Fatalf("original message not preserved: %#v", body.Messages[0])
	}
	if !strings.Contains(string(rewritten), `"model":"native"`) {
		t.Fatalf("model field lost: %s", rewritten)
	}
}

func TestAppendPartialChatHistoryNoPartialIsNoop(t *testing.T) {
	original := []byte(`{"messages":[{"role":"user","content":"hi"}]}`)
	rewritten, err := AppendPartialChatHistory(original, &PartialStreamOutput{})
	if err != nil {
		t.Fatal(err)
	}
	if string(rewritten) != string(original) {
		t.Fatalf("no-partial request must be unchanged: %s", rewritten)
	}
}

func TestAppendPartialChatHistoryRefusesToReshareToolCallPartial(t *testing.T) {
	original := []byte(`{"messages":[{"role":"user","content":"hi"}]}`)
	partial := &PartialStreamOutput{Content: "<invoke name=\"write\">", GotText: true, GotToolCalls: true}
	rewritten, err := AppendPartialChatHistory(original, partial)
	if err != nil {
		t.Fatal(err)
	}
	if string(rewritten) != string(original) {
		t.Fatalf("a tool-call partial must not be reshared as assistant text: %s", rewritten)
	}

	completed := &PartialStreamOutput{GotToolCalls: true}
	rewritten, err = AppendPartialChatHistory(original, completed)
	if err != nil {
		t.Fatal(err)
	}
	if string(rewritten) != string(original) {
		t.Fatalf("a completed tool-call partial must not be reshared either: %s", rewritten)
	}

	textOnly := &PartialStreamOutput{Content: "thinking out loud", GotText: true}
	rewritten, err = AppendPartialChatHistory(original, textOnly)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rewritten), "thinking out loud") {
		t.Fatalf("a text-only partial must still be reshared: %s", rewritten)
	}
}

func TestAppendPartialChatHistoryResharesReasoningOnly(t *testing.T) {
	original := []byte(`{"model":"native","messages":[{"role":"user","content":"hi"}]}`)
	partial := &PartialStreamOutput{Reasoning: "step 1 thinking"}
	rewritten, err := AppendPartialChatHistory(original, partial)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(rewritten, &body); err != nil {
		t.Fatalf("rewritten body invalid: %v (%s)", err, rewritten)
	}
	if len(body.Messages) != 2 {
		t.Fatalf("messages len = %d, want 2: %s", len(body.Messages), rewritten)
	}
	last := body.Messages[1]
	if last["role"] != "assistant" || last["reasoning_content"] != "step 1 thinking" {
		t.Fatalf("appended reasoning-only assistant message = %#v", last)
	}
}
