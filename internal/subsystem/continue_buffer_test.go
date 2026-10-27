package subsystem

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/mytecor/llm-gateway/internal/model"
)

func textDelta(text string) model.StreamDelta {
	body, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"delta": map[string]string{"content": text, "reasoning": text}}}})
	delta, _ := model.DecodeStreamDelta(body)
	return delta
}

func TestStreamWithoutContinuationKeepsOnlyMetadata(t *testing.T) {
	out := NewPartialStreamOutput(false)
	delta := textDelta(strings.Repeat("x", 4096))
	for range 1000 {
		out.Accumulate(delta)
	}
	if !out.GotText || out.Content != "" || out.Reasoning != "" || out.content != nil || out.reasoning != nil || out.retainedBytes != 0 {
		t.Fatalf("plain relay retained full response: %+v", out)
	}
}

func TestContinuationBufferDisablesRecoveryOnOverflow(t *testing.T) {
	out := NewPartialStreamOutput(true, 12)
	out.Accumulate(textDelta("abc"))
	firstSnapshot := out.Content
	out.Accumulate(textDelta("def"))
	if firstSnapshot != "abc" || out.Content != "abcdef" || out.Reasoning != "abcdef" {
		t.Fatal("builder corrupted a previous snapshot")
	}
	out.Accumulate(textDelta("g"))
	if !out.Overflowed || out.Content != "" || out.Reasoning != "" || out.content != nil || out.reasoning != nil {
		t.Fatal("overflow retained continuation text")
	}
	body := []byte(`{"messages":[{"role":"user","content":"hello"}]}`)
	rewritten, err := AppendPartialChatHistory(body, out)
	if err != nil || string(body) != string(rewritten) {
		t.Fatal("overflow replayed incomplete context")
	}
}
