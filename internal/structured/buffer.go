package structured

import (
	"encoding/json"
	"strings"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/scheduler"
)

// Buffer is a transport-neutral sink. It holds every selected generation event
// until validation, while keeping the client's waiting connection alive safely.
type Buffer struct {
	Events      []model.StreamEvent
	Err         *model.CallError
	Completed   bool
	KeepWaiting func() bool
	bytes       int
	content     strings.Builder
	refusal     strings.Builder
	finish      string
}

func (*Buffer) Begin() {}
func (b *Buffer) Write(event model.StreamEvent) bool {
	if b.Err != nil {
		return false
	}
	b.bytes += len(event.Data) + 128
	var chunk struct {
		Choices []struct {
			Index int `json:"index"`
			Delta struct {
				Content  string            `json:"content"`
				Refusal  string            `json:"refusal"`
				Tools    []json.RawMessage `json:"tool_calls"`
				Function json.RawMessage   `json:"function_call"`
			} `json:"delta"`
			Finish string `json:"finish_reason"`
		} `json:"choices"`
	}
	if json.Unmarshal(event.Data, &chunk) != nil || len(chunk.Choices) > 1 {
		b.Err = Error(502, "invalid_upstream_response")
		return false
	}
	for _, choice := range chunk.Choices {
		if choice.Index != 0 {
			b.Err = Error(502, "invalid_upstream_response")
			return false
		}
		if len(choice.Delta.Tools) > 0 || (len(choice.Delta.Function) > 0 && string(choice.Delta.Function) != "null") || choice.Finish == "tool_calls" || choice.Finish == "function_call" {
			b.Err = Error(502, "structured_output_unexpected_tool_calls")
			return false
		}
		b.bytes += len(choice.Delta.Content) + len(choice.Delta.Refusal)
		if b.bytes > MaxBufferBytes {
			b.Err = Error(502, "structured_output_buffer_exceeded")
			return false
		}
		b.content.WriteString(choice.Delta.Content)
		b.refusal.WriteString(choice.Delta.Refusal)
		if choice.Finish != "" {
			b.finish = choice.Finish
		}
	}
	if b.bytes > MaxBufferBytes {
		b.Err = Error(502, "structured_output_buffer_exceeded")
		return false
	}
	event.Data = append([]byte(nil), event.Data...)
	b.Events = append(b.Events, event)
	return true
}
func (b *Buffer) KeepAlive(scheduler.StreamHeartbeat) bool {
	return b.KeepWaiting == nil || b.KeepWaiting()
}
func (b *Buffer) Complete()                         { b.Completed = true }
func (b *Buffer) Fail(err *model.CallError, _ bool) { b.Err = err }
func (b *Buffer) Body() []byte {
	msg := map[string]any{"role": "assistant", "content": b.content.String()}
	if b.refusal.Len() > 0 {
		msg["refusal"] = b.refusal.String()
	}
	data, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "message": msg, "finish_reason": b.finish}}})
	return data
}

// PublicEvents replaces per-generation usage with one aggregate usage event,
// following the Chat Completions stream_options.include_usage contract.
func PublicEvents(events []model.StreamEvent, usage model.UsageSummary, hasUsage, includeUsage bool) []model.StreamEvent {
	out := make([]model.StreamEvent, 0, len(events)+1)
	var last map[string]json.RawMessage
	for _, event := range events {
		var root map[string]json.RawMessage
		if json.Unmarshal(event.Data, &root) != nil {
			continue
		}
		last = root
		delete(root, "usage")
		var choices []json.RawMessage
		_ = json.Unmarshal(root["choices"], &choices)
		if len(choices) == 0 {
			continue
		}
		event.Data, _ = json.Marshal(root)
		out = append(out, event)
	}
	if includeUsage && hasUsage && last != nil {
		root := map[string]json.RawMessage{}
		for _, key := range []string{"id", "object", "created", "model"} {
			if value, ok := last[key]; ok {
				root[key] = value
			}
		}
		root["choices"] = json.RawMessage(`[]`)
		data, _ := json.Marshal(root)
		out = append(out, model.StreamEvent{Data: WithUsage(data, usage, true)})
	}
	return out
}
