package session

import "encoding/json"

type chatMessage struct {
	ToolCalls  []toolCall      `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
}

type transcriptMessage struct {
	ToolCalls  []toolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	Role       string     `json:"role"`
	Content    string     `json:"content"`
}

// FunctionTool and ToolCall are the client function-calling wire contracts.
type FunctionTool = functionTool
type ToolCall = toolCall
type Message = chatMessage

// Exported definitions let native adapters construct tool contracts directly.
type FunctionDefinition = functionDefinition
type CalledFunction = calledFunction
