package web

import (
	"github.com/mytecor/llm-gateway/internal/executor/external"
	"github.com/mytecor/llm-gateway/internal/session"
)

type TextToolCall = external.TextToolCall

func ToolInstructions(tools []session.FunctionTool) string {
	return external.ToolInstructions(tools)
}

func DecodeToolResponse(text string) (string, []TextToolCall, error) {
	return external.DecodeToolResponse(text)
}
