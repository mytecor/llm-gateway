package server

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// A browser driver test double; it speaks the documented direct protocol,
// with no ACP, MCP or HTTP adapter between the driver and gateway runtime.
func runWebWorker() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 4<<20)
	enc := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var input struct{ Type, Text, Content, System string }
		if json.Unmarshal(scanner.Bytes(), &input) != nil {
			return
		}
		switch input.Type {
		case "start":
			if !strings.Contains(input.System, "Available client function tools") {
				return
			}
			_ = enc.Encode(map[string]string{"type": "ready"})
		case "prompt":
			_ = enc.Encode(map[string]string{"type": "heartbeat"})
			if input.Text == "no-call" {
				_ = enc.Encode(map[string]string{"type": "text", "text": "final"})
				_ = enc.Encode(map[string]string{"type": "done"})
				continue
			}
			_ = enc.Encode(map[string]string{"type": "message", "id": "website-1", "text": `{"content":"before foo","tool_calls":[{"name":"foo","arguments":{"x":1}}]}`})
		case "tool_result":
			_ = enc.Encode(map[string]string{"type": "text", "text": "final " + input.Content})
			_ = enc.Encode(map[string]any{"type": "usage", "input_tokens": 10, "output_tokens": 5})
			_ = enc.Encode(map[string]string{"type": "done"})
		}
	}
}
func TestWebExecutorUsesPiToolsThroughSharedRuntime(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "stream"}[stream], func(t *testing.T) {
			s, _ := newNativeServer(t, "web", "--test-web-worker", 10*time.Second, "", t.TempDir())
			req := []chatMessage{userMessage("single")}
			assistant, reason := responseMessage(t, toolRequest(t, s, req, testTools, stream), stream)
			if reason != "tool_calls" || len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].Function.Name != "foo" || string(assistant.Content) != `"before foo"` {
				t.Fatalf("%+v %s", assistant, reason)
			}
			final, reason := responseMessage(t, toolRequest(t, s, addResult(req, assistant), testTools, stream), stream)
			if reason != "stop" || string(final.Content) != `"final ok"` {
				t.Fatalf("%+v %s", final, reason)
			}
		})
	}
}
