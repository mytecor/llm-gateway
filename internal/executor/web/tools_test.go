package web

import "testing"

func TestTextToolResponseValidation(t *testing.T) {
	text, calls, err := DecodeToolResponse(`{"content":"hello","tool_calls":[{"name":"lookup","arguments":{"id":9007199254740993}}]}`)
	if err != nil || text != "hello" || len(calls) != 1 || string(calls[0].Arguments) != `{"id":9007199254740993}` {
		t.Fatal(text, calls, err)
	}
	for _, bad := range []string{"null", `[]`, `{"content":"a"} {}`, "```json\n{}\n```", `{"tool_calls":[{"name":"foo","arguments":[]}]}`, `{"tool_calls":[{"arguments":{}}]}`, `{"unknown":1}`} {
		if _, _, err := DecodeToolResponse(bad); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}
