package bifrost

import "testing"

func TestHeartbeatDoesNotWinSelection(t *testing.T) {
	for _, payload := range []string{
		`{"choices":[{"delta":{"reasoning_content":"\u200b"}}]}`,
		`{"choices":[{"delta":{"reasoning_content":" "}}]}`,
		`{"choices":[{"delta":{"content":""}}]}`,
	} {
		if MeaningfulPayload([]byte(payload), "") {
			t.Fatalf("heartbeat considered meaningful: %s", payload)
		}
	}
	for _, payload := range []string{
		`{"choices":[{"delta":{"reasoning_content":"thinking"}}]}`,
		`{"choices":[{"delta":{"content":"hello"}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"foo"}}]}}]}`,
	} {
		if !MeaningfulPayload([]byte(payload), "") {
			t.Fatalf("lost meaningful output: %s", payload)
		}
	}
}
