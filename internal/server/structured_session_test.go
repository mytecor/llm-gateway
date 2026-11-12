package server

import (
	"bufio"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func init() {
	if len(os.Args) > 1 && os.Args[1] == "--test-strict-driver" {
		runStrictDriver()
		os.Exit(0)
	}
}

// Both ACP and Web use this same native-session NDJSON contract. The worker
// rejects feedback in a different session and rejects replayed public history.
func runStrictDriver() {
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	pending, resumed := false, false
	for scanner.Scan() {
		var input struct {
			Type, Text, System string
			ResumeID           string `json:"resume_id"`
		}
		if json.Unmarshal(scanner.Bytes(), &input) != nil {
			return
		}
		switch input.Type {
		case "init":
			_ = encoder.Encode(map[string]any{"type": "ready", "protocol_version": 1, "capabilities": map[string]bool{"chat": true, "streaming": true, "stateful": true, "resume": true}, "tool_strategy": "wire"})
		case "models":
			_ = encoder.Encode(map[string]any{"type": "models", "models": []any{map[string]any{"id": "default"}}})
		case "start":
			if strings.Contains(input.System, "JSON Schema") {
				return
			}
			resumed = input.ResumeID == "strict-native"
			_ = encoder.Encode(map[string]any{"type": "ready", "resume_id": "strict-native"})
		case "prompt":
			output := ""
			switch {
			case strings.HasPrefix(input.Text, "original") && strings.Contains(input.Text, `"minimum":0`):
				pending = true
				output = `{"value":-1}`
			case pending && strings.HasPrefix(input.Text, "Your previous response failed JSON Schema validation:"):
				pending = false
				output = `{"value":1}`
			case !pending && input.Text == "next":
				output = `{"value":2}`
			case !pending && resumed && input.Text == "restored":
				output = `{"value":3}`
			}
			if output == "" {
				_ = encoder.Encode(map[string]any{"type": "error", "error": "native conversation contract violated"})
				continue
			}
			_ = encoder.Encode(map[string]any{"type": "text", "text": output})
			_ = encoder.Encode(map[string]any{"type": "usage", "input_tokens": 3, "output_tokens": 2})
			_ = encoder.Encode(map[string]any{"type": "done"})
		}
	}
}

func TestStrictNativeSessionThroughRegistryAndRestart(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			cwd := t.TempDir()
			path := filepath.Join(t.TempDir(), "sessions.json")
			server, closeServer := newNativeServer(t, "", "--test-strict-driver", time.Second, path, cwd)
			send := func(s *Server, messages []chatMessage, strict bool) *httptest.ResponseRecorder {
				wire := map[string]any{"model": "fake", "messages": messages, "stream": stream}
				if strict {
					var fields map[string]json.RawMessage
					_ = json.Unmarshal([]byte(`{`+strictFormat+`}`), &fields)
					wire["response_format"] = fields["response_format"]
				}
				raw, _ := json.Marshal(wire)
				req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw)))
				req.Header.Set("X-Session-Affinity", "strict-conversation")
				rec := httptest.NewRecorder()
				s.ServeHTTP(rec, req)
				return rec
			}
			messages := []chatMessage{userMessage("original")}
			rec := send(server, messages, true)
			assistant, reason := responseMessage(t, rec, stream)
			if reason != "stop" || string(assistant.Content) != `"{\"value\":1}"` {
				t.Fatalf("%+v %s", assistant, rec.Body.String())
			}
			if !stream && !strings.Contains(rec.Body.String(), `"prompt_tokens":6`) {
				t.Fatal("lost native correction usage")
			}
			messages = append(messages, assistant, userMessage("next"))
			assistant, _ = responseMessage(t, send(server, messages, false), stream)
			if string(assistant.Content) != `"{\"value\":2}"` {
				t.Fatal("next turn recreated native conversation")
			}
			closeServer()
			server, _ = newNativeServer(t, "", "--test-strict-driver", time.Second, path, cwd)
			messages = append(messages, assistant, userMessage("restored"))
			assistant, _ = responseMessage(t, send(server, messages, false), stream)
			if string(assistant.Content) != `"{\"value\":3}"` {
				t.Fatal("native resume failed")
			}
		})
	}
}
