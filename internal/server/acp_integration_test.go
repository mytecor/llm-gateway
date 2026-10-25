package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/executor"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/scheduler"
	"github.com/mytecor/llm-gateway/internal/session"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

type chatMessage = session.Message
type toolCall = session.ToolCall

func init() {
	if len(os.Args) > 1 && os.Args[1] == "--test-web-worker" {
		runWebWorker()
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == "--test-acp-worker" {
		runToolACP()
		os.Exit(0)
	}
}

type testMCPResponse struct {
	Result json.RawMessage
	Err    error
}

type testMCPClient struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	writeMu sync.Mutex
	enc     *json.Encoder
	nextID  atomic.Int64
	mu      sync.Mutex
	pending map[int64]chan testMCPResponse
	done    chan struct{}
}

func connectTestMCP(command string, args ...string) (*testMCPClient, error) {
	cmd := exec.Command(command, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	c := &testMCPClient{
		cmd:     cmd,
		stdin:   stdin,
		enc:     json.NewEncoder(stdin),
		pending: make(map[int64]chan testMCPResponse),
		done:    make(chan struct{}),
	}
	go func() {
		defer close(c.done)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 4<<20)
		for scanner.Scan() {
			var msg struct {
				ID     *int64          `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if json.Unmarshal(scanner.Bytes(), &msg) != nil || msg.ID == nil {
				continue
			}
			c.mu.Lock()
			ch, ok := c.pending[*msg.ID]
			c.mu.Unlock()
			if ok {
				var resp testMCPResponse
				if msg.Error != nil {
					resp.Err = errors.New(msg.Error.Message)
				} else {
					resp.Result = msg.Result
				}
				ch <- resp
			}
		}
	}()
	if _, err := c.call("initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]string{"name": "fake-acp", "version": "1"},
	}); err != nil {
		_ = c.Close()
		return nil, err
	}
	c.writeMu.Lock()
	_ = c.enc.Encode(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
	c.writeMu.Unlock()
	return c, nil
}

func (c *testMCPClient) call(method string, params any) (json.RawMessage, error) {
	id := c.nextID.Add(1)
	ch := make(chan testMCPResponse, 1)
	c.mu.Lock()
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()
	c.writeMu.Lock()
	err := c.enc.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	c.writeMu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case resp := <-ch:
		return resp.Result, resp.Err
	case <-c.done:
		return nil, errors.New("MCP bridge closed")
	}
}

func (c *testMCPClient) ListTools() ([]json.RawMessage, error) {
	raw, err := c.call("tools/list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var out struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Tools, nil
}

func (c *testMCPClient) CallTool(name string, args map[string]any) (string, bool, error) {
	raw, err := c.call("tools/call", map[string]any{"name": name, "arguments": args})
	if err != nil {
		return "", false, err
	}
	var out struct {
		IsError bool `json:"isError"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", false, err
	}
	var text strings.Builder
	for _, item := range out.Content {
		if item.Type == "text" {
			text.WriteString(item.Text)
		}
	}
	return text.String(), out.IsError, nil
}

func (c *testMCPClient) Close() error {
	_ = c.stdin.Close()
	if c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
	return c.cmd.Wait()
}

// This subprocess speaks ACP over real stdio and launches the descriptor it
// receives as an MCP subprocess. It knows no gateway internals or IPC details.
func runToolACP() {
	logEvent := func(v any) {
		f, err := os.OpenFile(os.Getenv("TEST_TOOL_ACP"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err == nil {
			_ = json.NewEncoder(f).Encode(v)
			f.Close()
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 4<<20)
	enc := json.NewEncoder(os.Stdout)
	var mcpSession *testMCPClient
	defer func() {
		if mcpSession != nil {
			_ = mcpSession.Close()
		}
	}()
	for scanner.Scan() {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal(scanner.Bytes(), &req) != nil {
			continue
		}
		reply := func(result any) { _ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}) }
		fail := func(err error) {
			logEvent(map[string]any{"error": err.Error()})
			_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32603, "message": err.Error()}})
		}
		text := func(s string) {
			_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": "tool-session", "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"type": "text", "text": s}}}})
		}
		switch req.Method {
		case "initialize":
			reply(map[string]any{"protocolVersion": 1, "agentCapabilities": map[string]any{"loadSession": true}, "agentInfo": map[string]string{"name": "fake", "version": "1"}, "authMethods": []any{}})
		case "session/new", "session/load":
			var p struct {
				SessionID  string `json:"sessionId"`
				MCPServers []struct {
					Name, Command string
					Args          []string
				} `json:"mcpServers"`
			}
			_ = json.Unmarshal(req.Params, &p)
			if req.Method == "session/load" {
				if os.Getenv("TEST_TOOL_ACP_EXPIRE_LOAD") == "1" {
					fail(fmt.Errorf("session not found: %s", p.SessionID))
					continue
				}
				logEvent(map[string]any{"load": true, "sessionId": p.SessionID, "servers": len(p.MCPServers)})
			} else {
				logEvent(map[string]any{"start": true, "servers": len(p.MCPServers)})
			}
			if len(p.MCPServers) > 0 {
				d := p.MCPServers[0]
				var err error
				mcpSession, err = connectTestMCP(d.Command, d.Args...)
				if err != nil {
					fail(err)
					continue
				}
				tools, err := mcpSession.ListTools()
				if err != nil {
					fail(err)
					continue
				}
				logEvent(map[string]any{"tools": tools})
			}
			if req.Method == "session/load" {
				reply(map[string]any{})
			} else {
				reply(map[string]any{"sessionId": "tool-session"})
			}
		case "session/prompt":
			var p struct {
				Prompt []struct {
					Text string `json:"text"`
				} `json:"prompt"`
			}
			_ = json.Unmarshal(req.Params, &p)
			prompt := ""
			for _, v := range p.Prompt {
				prompt += v.Text
			}
			logEvent(map[string]any{"prompt": true, "text": prompt})
			if strings.Contains(prompt, "stall") {
				select {}
			}
			if strings.Contains(prompt, "parallel") {
				errs := make(chan error, 2)
				for _, name := range []string{"foo", "bar"} {
					go func(name string) {
						_, isErr, err := mcpSession.CallTool(name, map[string]any{"x": 1})
						if err == nil && isErr {
							err = fmt.Errorf("tool error")
						}
						errs <- err
					}(name)
				}
				one, two := <-errs, <-errs
				if one != nil {
					fail(one)
				} else if two != nil {
					fail(two)
				} else {
					text("final")
					reply(map[string]string{"stopReason": "end_turn"})
				}
				continue
			}
			if mcpSession == nil || strings.Contains(prompt, "no-call") {
				text("final")
				reply(map[string]string{"stopReason": "end_turn"})
				continue
			}
			names := []string{"foo"}
			if strings.Contains(prompt, "sequential") {
				names = append(names, "bar")
			}
			failed := false
			for _, name := range names {
				text("before " + name)
				content, isErr, err := mcpSession.CallTool(name, map[string]any{"x": 1})
				if err != nil {
					fail(err)
					failed = true
					break
				}
				if isErr {
					fail(fmt.Errorf("MCP error: %s", content))
					failed = true
					break
				}
				logEvent(map[string]any{"result": content, "name": name})
			}
			if !failed {
				text("final")
				reply(map[string]string{"stopReason": "end_turn"})
			}
		}
	}
}

const testTools = `[{"type":"function","function":{"name":"foo","description":"Foo tool","parameters":{"type":"object","properties":{"x":{"type":"integer"}},"required":["x"]}}},{"type":"function","function":{"name":"bar","description":"Bar tool","parameters":{"type":"object","properties":{"x":{"type":"integer"}}}}}]`

func toolServer(t *testing.T, timeout time.Duration) (*Server, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.jsonl")
	t.Setenv("TEST_TOOL_ACP", path)
	s, _ := newACPServer(t, timeout, "", t.TempDir())
	return s, path
}
func newACPServer(t *testing.T, timeout time.Duration, store, cwd string) (*Server, func()) {
	return newNativeServer(t, "acp", "--test-acp-worker", timeout, store, cwd)
}
func newNativeServer(t *testing.T, kind, mode string, timeout time.Duration, store, cwd string) (*Server, func()) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{
		"session_file": store,
		"providers":    []any{map[string]any{"id": "native", "driver": kind, "command": []string{exe, mode}, "session": map[string]any{"default_cwd": cwd, "turn_timeout": timeout.String()}}},
		"rules": []any{
			map[string]any{"route": "agent", "action": "filter", "where": map[string]any{"model": map[string]string{"eq": "fake"}}},
			map[string]any{"route": "agent", "action": "map", "providers": []string{"native"}, "native": "default"},
			map[string]any{"route": "agent", "action": "race", "count": 1},
		},
	})
	var cfg policy.Config
	if err = json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	compiled, err := policy.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	executors, err := executor.New(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	catalog := subsystem.NewCatalog(compiled)
	runner := scheduler.NewRunner(compiled, catalog, executors)
	close := func() { _ = executors.Close(); _ = runner.Close() }
	t.Cleanup(close)
	return NewServer(compiled, catalog, runner), close
}
func toolRequest(t *testing.T, s *Server, messages []chatMessage, tools string, stream bool) *httptest.ResponseRecorder {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"model": "fake", "messages": messages, "tools": json.RawMessage(tools), "stream": stream})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw))).WithContext(ctx)
	req.Header.Set("X-Session-ID", "tools-session")
	// affinity accepts X-Session-Affinity; response uses X-Session-ID.
	req.Header.Set("X-Session-Affinity", "tools-session")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}
func userMessage(s string) chatMessage {
	raw, _ := json.Marshal(s)
	return chatMessage{Role: "user", Content: raw}
}
func responseMessage(t *testing.T, r *httptest.ResponseRecorder, stream bool) (chatMessage, string) {
	t.Helper()
	if r.Code != 200 {
		t.Fatalf("status %d: %s", r.Code, r.Body.String())
	}
	if !stream {
		var v struct {
			Choices []struct {
				Message chatMessage `json:"message"`
				Reason  string      `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(r.Body.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		return v.Choices[0].Message, v.Choices[0].Reason
	}
	message := chatMessage{Role: "assistant"}
	text := ""
	reason := ""
	for _, line := range strings.Split(r.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		var chunk struct {
			Error   any `json:"error"`
			Choices []struct {
				Delta struct {
					Content string     `json:"content"`
					Calls   []toolCall `json:"tool_calls"`
				} `json:"delta"`
				Reason string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Error != nil {
			t.Fatalf("stream error: %s", line)
		}
		for _, choice := range chunk.Choices {
			text += choice.Delta.Content
			message.ToolCalls = append(message.ToolCalls, choice.Delta.Calls...)
			if choice.Reason != "" {
				reason = choice.Reason
			}
		}
	}
	if !strings.Contains(r.Body.String(), "data: [DONE]") {
		t.Fatal("missing DONE")
	}
	message.Content, _ = json.Marshal(text)
	return message, reason
}
func addResult(messages []chatMessage, assistant chatMessage) []chatMessage {
	return append(messages, assistant, chatMessage{Role: "tool", ToolCallID: assistant.ToolCalls[0].ID, Content: json.RawMessage(`"ok"`)})
}
func TestToolsACPIntegration(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, sequential := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%v/sequential=%v", stream, sequential), func(t *testing.T) {
				s, path := toolServer(t, 10*time.Second)
				prompt := "single"
				if sequential {
					prompt = "sequential"
				}
				messages := []chatMessage{userMessage(prompt)}
				names := []string{"foo"}
				if sequential {
					names = append(names, "bar")
				}
				for _, name := range names {
					assistant, reason := responseMessage(t, toolRequest(t, s, messages, testTools, stream), stream)
					if reason != "tool_calls" || len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].Function.Name != name || assistant.ToolCalls[0].Function.Arguments != `{"x":1}` {
						t.Fatalf("response: %+v, reason %s", assistant, reason)
					}
					messages = addResult(messages, assistant)
				}
				assistant, reason := responseMessage(t, toolRequest(t, s, messages, testTools, stream), stream)
				if reason != "stop" || !strings.HasSuffix(string(assistant.Content), `final"`) {
					t.Fatalf("final: %+v %s", assistant, reason)
				}
				data, _ := os.ReadFile(path)
				log := string(data)
				if strings.Count(log, `"prompt":true`) != 1 || strings.Count(log, `"start":true`) != 1 || strings.Count(log, `"result":"ok"`) != len(names) {
					t.Fatalf("not one ACP turn: %s", log)
				}
				if !strings.Contains(log, `"description":"Foo tool"`) || !strings.Contains(log, `"name":"bar"`) || !strings.Contains(log, `"required":["x"]`) {
					t.Fatalf("discovery lost schemas: %s", log)
				}
			})
		}
	}
}
func TestToolsInvalidResultsAndIdentity(t *testing.T) {
	s, path := toolServer(t, 10*time.Second)
	messages := []chatMessage{userMessage("single")}
	assistant, _ := responseMessage(t, toolRequest(t, s, messages, testTools, false), false)
	valid := addResult(messages, assistant)
	bad := append([]chatMessage(nil), valid...)
	bad[len(bad)-1].ToolCallID = "unknown"
	if r := toolRequest(t, s, bad, testTools, false); r.Code != 400 {
		t.Fatalf("unknown: %d %s", r.Code, r.Body.String())
	}
	dup := append(append([]chatMessage(nil), valid...), valid[len(valid)-1])
	if r := toolRequest(t, s, dup, testTools, false); r.Code != 400 {
		t.Fatalf("duplicate: %d %s", r.Code, r.Body.String())
	}
	// Object key order does not reset the session.
	reordered := strings.Replace(testTools, `"type":"object","properties":{"x":{"type":"integer"}},"required":["x"]`, `"required":["x"],"properties":{"x":{"type":"integer"}},"type":"object"`, 1)
	_, reason := responseMessage(t, toolRequest(t, s, valid, reordered, false), false)
	if reason != "stop" {
		t.Fatal(reason)
	}
	if r := toolRequest(t, s, valid, testTools, false); r.Code != 400 {
		t.Fatalf("replayed result: %d", r.Code)
	}
	data, _ := os.ReadFile(path)
	if strings.Count(string(data), `"start":true`) != 1 {
		t.Fatal(string(data))
	}
}
func TestToolsReuseAfterFinal(t *testing.T) {
	s, path := toolServer(t, 10*time.Second)
	messages := []chatMessage{userMessage("single")}
	assistant, _ := responseMessage(t, toolRequest(t, s, messages, testTools, false), false)
	messages = addResult(messages, assistant)
	final, _ := responseMessage(t, toolRequest(t, s, messages, testTools, false), false)
	messages = append(messages, final, userMessage("no-call"))
	responseMessage(t, toolRequest(t, s, messages, testTools, false), false)
	data, _ := os.ReadFile(path)
	if strings.Count(string(data), `"prompt":true`) != 2 || strings.Count(string(data), `"start":true`) != 1 {
		t.Fatal(string(data))
	}
}

func TestACPToolsResumeAcrossGatewayRestart(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%v", missing), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.jsonl")
			t.Setenv("TEST_TOOL_ACP", path)
			cwd, store := t.TempDir(), filepath.Join(t.TempDir(), "sessions.json")
			s, close := newACPServer(t, 10*time.Second, store, cwd)
			messages := []chatMessage{userMessage("single")}
			assistant, _ := responseMessage(t, toolRequest(t, s, messages, testTools, false), false)
			messages = addResult(messages, assistant)
			final, _ := responseMessage(t, toolRequest(t, s, messages, testTools, false), false)
			close()
			if missing {
				t.Setenv("TEST_TOOL_ACP_EXPIRE_LOAD", "1")
			}
			s, _ = newACPServer(t, 10*time.Second, store, cwd)
			messages = append(messages, final, userMessage("single again"))
			assistant, reason := responseMessage(t, toolRequest(t, s, messages, testTools, true), true)
			if reason != "tool_calls" || len(assistant.ToolCalls) != 1 {
				t.Fatal(assistant, reason)
			}
			messages = addResult(messages, assistant)
			final, reason = responseMessage(t, toolRequest(t, s, messages, testTools, true), true)
			if reason != "stop" || !strings.HasSuffix(string(final.Content), `final"`) {
				t.Fatal(final, reason)
			}
			data, _ := os.ReadFile(path)
			log := string(data)
			if strings.Count(log, `"result":"ok"`) != 2 || strings.Count(log, `"prompt":true`) != 2 {
				t.Fatal("did not continue expected turns", log)
			}
			if !missing && (strings.Count(log, `"load":true`) != 1 || strings.Count(log, `"start":true`) != 1) {
				t.Fatal("did not restore ACP session", log)
			}
			if missing && strings.Count(log, `"start":true`) != 2 {
				t.Fatal("did not recreate missing session", log)
			}
		})
	}
}

func TestGeneratedSessionIDCanResumeViaJSON(t *testing.T) {
	s, _ := toolServer(t, 10*time.Second)
	messages := []chatMessage{userMessage("single")}
	raw, _ := json.Marshal(map[string]any{"model": "fake", "messages": messages, "tools": json.RawMessage(testTools)})
	first := httptest.NewRecorder()
	s.ServeHTTP(first, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw))))
	assistant, reason := responseMessage(t, first, false)
	id := first.Header().Get("X-Session-ID")
	if id == "" || reason != "tool_calls" {
		t.Fatal("missing session identity", id, reason)
	}
	raw, _ = json.Marshal(map[string]any{"model": "fake", "session_id": id, "messages": addResult(messages, assistant), "tools": json.RawMessage(testTools)})
	next := httptest.NewRecorder()
	s.ServeHTTP(next, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw))))
	_, reason = responseMessage(t, next, false)
	if reason != "stop" || next.Header().Get("X-Session-ID") != id {
		t.Fatal("did not preserve generated identity")
	}
}

func TestNativeErrorsExposeStableCodes(t *testing.T) {
	s, _ := toolServer(t, 10*time.Second)
	messages := []chatMessage{userMessage("single")}
	assistant, _ := responseMessage(t, toolRequest(t, s, messages, testTools, false), false)
	repeated := toolRequest(t, s, messages, testTools, false)
	if repeated.Code != 409 || !strings.Contains(repeated.Body.String(), `"code":"turn_pending"`) {
		t.Fatal(repeated.Code, repeated.Body.String())
	}
	invalid := addResult(messages, assistant)
	invalid[len(invalid)-1].ToolCallID = "unknown"
	bad := toolRequest(t, s, invalid, testTools, false)
	if bad.Code != 400 || !strings.Contains(bad.Body.String(), `"code":"invalid_tool_result"`) {
		t.Fatal(bad.Code, bad.Body.String())
	}
}
