package session

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeEngine struct {
	mu         sync.Mutex
	specs      []Spec
	prompts    []string
	failResume bool
	closed     int
	run        func(context.Context, Spec, string, func(Event) error) error
}

func (e *fakeEngine) Start(_ context.Context, spec Spec) (Session, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.specs = append(e.specs, spec)
	if e.failResume && spec.ResumeID != "" {
		return nil, ErrSessionNotFound
	}
	return &fakeSession{engine: e, spec: spec}, nil
}

type fakeSession struct {
	engine *fakeEngine
	spec   Spec
	once   sync.Once
}

func (s *fakeSession) Close() error {
	s.once.Do(func() { s.engine.mu.Lock(); s.engine.closed++; s.engine.mu.Unlock() })
	return nil
}
func (s *fakeSession) Run(ctx context.Context, prompt string, emit func(Event) error) error {
	s.engine.mu.Lock()
	s.engine.prompts = append(s.engine.prompts, prompt)
	s.engine.mu.Unlock()
	if err := emit(Event{Kind: EventInit, ResumeID: "native-session"}); err != nil {
		return err
	}
	if s.engine.run != nil {
		return s.engine.run(ctx, s.spec, prompt, emit)
	}
	if strings.Contains(prompt, "stall") {
		<-ctx.Done()
		return ctx.Err()
	}
	if strings.Contains(prompt, "call") {
		result, err := s.spec.CallTool(ctx, "lookup", json.RawMessage(`{"id":9007199254740993}`))
		if err != nil {
			return err
		}
		return emit(Event{Kind: EventText, Text: "result: " + result})
	}
	return emit(Event{Kind: EventText, Text: "answer"})
}

const toolsJSON = `[{"type":"function","function":{"name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}}]`

func request(prompt string) Request {
	var tools []FunctionTool
	_ = json.Unmarshal([]byte(toolsJSON), &tools)
	return Request{Principal: "user", Conversation: "conversation", Model: "model", Tools: tools, Messages: []Message{message("user", prompt)}}
}
func message(role, text string) Message {
	raw, _ := json.Marshal(text)
	return Message{Role: role, Content: raw}
}
func appendResult(req Request, result *Result) Request {
	assistant := message("assistant", result.Text)
	assistant.ToolCalls = result.ToolCalls
	req.Messages = append(append([]Message(nil), req.Messages...), assistant)
	for _, call := range result.ToolCalls {
		m := message("tool", "ok")
		m.ToolCallID = call.ID
		req.Messages = append(req.Messages, m)
	}
	return req
}
func newRuntime(t *testing.T, engine *fakeEngine, cfg Config) *Runtime {
	t.Helper()
	if cfg.DefaultCWD == "" {
		cfg.DefaultCWD = t.TempDir()
	}
	r, err := New(engine, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}
func TestToolTurnSurvivesExchangeContextAndRejectsBadResults(t *testing.T) {
	engine := &fakeEngine{}
	r := newRuntime(t, engine, Config{})
	req := request("call")
	ctx, cancel := context.WithCancel(context.Background())
	first, err := r.Exchange(ctx, req, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if first.FinishReason != "tool_calls" || len(first.ToolCalls) != 1 || first.ToolCalls[0].Function.Arguments != `{"id":9007199254740993}` {
		t.Fatalf("%+v", first)
	}
	valid := appendResult(req, first)
	bad := valid
	bad.Messages = append([]Message(nil), valid.Messages...)
	bad.Messages[len(bad.Messages)-1].ToolCallID = "unknown"
	if _, err = r.Exchange(context.Background(), bad, nil); err == nil {
		t.Fatal("accepted unknown result")
	}
	dup := valid
	dup.Messages = append(append([]Message(nil), valid.Messages...), valid.Messages[len(valid.Messages)-1])
	if _, err = r.Exchange(context.Background(), dup, nil); err == nil {
		t.Fatal("accepted duplicate result")
	}
	if _, err = r.Exchange(context.Background(), req, nil); err == nil {
		t.Fatal("accepted prompt while waiting for tools")
	}
	final, err := r.Exchange(context.Background(), valid, nil)
	if err != nil {
		t.Fatal(err)
	}
	if final.FinishReason != "stop" || final.Text != "result: ok" || len(engine.prompts) != 1 {
		t.Fatalf("%+v prompts=%v", final, engine.prompts)
	}
	if _, err = r.Exchange(context.Background(), valid, nil); err == nil {
		t.Fatal("accepted stale result")
	}
}
func TestSessionReuseResumeBranchAndMissingNativeSession(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "missing"}[missing], func(t *testing.T) {
			cwd := t.TempDir()
			store := filepath.Join(t.TempDir(), "sessions.json")
			firstEngine := &fakeEngine{}
			r := newRuntime(t, firstEngine, Config{DefaultCWD: cwd, StorePath: store})
			req := request("hello")
			first, err := r.Exchange(context.Background(), req, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Messages = append(req.Messages, message("assistant", first.Text), message("user", "second"))
			second, err := r.Exchange(context.Background(), req, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(firstEngine.specs) != 1 || firstEngine.prompts[1] != "second" {
				t.Fatalf("%+v %v", firstEngine.specs, firstEngine.prompts)
			}
			_ = r.Close()
			data, err := os.ReadFile(store)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "hello") || strings.Contains(string(data), "second") {
				t.Fatal("persisted prompt")
			}
			engine := &fakeEngine{failResume: missing}
			resumed := newRuntime(t, engine, Config{DefaultCWD: cwd, StorePath: store})
			req.Messages = append(req.Messages, message("assistant", second.Text), message("user", "third"))
			third, err := resumed.Exchange(context.Background(), req, nil)
			if err != nil {
				t.Fatal(err)
			}
			if engine.specs[0].ResumeID != "native-session" {
				t.Fatal("did not attempt resume")
			}
			if !missing && engine.prompts[0] != "third" {
				t.Fatal(engine.prompts)
			}
			if missing && (len(engine.specs) != 2 || !strings.Contains(engine.prompts[0], "hello")) {
				t.Fatal("did not replay full history after missing session")
			}
			req.Messages = append(req.Messages, message("assistant", third.Text), message("user", "branch"))
			req.Messages[0] = message("user", "edited")
			if _, err = resumed.Exchange(context.Background(), req, nil); err != nil {
				t.Fatal(err)
			}
			if engine.specs[len(engine.specs)-1].ResumeID != "" {
				t.Fatal("branch reused native session")
			}
		})
	}
}
func TestPrincipalIsolationAndIndependentSessionLocks(t *testing.T) {
	entered := make(chan struct{}, 1)
	engine := &fakeEngine{run: func(ctx context.Context, spec Spec, prompt string, emit func(Event) error) error {
		if prompt == "stall" {
			entered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		return emit(Event{Kind: EventText, Text: "ok"})
	}}
	r := newRuntime(t, engine, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := r.Exchange(ctx, request("stall"), nil); done <- err }()
	<-entered
	other := request("hello")
	other.Principal = "another-user"
	independent, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := r.Exchange(independent, other, nil); err != nil {
		t.Fatal("one session blocked another", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.specs) != 2 {
		t.Fatal("principals shared a session")
	}
}
func TestPendingTimeoutToolsetChangeAndShutdown(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		r := newRuntime(t, &fakeEngine{}, Config{TurnTimeout: 40 * time.Millisecond})
		req := request("call")
		first, err := r.Exchange(context.Background(), req, nil)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(70 * time.Millisecond)
		if _, err = r.Exchange(context.Background(), appendResult(req, first), nil); err == nil {
			t.Fatal("expired turn accepted a result")
		}
	})
	t.Run("changed-tools", func(t *testing.T) {
		r := newRuntime(t, &fakeEngine{}, Config{})
		req := request("call")
		first, err := r.Exchange(context.Background(), req, nil)
		if err != nil {
			t.Fatal(err)
		}
		req = appendResult(req, first)
		req.Tools[0].Function.Description = "changed"
		if _, err = r.Exchange(context.Background(), req, nil); err == nil {
			t.Fatal("changed toolset accepted old result")
		}
	})
	t.Run("shutdown", func(t *testing.T) {
		r := newRuntime(t, &fakeEngine{}, Config{})
		if _, err := r.Exchange(context.Background(), request("call"), nil); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { _ = r.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("shutdown stuck on tool call")
		}
	})
}
func TestToolPolicyAndCWD(t *testing.T) {
	for _, choice := range []string{`"none"`, `{"type":"function","function":{"name":"other"}}`} {
		r := newRuntime(t, &fakeEngine{}, Config{})
		req := request("call")
		req.ToolChoice = json.RawMessage(choice)
		if _, err := r.Exchange(context.Background(), req, nil); err == nil {
			t.Fatal("accepted forbidden call")
		}
	}
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	r := newRuntime(t, &fakeEngine{}, Config{DefaultCWD: root, AllowedRoots: []string{root}})
	req := request("hello")
	req.CWD = link
	if _, err := r.Exchange(context.Background(), req, nil); err == nil {
		t.Fatal("symlink escaped allowed root")
	}
}

func TestParallelToolCallsResolveInBatchesWithoutStartingAnotherTurn(t *testing.T) {
	engine := &fakeEngine{run: func(ctx context.Context, spec Spec, _ string, emit func(Event) error) error {
		results := make(chan error, 2)
		for range 2 {
			go func() { _, err := spec.CallTool(ctx, "lookup", json.RawMessage(`{"id":1}`)); results <- err }()
		}
		for range 2 {
			if err := <-results; err != nil {
				return err
			}
		}
		return emit(Event{Kind: EventText, Text: "both completed"})
	}}
	r := newRuntime(t, engine, Config{})
	req := request("parallel")
	calls := 0
	for range 4 {
		result, err := r.Exchange(context.Background(), req, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result.FinishReason == "stop" {
			if calls != 2 || result.Text != "both completed" || len(engine.prompts) != 1 {
				t.Fatalf("calls=%d result=%+v", calls, result)
			}
			return
		}
		calls += len(result.ToolCalls)
		req = appendResult(req, result)
	}
	t.Fatal("parallel calls did not finish")
}

func TestResumeMissingDuringFirstTurnFallsBackOnlyBeforeOutput(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-output", true: "partial-output"}[partial], func(t *testing.T) {
			cwd, store := t.TempDir(), filepath.Join(t.TempDir(), "sessions.json")
			r := newRuntime(t, &fakeEngine{}, Config{DefaultCWD: cwd, StorePath: store})
			req := request("hello")
			result, err := r.Exchange(context.Background(), req, nil)
			if err != nil {
				t.Fatal(err)
			}
			_ = r.Close()
			req.Messages = append(req.Messages, message("assistant", result.Text), message("user", "next"))
			engine := &fakeEngine{run: func(_ context.Context, spec Spec, _ string, emit func(Event) error) error {
				if spec.ResumeID != "" {
					if partial {
						if err := emit(Event{Kind: EventText, Text: "partial"}); err != nil {
							return err
						}
					}
					return ErrSessionNotFound
				}
				return emit(Event{Kind: EventText, Text: "fresh"})
			}}
			resumed := newRuntime(t, engine, Config{DefaultCWD: cwd, StorePath: store})
			result, err = resumed.Exchange(context.Background(), req, nil)
			if partial {
				if err == nil || len(engine.specs) != 1 {
					t.Fatal("replayed a turn after output", result, err)
				}
				return
			}
			if err != nil || result.Text != "fresh" || len(engine.specs) != 2 {
				t.Fatal(result, err)
			}
		})
	}
}

func TestCheckpointStoreRejectsLegacyAndCorruptFiles(t *testing.T) {
	for _, data := range []string{`{"version":1,"sessions":{}}`, `{broken`} {
		path := filepath.Join(t.TempDir(), "sessions.json")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := New(&fakeEngine{}, Config{DefaultCWD: t.TempDir(), StorePath: path}); err == nil {
			t.Fatal("accepted incompatible checkpoint file")
		}
	}
}

func TestHeartbeatIsSemanticAndDoesNotBecomeModelOutput(t *testing.T) {
	stop := errors.New("heartbeat observed")
	r := newRuntime(t, &fakeEngine{}, Config{HeartbeatInterval: 5 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := r.Exchange(ctx, request("stall"), func(event Event) error {
		if event.Kind == EventHeartbeat {
			if event.Text != "" || event.ToolCall != nil || event.Usage != nil {
				t.Error("heartbeat carried model output")
			}
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatal("missing heartbeat", err)
	}
}
