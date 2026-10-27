package external

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/session"
)

func testCustomCommand(t *testing.T) []string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return []string{exe, "-test.run=^TestDriverSessionUpdateProcess$"}
}

func TestExternalDriverConfigValidation(t *testing.T) {
	if _, err := NewDriver(Config{}); err == nil {
		t.Fatal("expected error on empty command")
	}
	if _, err := NewDriver(Config{Command: []string{"nonexistent-binary-12345"}}); err == nil {
		t.Fatal("expected error on nonexistent binary")
	}
	if _, err := NewDriver(Config{Driver: "unknown", Command: []string{"go"}}); err == nil {
		t.Fatal("expected error on unknown driver")
	}
}

// Exercise the actual subprocess wire, including IDs assigned after the first prompt.
func TestDriverSessionUpdateWire(t *testing.T) {
	t.Setenv("CUSTOM_DRIVER_TEST_PROCESS", "1")
	engine, err := NewDriver(Config{
		Command:      testCustomCommand(t),
		ToolStrategy: ToolStrategyPrompt,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	proc, err := engine.Start(ctx, session.Spec{})
	if err != nil {
		t.Fatal(err)
	}
	defer proc.Close()
	var resumed string
	err = proc.Run(ctx, "hello", func(event session.Event) error {
		if event.Kind == session.EventInit && event.ResumeID != "" {
			resumed = event.ResumeID
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if resumed != "https://example.test/thread/created" {
		t.Fatalf("lost updated resume ID: %q", resumed)
	}
}

func TestDriverSessionUpdateProcess(t *testing.T) {
	if os.Getenv("CUSTOM_DRIVER_TEST_PROCESS") != "1" {
		return
	}
	decoder := json.NewDecoder(os.Stdin)
	var input map[string]any
	if decoder.Decode(&input) != nil {
		os.Exit(2)
	}
	encoder := json.NewEncoder(os.Stdout)
	_ = encoder.Encode(wireEvent{Type: "ready", Capabilities: &wireCapabilities{Chat: true, Streaming: true, Tools: true, Stateful: true, Resume: true}})
	if decoder.Decode(&input) != nil {
		os.Exit(0)
	}
	_ = encoder.Encode(wireEvent{Type: "session_updated", ResumeID: "https://example.test/thread/created"})
	_ = encoder.Encode(wireEvent{Type: "text", Text: "hello"})
	_ = encoder.Encode(wireEvent{Type: "done"})
	os.Exit(0)
}

func TestExternalACPRejectsUnsupportedToolStrategies(t *testing.T) {
	for _, strategy := range []string{"prompt", "wire", "both", " Prompt ", "unknown"} {
		t.Run(strategy, func(t *testing.T) {
			_, err := NewDriver(Config{Driver: " ACP ", Command: []string{"go"}, ToolStrategy: strategy})
			if err == nil || !strings.Contains(err.Error(), "ACP driver supports only auto or mcp tool_strategy") {
				t.Fatalf("expected unsupported ACP tool strategy error, got %v", err)
			}
		})
	}
}

func TestExternalDriverToolStrategyNormalization(t *testing.T) {
	t.Setenv("CUSTOM_DRIVER_TEST_PROCESS", "1")
	d, err := NewDriver(Config{Command: testCustomCommand(t), ToolStrategy: "AUTO"})
	if err != nil {
		t.Fatal(err)
	}
	if d.toolStrategy != ToolStrategyBoth {
		t.Fatalf("expected auto strategy to normalize to %q, got %q", ToolStrategyBoth, d.toolStrategy)
	}

	d, err = NewDriver(Config{Command: testCustomCommand(t), ToolStrategy: "Prompt"})
	if err != nil {
		t.Fatal(err)
	}
	if d.toolStrategy != ToolStrategyPrompt {
		t.Fatalf("expected %q, got %q", ToolStrategyPrompt, d.toolStrategy)
	}
}

func TestExternalNewDispatchesDriver(t *testing.T) {
	t.Setenv("CUSTOM_DRIVER_TEST_PROCESS", "1")
	e, err := NewDriver(Config{Command: testCustomCommand(t)})
	if err != nil {
		t.Fatal(err)
	}
	if !e.Capabilities().Stateful {
		t.Fatalf("expected stateful capabilities, got %+v", e.Capabilities())
	}

	e, err = NewDriver(Config{Driver: "acp", Command: []string{"go"}})
	if err != nil {
		t.Fatal(err)
	}
	if !e.Capabilities().Stateful || e.toolStrategy != ToolStrategyMCP {
		t.Fatalf("unexpected ACP driver state: %+v, toolStrategy=%q", e.Capabilities(), e.toolStrategy)
	}
}

func TestDriverModelsAndEffortResolution(t *testing.T) {
	t.Setenv("MODELS_DRIVER_TEST_PROCESS", "1")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	engine := &Driver{
		command: []string{executable, "-test.run=^TestDriverModelsProcess$"},
	}
	models, err := engine.Models(context.Background())
	if err != nil || len(models) != 1 || len(models[0].ReasoningEfforts) != 2 {
		t.Fatal(models, err)
	}
	for _, tc := range []struct{ model, effort, want string }{
		{"model", "high", "model"},
		{"model", "", "model"},
		{"default", "", ""},
	} {
		got, err := engine.Resolve(context.Background(), tc.model, tc.effort)
		if err != nil || got != tc.want {
			t.Fatal(got, err)
		}
	}
	if _, err := engine.Resolve(context.Background(), "model", "unknown"); err == nil {
		t.Fatal("accepted unknown effort")
	}
	if _, err := engine.Resolve(context.Background(), "default", "high"); err == nil {
		t.Fatal("accepted effort for backend default")
	}
}

func TestDriverModelsProcess(t *testing.T) {
	if os.Getenv("MODELS_DRIVER_TEST_PROCESS") != "1" {
		return
	}
	decoder := json.NewDecoder(os.Stdin)
	var input map[string]any
	if decoder.Decode(&input) != nil {
		os.Exit(2)
	}
	encoder := json.NewEncoder(os.Stdout)
	_ = encoder.Encode(wireEvent{
		Type: "models",
		Models: []wireModelInfo{
			{ID: "model", Name: "Model", ReasoningEfforts: []string{"low", "high"}},
		},
	})
	os.Exit(0)
}

func TestStatelessDriverDoAndStream(t *testing.T) {
	t.Setenv("STATELESS_DRIVER_TEST_PROCESS", "1")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	d, err := New(context.Background(), Config{
		ID:      "p1",
		Command: []string{executable, "-test.run=^TestStatelessDriverProcess$"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if d.Capabilities().Stateful || !d.Capabilities().SafeReplay {
		t.Fatalf("unexpected stateless capabilities: %+v", d.Capabilities())
	}

	out, callErr := d.Do(context.Background(), model.Target{Provider: "p1", Model: "m1"}, model.ExecuteRequest{
		Kind: model.RequestChat,
		Body: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
	})
	if callErr != nil {
		t.Fatal(callErr)
	}
	if string(out) != `{"ok":true}` {
		t.Fatalf("unexpected response: %s", string(out))
	}
}

func TestStatelessDriverProcess(t *testing.T) {
	if os.Getenv("STATELESS_DRIVER_TEST_PROCESS") != "1" {
		return
	}
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for {
		var msg wireInbound
		if err := decoder.Decode(&msg); err != nil {
			os.Exit(0)
		}
		switch msg.Type {
		case "init", "start":
			_ = encoder.Encode(wireEvent{
				Type: "ready",
				Capabilities: &wireCapabilities{
					Chat:       true,
					Responses:  true,
					Streaming:  true,
					Tools:      true,
					SafeReplay: true,
				},
			})
			if msg.Type == "init" {
				os.Exit(0)
			}
		case "request":
			_ = encoder.Encode(wireEvent{
				Type: "response",
				ID:   msg.ID,
				Data: json.RawMessage(`{"ok":true}`),
			})
		}
	}
}

func TestPreparedDriverStartsWithoutRepeatingDiscovery(t *testing.T) {
	t.Setenv("PREPARED_DRIVER_TEST_PROCESS", "1")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	d, err := New(context.Background(), Config{
		ID: "p", Command: []string{executable, "-test.run=^TestPreparedDriverProcess$"},
		Description: &Description{Capabilities: model.Capabilities{Chat: true, Streaming: true, SafeReplay: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
}

func TestPreparedDriverProcess(t *testing.T) {
	if os.Getenv("PREPARED_DRIVER_TEST_PROCESS") != "1" {
		return
	}
	var input wireInbound
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	if decoder.Decode(&input) != nil {
		os.Exit(2)
	}
	if input.Type != "start" {
		_ = encoder.Encode(wireEvent{Type: "error", Error: "preparation was repeated at startup"})
		os.Exit(0)
	}
	_ = encoder.Encode(wireEvent{Type: "ready", Capabilities: &wireCapabilities{Chat: true, Streaming: true, SafeReplay: true}})
	_ = decoder.Decode(&input)
	os.Exit(0)
}
