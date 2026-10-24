package external

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/session"
)

func TestExternalDriverConfigValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error on empty command")
	}
	if _, err := New(Config{Command: []string{"nonexistent-binary-12345"}}); err == nil {
		t.Fatal("expected error on nonexistent binary")
	}
	if _, err := New(Config{Protocol: "unknown", Command: []string{"go"}}); err == nil {
		t.Fatal("expected error on unknown protocol")
	}
}

// Exercise the actual subprocess wire, including IDs assigned after the first prompt.
func TestDriverSessionUpdateWire(t *testing.T) {
	t.Setenv("WEB_DRIVER_TEST_PROCESS", "1")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewDriver(Config{Command: []string{executable, "-test.run=^TestDriverSessionUpdateProcess$"}, ToolStrategy: ToolStrategyPrompt})
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
	if os.Getenv("WEB_DRIVER_TEST_PROCESS") != "1" {
		return
	}
	decoder := json.NewDecoder(os.Stdin)
	var input map[string]any
	if decoder.Decode(&input) != nil {
		os.Exit(2)
	}
	encoder := json.NewEncoder(os.Stdout)
	_ = encoder.Encode(wireEvent{Type: "ready"})
	if decoder.Decode(&input) != nil {
		os.Exit(2)
	}
	_ = encoder.Encode(wireEvent{Type: "session_updated", ResumeID: "https://example.test/thread/created"})
	_ = encoder.Encode(wireEvent{Type: "text", Text: "hello"})
	_ = encoder.Encode(wireEvent{Type: "done"})
	os.Exit(0)
}

func TestExternalACPRejectsUnsupportedToolStrategies(t *testing.T) {
	for _, strategy := range []string{"prompt", "wire", "both", " Prompt ", "unknown"} {
		t.Run(strategy, func(t *testing.T) {
			_, err := New(Config{Protocol: " ACP ", Command: []string{"go"}, ToolStrategy: strategy})
			if err == nil || !strings.Contains(err.Error(), "ACP protocol supports only auto or mcp tool_strategy") {
				t.Fatalf("expected unsupported ACP tool strategy error, got %v", err)
			}
		})
	}
}

func TestExternalDriverToolStrategyNormalization(t *testing.T) {
	d, err := NewDriver(Config{Command: []string{"go"}, ToolStrategy: "AUTO"})
	if err != nil {
		t.Fatal(err)
	}
	if d.toolStrategy != ToolStrategyBoth {
		t.Fatalf("expected auto strategy to normalize to %q, got %q", ToolStrategyBoth, d.toolStrategy)
	}

	d, err = NewDriver(Config{Command: []string{"go"}, ToolStrategy: "Prompt"})
	if err != nil {
		t.Fatal(err)
	}
	if d.toolStrategy != ToolStrategyPrompt {
		t.Fatalf("expected %q, got %q", ToolStrategyPrompt, d.toolStrategy)
	}
}

func TestExternalNewDispatchesProtocol(t *testing.T) {
	ctx := context.Background()
	_ = ctx
	// Driver protocol with "go" binary
	e, err := New(Config{Protocol: ProtocolDriver, Command: []string{"go"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*DriverEngine); !ok {
		t.Fatalf("expected *DriverEngine, got %T", e)
	}

	// Web alias
	e, err = New(Config{Protocol: ProtocolWeb, Command: []string{"go"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*DriverEngine); !ok {
		t.Fatalf("expected *DriverEngine, got %T", e)
	}
}
