package external

import (
	"context"
	"strings"
	"testing"
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
