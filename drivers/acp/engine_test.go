package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/dmora/agentrun"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func init() {
	if len(os.Args) > 1 && os.Args[1] == "__mcp-bridge" {
		if err := runMCPBridge(context.Background(), os.Args[2:]); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
}

type catalogEngine struct {
	models []agentrun.ModelInfo
	err    error
	lists  int
}

func (e *catalogEngine) Validate() error { return nil }
func (e *catalogEngine) Start(context.Context, agentrun.Session, ...agentrun.Option) (agentrun.Process, error) {
	return nil, errors.New("not used")
}
func (e *catalogEngine) ListModels(context.Context, agentrun.Session) ([]agentrun.ModelInfo, error) {
	e.lists++
	return e.models, e.err
}

func TestEffortSelectionAndCatalogCache(t *testing.T) {
	source := &catalogEngine{models: []agentrun.ModelInfo{
		{ID: "model[low]", Name: "Model low"},
		{ID: "model[high]", Name: "Model high", Aliases: []string{"alias"}},
	}}
	engine := &Engine{engine: source}
	models, err := engine.Models(context.Background())
	if err != nil || len(models) != 1 || len(models[0].ReasoningEfforts) != 2 {
		t.Fatal(models, err)
	}
	for _, tc := range []struct{ model, effort, want string }{
		{"model", "high", "model[high]"}, {"model", "", "model[low]"}, {"default", "", ""},
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
	if !engine.acceptsModel("model[high]", "alias") || engine.acceptsModel("model[high]", "other") {
		t.Fatal("bad effective model validation")
	}
	if _, err := engine.Models(context.Background()); err != nil || source.lists != 1 {
		t.Fatal("catalog not cached", source.lists, err)
	}
	engine.catalogAt = engine.catalogAt.Add(-2 * catalogTTL)
	source.err = errors.New("temporarily unavailable")
	if models, err = engine.Models(context.Background()); err != nil || len(models) != 1 {
		t.Fatal("lost last known catalog", models, err)
	}
}

func TestMCPBridgeFacadeRoundTrip(t *testing.T) {
	tools := []Tool{{
		Name:        "echo",
		Description: "Echoes input",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"x":{"type":"integer"}},"required":["x"]}`),
	}}
	facade, err := NewFacade(context.Background(), tools, func(_ context.Context, name string, args json.RawMessage) (string, error) {
		return name + ":" + string(args), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer facade.Close()

	desc := facade.Descriptor()
	cmd := exec.Command(desc.Command, desc.Args...)
	clientSession, err := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil).Connect(context.Background(), &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()

	listed, err := clientSession.ListTools(context.Background(), nil)
	if err != nil || len(listed.Tools) != 1 || listed.Tools[0].Name != "echo" {
		t.Fatalf("unexpected tools list: %+v, err=%v", listed, err)
	}
	res, err := clientSession.CallTool(context.Background(), &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"x": 42}})
	if err != nil || res.IsError || len(res.Content) != 1 {
		t.Fatalf("unexpected tool result: %+v, err=%v", res, err)
	}
	textContent, ok := res.Content[0].(*mcp.TextContent)
	if !ok || textContent.Text != `echo:{"x":42}` {
		t.Fatalf("unexpected text content: %+v", res.Content[0])
	}
}
