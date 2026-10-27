package external

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	sessionexecutor "github.com/mytecor/llm-gateway/internal/executor/session"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/session"
)

func TestSessionEffortRemainsSeparateOnWire(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	d, err := New(context.Background(), Config{
		Command:     []string{executable, "-test.run=^TestSessionEffortProcess$"},
		Env:         map[string]string{"SESSION_EFFORT_PROCESS": "1"},
		Description: &Description{Capabilities: model.Capabilities{Chat: true, Stateful: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	d.SetModelLister(model.ModelListFunc(func(context.Context) ([]model.ModelInfo, error) {
		return []model.ModelInfo{{ID: "model", ReasoningEfforts: []string{"high"}}}, nil
	}))
	executor, err := sessionexecutor.New(d, session.Config{DefaultCWD: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, failure := executor.Do(ctx, model.Target{Model: "model"}, model.ExecuteRequest{
		Kind: model.RequestChat, Call: model.Call{Conversation: "conversation"},
		Body: []byte(`{"messages":[{"role":"user","content":"hello"}],"reasoning_effort":"high"}`),
	})
	if failure != nil {
		t.Fatalf("model/effort were not forwarded separately: %+v", failure)
	}
}

func TestSessionEffortProcess(t *testing.T) {
	if os.Getenv("SESSION_EFFORT_PROCESS") != "1" {
		return
	}
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	var command wireInbound
	if decoder.Decode(&command) != nil {
		os.Exit(2)
	}
	if command.Type != "start" || command.Model != "model" || command.ReasoningEffort != "high" {
		_ = encoder.Encode(wireEvent{Type: "error", Error: "model and effort must be separate"})
		os.Exit(2)
	}
	_ = encoder.Encode(wireEvent{Type: "ready"})
	if decoder.Decode(&command) != nil || command.Type != "prompt" {
		os.Exit(2)
	}
	_ = encoder.Encode(wireEvent{Type: "text", Text: "ok"})
	_ = encoder.Encode(wireEvent{Type: "done"})
	// A native session belongs to the runtime until explicit Close.
	_ = decoder.Decode(&command)
	os.Exit(0)
}
