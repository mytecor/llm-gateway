package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPrivateExchangePublicCheckpointAndResume(t *testing.T) {
	cwd := t.TempDir()
	path := filepath.Join(t.TempDir(), "sessions.json")
	engine := &fakeEngine{}
	r := newRuntime(t, engine, Config{DefaultCWD: cwd, StorePath: path})
	req := request("original")
	req.Tools = nil
	x, err := r.Begin(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Abort()
	req.Exchange = x
	req.ResponseSchema = `{"type":"object"}`
	if _, err = r.Exchange(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.registry.store.get(x.key); ok {
		t.Fatal("checkpoint before validation")
	}
	req.Correction = "private feedback"
	if _, err = r.Exchange(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	if len(engine.specs) != 1 || !strings.Contains(engine.prompts[0], req.ResponseSchema) || engine.prompts[1] != "private feedback" {
		t.Fatalf("native prompts: %v", engine.prompts)
	}
	// The entire exchange owns the gate, including the interval between turns.
	blocked, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = r.Begin(blocked, req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gate not held: %v", err)
	}
	if err = x.Commit(`{"ok":true}`); err != nil {
		t.Fatal(err)
	}
	saved, ok := r.registry.store.get(x.key)
	expected := []transcriptMessage{{Role: "user", Content: "original"}, {Role: "assistant", Content: `{"ok":true}`}}
	if !ok || saved.HistoryCount != len(expected) || saved.HistoryHash != transcriptHash(expected) {
		t.Fatalf("public checkpoint: %+v", saved)
	}
	disk, _ := os.ReadFile(path)
	if strings.Contains(string(disk), "private feedback") || strings.Contains(string(disk), "original") {
		t.Fatal("transcript persisted")
	}
	req.Exchange = nil
	req.Correction = ""
	req.ResponseSchema = ""
	req.Messages = append(req.Messages, message("assistant", `{"ok":true}`), message("user", "next"))
	if _, err = r.Exchange(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	if len(engine.specs) != 1 || engine.prompts[2] != "next" {
		t.Fatalf("public next turn: %v", engine.prompts)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	restoredEngine := &fakeEngine{}
	restored := newRuntime(t, restoredEngine, Config{DefaultCWD: cwd, StorePath: path})
	req.Messages = append(req.Messages, message("assistant", "answer"), message("user", "after restart"))
	if _, err = restored.Exchange(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	if len(restoredEngine.specs) != 1 || restoredEngine.specs[0].ResumeID != "native-session" || restoredEngine.prompts[0] != "after restart" {
		t.Fatalf("resume: %v %v", restoredEngine.specs, restoredEngine.prompts)
	}
}

func TestPrivateExchangeAbortAndMissingCorrectionSession(t *testing.T) {
	engine := &fakeEngine{}
	r := newRuntime(t, engine, Config{})
	req := request("original")
	req.Tools = nil
	x, err := r.Begin(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Abort()
	req.Exchange = x
	if _, err = r.Exchange(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	engine.run = func(context.Context, Spec, string, func(Event) error) error { return ErrSessionNotFound }
	req.Correction = "fix only"
	if _, err = r.Exchange(context.Background(), req, nil); !errors.Is(err, ErrSessionNotFound) {
		t.Fatal(err)
	}
	if len(engine.specs) != 1 || len(engine.prompts) != 2 {
		t.Fatal("replayed original after correction lost session")
	}
	if err = x.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.registry.store.get(x.key); ok || x.state.process != nil {
		t.Fatal("abort retained session")
	}
	engine.run = nil
	req.Exchange = nil
	req.Correction = ""
	if _, err = r.Exchange(context.Background(), req, nil); err != nil {
		t.Fatal("abort did not release gate", err)
	}
}

func TestPrivateExchangeCancellationClearsCheckpoint(t *testing.T) {
	engine := &fakeEngine{}
	r := newRuntime(t, engine, Config{})
	req := request("original")
	req.Tools = nil
	x, err := r.Begin(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Abort()
	req.Exchange = x
	if _, err = r.Exchange(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	req.Correction = "stall"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = r.Exchange(ctx, req, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err = x.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.registry.store.get(x.key); ok || x.state.process != nil {
		t.Fatal("cancel retained state")
	}
}
