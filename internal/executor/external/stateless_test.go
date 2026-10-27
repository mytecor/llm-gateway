package external

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
)

func regressionDriver(t *testing.T, mode string) (*Driver, string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(t.TempDir(), "wire.log")
	d, err := New(context.Background(), Config{
		ID: "provider", Command: []string{executable, "-test.run=^TestRegressionDriverProcess$"},
		Env:         map[string]string{"DRIVER_REGRESSION_MODE": mode, "DRIVER_REGRESSION_LOG": log},
		Description: &Description{Capabilities: model.Capabilities{Chat: true, Streaming: true, SafeReplay: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d, log
}

func regressionRequest(body string) model.ExecuteRequest {
	return model.ExecuteRequest{Kind: model.RequestChat, Body: []byte(`{"operation":"` + body + `"}`)}
}

func readWireLog(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestSlowStreamDoesNotBlockOtherRequests(t *testing.T) {
	d, log := regressionDriver(t, "normal")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := d.Stream(ctx, model.Target{}, regressionRequest("flood"))
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately do not read stream. The child emits more frames than both
	// per-request buffers before it can read this independent request.
	data, err := d.Do(ctx, model.Target{}, regressionRequest("ok"))
	if err != nil || string(data) != `{"ok":true}` {
		t.Fatalf("independent request stalled: %s %v", data, err)
	}
	foundFailure := false
	for event := range stream {
		if event.Err != nil {
			foundFailure = true
			if event.Err.Replay != model.ReplayUnknown || event.Err.Scope != model.FailureScopeRequest {
				t.Fatalf("unexpected backpressure failure: %+v", event.Err)
			}
		}
	}
	if !foundFailure {
		t.Fatal("slow stream did not fail within the bounded queue")
	}
	// Ask the same child for an acknowledgement after it has consumed cancel.
	if _, err := d.Do(ctx, model.Target{}, regressionRequest("barrier")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(readWireLog(t, log), "cancel\n") {
		t.Fatal("slow stream was not cancelled upstream")
	}
}

func TestCancellingOneStreamKeepsWorkerAvailable(t *testing.T) {
	d, _ := regressionDriver(t, "normal")
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := d.Stream(ctx, model.Target{}, regressionRequest("hold"))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	for range stream {
	}
	other, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := d.Do(other, model.Target{}, regressionRequest("ok")); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRestartsWithoutReplayingAcceptedRequests(t *testing.T) {
	d, log := regressionDriver(t, "fail-second-start")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	_, failure := d.Do(ctx, model.Target{}, regressionRequest("crash"))
	if failure == nil || failure.Replay != model.ReplayUnknown {
		t.Fatalf("lost replay safety after crash: %+v", failure)
	}
	data, failure := d.Do(ctx, model.Target{}, regressionRequest("ok"))
	if failure != nil || string(data) != `{"ok":true}` {
		t.Fatalf("new worker unavailable: %s %v", data, failure)
	}
	wire := readWireLog(t, log)
	if strings.Count(wire, "request:crash\n") != 1 || strings.Count(wire, "request:ok\n") != 1 || strings.Count(wire, "start\n") != 3 {
		t.Fatalf("unexpected restart/replay sequence:\n%s", wire)
	}
}

func TestRecoveryWaitRespectsRequestCancellationAndDriverClose(t *testing.T) {
	d, log := regressionDriver(t, "normal")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := d.Do(ctx, model.Target{}, regressionRequest("crash")); err == nil {
		t.Fatal("expected crash")
	}
	wait, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, err := d.Do(wait, model.Target{}, regressionRequest("ok")); err == nil || err.Class != model.ErrorTimeout {
		t.Fatalf("recovery ignored deadline: %+v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// The supervisor is joined by Close; no timer can launch another worker.
	if wire := readWireLog(t, log); strings.Count(wire, "start\n") != 1 || strings.Contains(wire, "request:ok") {
		t.Fatalf("work submitted after cancellation: %s", wire)
	}
}

func TestBlockedDriverWriteHonorsContext(t *testing.T) {
	d, _ := regressionDriver(t, "block-stdin")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := d.Do(ctx, model.Target{}, regressionRequest(strings.Repeat("x", 2<<20)))
	if err == nil || err.Class != model.ErrorTimeout || err.Replay != model.ReplayUnknown {
		t.Fatalf("blocked pipe ignored cancellation: %+v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("cancelled pipe write did not return promptly")
	}
}

// The real subprocess wire exercises queues, EOF, restarts and cancellation.
func TestRegressionDriverProcess(t *testing.T) {
	mode := os.Getenv("DRIVER_REGRESSION_MODE")
	if mode == "" {
		return
	}
	path := os.Getenv("DRIVER_REGRESSION_LOG")
	appendLog := func(line string) {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			os.Exit(2)
		}
		_, _ = file.WriteString(line + "\n")
		_ = file.Close()
	}
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for {
		var command wireInbound
		if err := decoder.Decode(&command); err != nil {
			os.Exit(0)
		}
		switch command.Type {
		case "start":
			appendLog("start")
			previous, _ := os.ReadFile(path)
			if mode == "fail-second-start" && strings.Count(string(previous), "start\n") == 2 {
				_ = encoder.Encode(wireEvent{Type: "error", Error: "transient startup failure"})
				os.Exit(3)
			}
			_ = encoder.Encode(wireEvent{Type: "ready", Capabilities: &wireCapabilities{Chat: true, Streaming: true, SafeReplay: true}})
			if mode == "block-stdin" {
				time.Sleep(time.Minute)
				os.Exit(0)
			}
		case "cancel":
			appendLog("cancel")
		case "request":
			var body struct {
				Operation string `json:"operation"`
			}
			_ = json.Unmarshal(command.Body, &body)
			appendLog("request:" + body.Operation)
			if body.Operation == "crash" {
				os.Exit(7)
			}
			if command.Stream {
				_ = encoder.Encode(wireEvent{Type: "stream_opened", ID: command.ID})
				if body.Operation == "flood" {
					for i := 0; i < 1024; i++ {
						_ = encoder.Encode(wireEvent{Type: "chunk", ID: command.ID, Data: json.RawMessage(`{"text":"chunk"}`), Meaningful: true})
					}
				}
				continue
			}
			_ = encoder.Encode(wireEvent{Type: "response", ID: command.ID, Data: json.RawMessage(`{"ok":true}`)})
		}
	}
}
