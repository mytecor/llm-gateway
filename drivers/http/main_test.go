package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/driverproto"
)

func TestValidateProviderConfigDoesNotFreezeDefaultAndHonorsCeiling(t *testing.T) {
	rawCfg := json.RawMessage(`{"base_provider":"openai","inference_url":"http://127.0.0.1:9999/v1","allow_private_network":true}`)
	providerCfg, _, normalizedMap, err := ValidateProviderConfig(inboundMessage{
		ID:               "gonka-a",
		Config:           rawCfg,
		RequestTimeoutMs: 180_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if providerCfg.RequestTimeoutSeconds != 180 {
		t.Fatalf("providerCfg.RequestTimeoutSeconds = %d, want 180", providerCfg.RequestTimeoutSeconds)
	}
	if _, frozen := normalizedMap["request_timeout_seconds"]; frozen {
		t.Fatalf("normalizedMap unexpectedly froze default request_timeout_seconds: %#v", normalizedMap)
	}

	explicitCfg := json.RawMessage(`{"base_provider":"openai","inference_url":"http://127.0.0.1:9999/v1","allow_private_network":true,"request_timeout_seconds":45}`)
	providerCfg, _, normalizedMap, err = ValidateProviderConfig(inboundMessage{
		ID:               "gonka-b",
		Config:           explicitCfg,
		RequestTimeoutMs: 180_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if providerCfg.RequestTimeoutSeconds != 180 {
		t.Fatalf("providerCfg.RequestTimeoutSeconds = %d, want 180 ceiling", providerCfg.RequestTimeoutSeconds)
	}
	if got, ok := normalizedMap["request_timeout_seconds"].(float64); !ok || got != 45 {
		t.Fatalf("normalizedMap[request_timeout_seconds] = %#v, want 45", normalizedMap["request_timeout_seconds"])
	}
}

func startTestServePipe(t *testing.T) (send func(inboundMessage), recv func(timeout time.Duration) outboundMessage) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, inR, outW)
		_ = outW.Close()
	}()
	t.Cleanup(func() {
		cancel()
		_ = inW.Close()
		_ = outR.Close()
		<-done
	})

	events := make(chan outboundMessage, 16)
	go func() {
		scanner := bufio.NewScanner(outR)
		scanner.Buffer(make([]byte, 4096), 4<<20)
		for scanner.Scan() {
			var ev outboundMessage
			if err := json.Unmarshal(scanner.Bytes(), &ev); err == nil {
				events <- ev
			}
		}
	}()

	send = func(msg inboundMessage) {
		msg.ProtocolVersion = driverproto.Version
		data, err := json.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := inW.Write(append(data, '\n')); err != nil {
			t.Fatal(err)
		}
	}
	recv = func(timeout time.Duration) outboundMessage {
		select {
		case ev := <-events:
			return ev
		case <-time.After(timeout):
			t.Fatalf("timed out waiting for driver event after %s", timeout)
			return outboundMessage{}
		}
	}
	return send, recv
}

func startBlockingUpstream(t *testing.T) (url string, started, cancelled <-chan struct{}, release func()) {
	t.Helper()
	startedCh := make(chan struct{}, 1)
	cancelledCh := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	var once sync.Once
	releaseFn := func() { once.Do(func() { close(releaseCh) }) }

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case startedCh <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
			cancelledCh <- struct{}{}
		case <-releaseCh:
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(func() {
		releaseFn()
		server.Close()
	})
	return server.URL, startedCh, cancelledCh, releaseFn
}

func TestServePerAttemptTimeoutReturnsBeforeClientCeiling(t *testing.T) {
	serverURL, started, cancelled, release := startBlockingUpstream(t)
	defer release()

	send, recv := startTestServePipe(t)
	cfgJSON, _ := json.Marshal(map[string]any{
		"base_provider":         "openai",
		"inference_url":         serverURL + "/v1",
		"allow_private_network": true,
	})
	send(inboundMessage{Type: "start", ID: "p1", Config: cfgJSON, RequestTimeoutMs: 5000})
	if ready := recv(3 * time.Second); ready.Type != "ready" {
		t.Fatalf("expected ready event, got %+v", ready)
	}

	startedAt := time.Now()
	payload := json.RawMessage(`{"model":"m1","messages":[{"role":"user","content":"hi"}]}`)
	send(inboundMessage{
		Type:             "request",
		ID:               "req-1",
		Provider:         "p1",
		Model:            "m1",
		Kind:             "chat",
		RequestTimeoutMs: 80,
		Body:             payload,
	})

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream HTTP server never received request")
	}

	ev := recv(2 * time.Second)
	if ev.Type != "error" || ev.CallError == nil || ev.CallError.Class != "timeout" || ev.CallError.Status != 504 {
		t.Fatalf("expected timeout CallError (504), got %+v", ev)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("per-attempt timeout waited %s (likely hit client ceiling instead of RequestTimeoutMs)", elapsed)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("attempt timeout did not close the physical upstream request")
	}
}

func TestServeCancelCommandReturnsImmediately(t *testing.T) {
	serverURL, started, cancelled, release := startBlockingUpstream(t)
	defer release()

	send, recv := startTestServePipe(t)
	cfgJSON, _ := json.Marshal(map[string]any{
		"base_provider":         "openai",
		"inference_url":         serverURL + "/v1",
		"allow_private_network": true,
	})
	send(inboundMessage{Type: "start", ID: "p1", Config: cfgJSON, RequestTimeoutMs: 5000})
	if ready := recv(3 * time.Second); ready.Type != "ready" {
		t.Fatalf("expected ready event, got %+v", ready)
	}

	payload := json.RawMessage(`{"model":"m1","messages":[{"role":"user","content":"hi"}]}`)
	send(inboundMessage{
		Type:             "request",
		ID:               "req-cancel",
		Provider:         "p1",
		Model:            "m1",
		Kind:             "chat",
		RequestTimeoutMs: 5000,
		Body:             payload,
	})

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream HTTP server never received request")
	}

	send(inboundMessage{Type: "cancel", ID: "req-cancel"})

	ev := recv(2 * time.Second)
	if ev.Type != "error" || ev.CallError == nil || ev.CallError.Class != "cancelled" {
		t.Fatalf("expected cancelled CallError, got %+v", ev)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("cancel command did not close the physical upstream request")
	}
}

func TestServeCancelLeavesConcurrentRequestRunning(t *testing.T) {
	started := make(chan string, 2)
	cancelled := make(chan string, 2)
	release := make(chan struct{})
	defer close(release)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		started <- body.Model
		select {
		case <-r.Context().Done():
			cancelled <- body.Model
			return
		case <-release:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"ok","object":"chat.completion","model":"keep","choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`)
	}))
	t.Cleanup(upstream.Close)
	send, recv := startTestServePipe(t)
	cfg, _ := json.Marshal(map[string]any{"base_provider": "openai", "inference_url": upstream.URL, "allow_private_network": true})
	send(inboundMessage{Type: "start", ID: "p", Config: cfg, RequestTimeoutMs: 5000})
	if ev := recv(3 * time.Second); ev.Type != "ready" {
		t.Fatalf("expected ready, got %+v", ev)
	}
	for _, id := range []string{"cancel", "keep"} {
		send(inboundMessage{Type: "request", ID: id, Provider: "p", Model: id, Kind: "chat", Body: json.RawMessage(`{"messages":[{"role":"user","content":"hi"}]}`)})
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent upstream request did not start")
		}
	}
	send(inboundMessage{Type: "cancel", ID: "cancel"})
	ev := recv(time.Second)
	if ev.ID != "cancel" || ev.CallError == nil || ev.CallError.Class != "cancelled" {
		t.Fatalf("unexpected cancellation: %+v", ev)
	}
	select {
	case id := <-cancelled:
		if id != "cancel" {
			t.Fatalf("cancelled wrong request: %s", id)
		}
	case <-time.After(time.Second):
		t.Fatal("upstream request was not cancelled")
	}
	release <- struct{}{}
	ev = recv(time.Second)
	if ev.ID != "keep" || ev.Type != "response" {
		t.Fatalf("concurrent request did not survive cancellation: %+v", ev)
	}
}
