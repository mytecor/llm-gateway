package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/driverproto"
	"github.com/mytecor/llm-gateway/driverproto/systemone"
)

const decision = `{"model":"jev","state":"test","questions":{"q":{"type":"noul","instructions":"True?"}}}`
const answer = `{"model":"jev-1.14.0","answers":{"q":{"type":"noul","noul":0.1234567890123456789}}}`

func fixtureExecutor(t *testing.T, dialect string, handler http.HandlerFunc) *executor {
	t.Helper()
	api := httptest.NewServer(handler)
	t.Cleanup(api.Close)
	cfg, _ := json.Marshal(config{Dialect: dialect, InferenceURL: api.URL + "/v1", APIKey: "fixture-key"})
	e, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func TestSingleAttemptAndNativeErrors(t *testing.T) {
	for _, tc := range []struct {
		status             int
		class, scope, code string
		public             int
	}{
		{403, "5xx", "provider", "provider_not_eligible", 503}, {429, "429", "provider", "jev_rate_limited", 429}, {529, "5xx", "provider", "jev_upstream_error", 503}, {422, "invalid_response", "request", "invalid_systemone_request", 422}, {401, "5xx", "provider", "jev_upstream_error", 502},
	} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			var calls atomic.Int32
			e := fixtureExecutor(t, "gonka", func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Retry-After", "2")
				w.WriteHeader(tc.status)
			})
			_, err := e.do(context.Background(), command{Kind: "systemone", Model: "jev", SingleAttempt: true, Body: json.RawMessage(decision)})
			if err == nil || err.Class != tc.class || err.Scope != tc.scope || err.Status != tc.public || err.Code != tc.code || calls.Load() != 1 || err.RetryAfterMs != 2000 {
				t.Fatalf("calls=%d error=%+v", calls.Load(), err)
			}
		})
	}
}
func TestRequestHeadersAndResponseValidation(t *testing.T) {
	var calls atomic.Int32
	e := fixtureExecutor(t, "typesafe", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/systemone" || r.Header.Get("Authorization") != "Bearer fixture-key" || r.Header.Get(systemone.LongContextHeader) != "true" {
			t.Error("bad upstream request")
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["model"] != "jev-latest" {
			t.Error("native model not mapped")
		}
		_, _ = io.WriteString(w, answer)
	})
	result, err := e.do(context.Background(), command{Kind: "systemone", Model: "jev-latest", Headers: map[string]string{systemone.LongContextHeader: "true"}, Body: json.RawMessage(decision)})
	if err != nil || !strings.Contains(string(result), "0.1234567890123456789") || calls.Load() != 1 {
		t.Fatalf("%s %+v", result, err)
	}
	_, err = e.do(context.Background(), command{Kind: "systemone", Model: "jev-latest", Headers: map[string]string{"Authorization": "bad"}, Body: json.RawMessage(decision)})
	if err == nil || calls.Load() != 1 {
		t.Fatal("arbitrary headers reached upstream")
	}
	e = fixtureExecutor(t, "typesafe", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, strings.Replace(answer, `"noul":0.1234567890123456789`, `"noul":2`, 1))
	})
	_, err = e.do(context.Background(), command{Kind: "systemone", Model: "jev-latest", Body: json.RawMessage(decision)})
	if err == nil || err.Code != "invalid_systemone_response" {
		t.Fatalf("invalid answer accepted: %+v", err)
	}
}
func TestWorkerMultiplexingAndCancellation(t *testing.T) {
	slowStarted := make(chan struct{}, 1)
	slowCancelled := make(chan struct{}, 1)
	e := fixtureExecutor(t, "typesafe", func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if strings.Contains(string(data), "slow") {
			slowStarted <- struct{}{}
			<-r.Context().Done()
			slowCancelled <- struct{}{}
			return
		}
		_, _ = io.WriteString(w, answer)
	})
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, inR, outW); _ = outW.Close() }()
	defer inR.Close()
	defer inW.Close()
	defer outR.Close()
	enc := json.NewEncoder(inW)
	events := make(chan driverproto.Event, 8)
	go func() {
		scan := bufio.NewScanner(outR)
		for scan.Scan() {
			var event driverproto.Event
			_ = json.Unmarshal(scan.Bytes(), &event)
			events <- event
		}
	}()
	cfg, _ := json.Marshal(e.config)
	_ = enc.Encode(command{Type: "start", Config: cfg})
	if ev := <-events; ev.Type != "ready" || !ev.Capabilities.SystemOne {
		t.Fatalf("%+v", ev)
	}
	_ = enc.Encode(command{Type: "request", ID: "slow", Kind: "systemone", Model: "jev-latest", Body: json.RawMessage(strings.Replace(decision, `"test"`, `"slow"`, 1))})
	select {
	case <-slowStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("slow request not started")
	}
	_ = enc.Encode(command{Type: "request", ID: "fast", Kind: "systemone", Model: "jev-latest", Body: json.RawMessage(decision)})
	select {
	case ev := <-events:
		if ev.ID != "fast" || ev.Type != "response" {
			t.Fatalf("fast call blocked: %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not multiplex")
	}
	_ = enc.Encode(command{Type: "cancel", ID: "slow"})
	select {
	case <-slowCancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream not cancelled")
	}
	select {
	case ev := <-events:
		if ev.ID != "slow" || ev.CallError == nil || ev.CallError.Class != "cancelled" {
			t.Fatalf("bad cancellation %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing cancellation event")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not shut down")
	}
}
func TestRetryAfterDate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	if got := retryAfter(now.Add(time.Minute).Format(http.TimeFormat), now); got != time.Minute {
		t.Fatal(got)
	}
}
