package executor

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
)

func TestMiddlewareIsolationAndStreamLifetime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstream := make(chan model.StreamEvent)
	var finishes atomic.Int32
	finished := make(chan struct{}, 1)
	fake := &model.FakeExecutor{StreamFunc: func(_ context.Context, _ model.Target, req model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		if string(req.Body) != "mutated" {
			t.Error("missing middleware preparation")
		}
		return upstream, nil
	}}
	registry := NewRegistry(map[string]Binding{"p": {Executor: fake, Capabilities: model.Capabilities{Chat: true, Streaming: true}}}, Middleware{
		Prepare: func(_ context.Context, _ model.Target, req *model.ExecuteRequest) *model.CallError {
			copy(req.Body, "mutated")
			return nil
		},
		Event: func(_ context.Context, _ model.Target, _ model.ExecuteRequest, event *model.StreamEvent) {
			event.Meaningful = true
		},
		Finish: func(context.Context, model.Target, model.ExecuteRequest, *model.CallError) {
			finishes.Add(1)
			finished <- struct{}{}
		},
	})
	req := model.ExecuteRequest{Kind: model.RequestChat, Body: []byte("initial")}
	stream, err := registry.Stream(ctx, model.Target{Provider: "p"}, req)
	if err != nil {
		t.Fatal(err)
	}
	if string(req.Body) != "initial" {
		t.Fatal("middleware mutated shared request")
	}
	if finishes.Load() != 0 {
		t.Fatal("finished before stream ended")
	}
	upstream <- model.StreamEvent{Data: []byte(`{}`)}
	if event := <-stream; !event.Meaningful {
		t.Fatal("event middleware not applied")
	}
	close(upstream)
	for range stream {
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("missing stream finish hook")
	}
	if finishes.Load() != 1 {
		t.Fatal("finish called more than once")
	}
}
func TestRegistryRejectsUnsupportedProtocolBeforeDispatch(t *testing.T) {
	var called bool
	fake := &model.FakeExecutor{DoFunc: func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError) {
		called = true
		return nil, nil
	}}
	registry := NewRegistry(map[string]Binding{"p": {Executor: fake, Capabilities: model.Capabilities{Chat: true, Stateful: true}}})
	_, err := registry.Do(context.Background(), model.Target{Provider: "p"}, model.ExecuteRequest{Kind: model.RequestResponses})
	if err == nil || err.Status != 400 || called {
		t.Fatal("dispatched unsupported protocol")
	}
	_, err = registry.Do(context.Background(), model.Target{Provider: "missing"}, model.ExecuteRequest{Kind: model.RequestChat})
	if err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatal(err)
	}
}
