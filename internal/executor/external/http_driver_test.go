package external

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

func httpTestConfig(t *testing.T, upstreamURL string) *policy.CompiledConfig {
	t.Helper()
	cfg := policy.TestConfig()
	cfg.Providers = cfg.Providers[:1]
	cfg.Providers[0].ID = "mock-openai"
	cfg.Providers[0].Driver = DriverHTTP
	cfg.Providers[0].InferenceURL = upstreamURL
	cfg.Providers[0].APIKey = "provider-secret"
	cfg.Providers[0].AllowPrivateNetwork = true
	cfg.Rules = []policy.Rule{
		policy.FilterModel("standard", "standard"),
		policy.FilterProvider("standard", "mock-openai"),
		policy.MapRuleHelper("standard", "native-model"),
		policy.RankRuleHelper("standard"),
		policy.RaceRuleHelper("standard", 1),
	}
	compiled, err := policy.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestHTTPDriverExecutorChatAndResponses(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer provider-secret" {
			t.Errorf("provider credential missing")
		}
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["model"] != "native-model" {
			t.Errorf("logical model reached upstream: %#v", body["model"])
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/chat/completions":
			_, _ = fmt.Fprint(writer, `{"id":"chat-1","object":"chat.completion","created":1,"model":"native-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
		case "/responses":
			_, _ = fmt.Fprint(writer, `{"id":"resp-1","object":"response","created_at":1,"completed_at":1,"status":"completed","model":"native-model","output":[],"error":null,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer upstream.Close()

	compiled := httpTestConfig(t, upstream.URL)
	executor, err := NewHTTP(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()

	chatBody, callErr := executor.Do(context.Background(), model.Target{Provider: "mock-openai", Model: "native-model"}, model.ExecuteRequest{
		Kind: model.RequestChat,
		Body: []byte(`{"model":"standard","messages":[{"role":"user","content":"hello"}]}`),
	})
	if callErr != nil {
		t.Fatal(callErr)
	}
	var chatResp map[string]any
	if err := json.Unmarshal(chatBody, &chatResp); err != nil || chatResp["model"] != "native-model" {
		t.Fatalf("unexpected chat response: %s (%v)", chatBody, err)
	}

	respBody, callErr := executor.Do(context.Background(), model.Target{Provider: "mock-openai", Model: "native-model"}, model.ExecuteRequest{
		Kind: model.RequestResponses,
		Body: []byte(`{"model":"standard","input":"hello"}`),
	})
	if callErr != nil {
		t.Fatal(callErr)
	}
	var respObj map[string]any
	if err := json.Unmarshal(respBody, &respObj); err != nil || respObj["object"] != "response" {
		t.Fatalf("unexpected responses payload: %s (%v)", respBody, err)
	}
}

func TestHTTPDriverExecutorStreaming(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		flusher := writer.(http.Flusher)
		messages := []string{
			`{"id":"chat-1","object":"chat.completion.chunk","created":1,"model":"native-model","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`,
			`{"id":"chat-1","object":"chat.completion.chunk","created":1,"model":"native-model","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}`,
		}
		for _, message := range messages {
			_, _ = fmt.Fprintf(writer, "data: %s\n\n", message)
			flusher.Flush()
		}
		_, _ = fmt.Fprint(writer, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer upstream.Close()

	compiled := httpTestConfig(t, upstream.URL)
	executor, err := NewHTTP(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()

	stream, callErr := executor.Stream(context.Background(), model.Target{Provider: "mock-openai", Model: "native-model"}, model.ExecuteRequest{
		Kind: model.RequestChat,
		Body: []byte(`{"model":"standard","stream":true,"messages":[{"role":"user","content":"hello"}]}`),
	})
	if callErr != nil {
		t.Fatal(callErr)
	}
	var meaningful []model.StreamEvent
	deadline := time.After(3 * time.Second)
	for {
		select {
		case event, open := <-stream:
			if !open {
				if len(meaningful) != 1 || !strings.Contains(string(meaningful[0].Data), "hello") {
					t.Fatalf("unexpected meaningful events: %#v", meaningful)
				}
				return
			}
			if event.Err != nil {
				t.Fatal(event.Err)
			}
			if event.Meaningful {
				meaningful = append(meaningful, event)
			}
		case <-deadline:
			t.Fatal("stream did not finish")
		}
	}
}
