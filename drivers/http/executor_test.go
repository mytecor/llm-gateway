package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

func testProviders(upstreamURL string) []ProviderConfig {
	return []ProviderConfig{{
		ID:                    "mock-openai",
		BaseProvider:          "openai",
		InferenceURL:          upstreamURL,
		APIKey:                "provider-secret",
		RequestTimeoutSeconds: 60,
		AllowPrivateNetwork:   true,
	}}
}

func TestBifrostKeyForVertexCarriesProjectRegionAndCredentials(t *testing.T) {
	provider := ProviderConfig{
		ID:                    "google-vertex",
		VertexProjectID:       "mytecor",
		VertexRegion:          "global",
		VertexAuthCredentials: `{"type":"service_account"}`,
	}
	key := bifrostKeyForProvider(provider, schemas.Vertex)
	if key.Value.GetValue() != "" {
		t.Fatal("OAuth-backed Vertex provider unexpectedly carries an API key")
	}
	if key.VertexKeyConfig == nil {
		t.Fatal("Vertex key config is missing")
	}
	if key.VertexKeyConfig.ProjectID.GetValue() != "mytecor" || key.VertexKeyConfig.Region.GetValue() != "global" {
		t.Fatalf("unexpected Vertex resource config: %#v", key.VertexKeyConfig)
	}
	if key.VertexKeyConfig.AuthCredentials.GetValue() != `{"type":"service_account"}` {
		t.Fatal("Vertex service-account credentials were not wired into the Bifrost key")
	}
}

func TestBifrostExecutorRegistersVertexAsStandardProvider(t *testing.T) {
	providers := []ProviderConfig{{
		ID:                    "google-vertex",
		BaseProvider:          "vertex",
		InferenceURL:          "https://aiplatform.googleapis.com",
		VertexProjectID:       "mytecor",
		VertexRegion:          "global",
		VertexAuthCredentials: `{"type":"service_account"}`,
	}}
	executor, err := NewExecutor(context.Background(), providers)
	if err != nil {
		t.Fatalf("Vertex must initialize through Bifrost's standard provider path: %v", err)
	}
	defer executor.Close()
	if got := executor.bifrostProviders["google-vertex"]; got != schemas.Vertex {
		t.Fatalf("google-vertex resolved to %q, want %q", got, schemas.Vertex)
	}
	request, callErr := executor.chatRequest(
		schemas.NewBifrostContext(context.Background(), schemas.NoDeadline),
		"google-vertex",
		"gemini-4-argon",
		[]byte(`{"messages":[{"role":"user","content":"hello"}]}`),
	)
	if callErr != nil {
		t.Fatal(callErr)
	}
	if request.Provider != schemas.Vertex {
		t.Fatalf("request provider = %q, want %q", request.Provider, schemas.Vertex)
	}
	responsesRequest, callErr := executor.responsesRequest(
		schemas.NewBifrostContext(context.Background(), schemas.NoDeadline),
		"google-vertex",
		"gemini-4-argon",
		[]byte(`{"input":"hello"}`),
	)
	if callErr != nil {
		t.Fatal(callErr)
	}
	if responsesRequest.Provider != schemas.Vertex {
		t.Fatalf("responses provider = %q, want %q", responsesRequest.Provider, schemas.Vertex)
	}
}

func TestBifrostExecutorCustomProviderChat(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path %s", request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
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
		_, _ = fmt.Fprint(writer, `{"id":"chat-1","object":"chat.completion","created":1,"model":"native-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer upstream.Close()

	executor, err := NewExecutor(context.Background(), testProviders(upstream.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()
	body, callErr := executor.Do(context.Background(), "mock-openai", "native-model", "chat", []byte(`{"model":"standard","messages":[{"role":"user","content":"hello"}]}`))
	if callErr != nil {
		t.Fatal(callErr)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response["model"] != "native-model" {
		t.Fatalf("unexpected Bifrost response: %s", body)
	}
	if _, leaked := response["extra_fields"]; leaked {
		t.Fatalf("Bifrost internal metadata was not removed: %s", body)
	}
}

func TestStripParamsRemovesUnsupportedReasoningControl(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if _, leaked := body["thinking"]; leaked {
			t.Errorf("thinking reached upstream despite strip_params: %#v", body)
		}
		if _, leaked := body["reasoning_effort"]; leaked {
			t.Errorf("reasoning_effort reached upstream despite strip_params: %#v", body)
		}
		if body["model"] != "native-model" {
			t.Errorf("model rewrite lost: %#v", body["model"])
		}
		if body["messages"] == nil {
			t.Errorf("messages were dropped: %#v", body)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(writer, `{"id":"chat-1","object":"chat.completion","created":1,"model":"native-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer upstream.Close()

	providers := testProviders(upstream.URL)
	providers[0].StripParams = []string{"thinking", "reasoning_effort"}
	executor, err := NewExecutor(context.Background(), providers)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()
	_, callErr := executor.Do(context.Background(), "mock-openai", "native-model", "chat", []byte(`{"model":"standard","thinking":{"type":"enabled"},"reasoning_effort":"high","messages":[{"role":"user","content":"hello"}]}`))
	if callErr != nil {
		t.Fatal(callErr)
	}
}

func TestSetParamsForcesDisabledThinking(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		thinking, ok := body["thinking"].(map[string]any)
		if !ok {
			t.Fatalf("thinking control missing after set_params: %#v", body)
		}
		if thinking["type"] != "disabled" {
			t.Errorf("thinking control not forced to disabled: %#v", body["thinking"])
		}
		if body["model"] != "native-model" {
			t.Errorf("model rewrite lost: %#v", body["model"])
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(writer, `{"id":"chat-1","object":"chat.completion","created":1,"model":"native-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer upstream.Close()

	providers := testProviders(upstream.URL)
	providers[0].StripParams = []string{"thinking"}
	providers[0].SetParams = map[string]json.RawMessage{
		"thinking": json.RawMessage(`{"type":"disabled"}`),
	}
	executor, err := NewExecutor(context.Background(), providers)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()
	_, callErr := executor.Do(context.Background(), "mock-openai", "native-model", "chat", []byte(`{"model":"standard","thinking":{"type":"enabled"},"messages":[{"role":"user","content":"hello"}]}`))
	if callErr != nil {
		t.Fatal(callErr)
	}
}

func TestBifrostExecutorCustomProviderChatWithVersionedBasePath(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/functions/v1/gonka/chat/completions" {
			t.Errorf("unexpected path %s", request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(writer, `{"id":"chat-1","object":"chat.completion","created":1,"model":"native-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer upstream.Close()

	executor, err := NewExecutor(context.Background(), testProviders(upstream.URL+"/functions/v1/gonka"))
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()
	_, callErr := executor.Do(context.Background(), "mock-openai", "native-model", "chat", []byte(`{"model":"standard","messages":[{"role":"user","content":"hello"}]}`))
	if callErr != nil {
		t.Fatal(callErr)
	}
}

func TestBifrostExecutorStreamingMeaningfulChunk(t *testing.T) {
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

	executor, err := NewExecutor(context.Background(), testProviders(upstream.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()
	stream, callErr := executor.Stream(context.Background(), "mock-openai", "native-model", "chat", []byte(`{"model":"standard","stream":true,"messages":[{"role":"user","content":"hello"}]}`), 0)
	if callErr != nil {
		t.Fatal(callErr)
	}
	var meaningful []StreamChunk
	deadline := time.After(2 * time.Second)
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

func TestBifrostExecutorResponses(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/responses" {
			t.Errorf("unexpected path %s", request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["model"] != "native-model" {
			t.Errorf("logical Responses model reached upstream: %#v", body["model"])
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(writer, `{"id":"resp-1","object":"response","created_at":1,"completed_at":1,"status":"completed","model":"native-model","output":[],"error":null,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
	}))
	defer upstream.Close()

	executor, err := NewExecutor(context.Background(), testProviders(upstream.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()
	body, callErr := executor.Do(context.Background(), "mock-openai", "native-model", "responses", []byte(`{"model":"standard","input":"hello"}`))
	if callErr != nil {
		t.Fatal(callErr)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response["model"] != "native-model" || response["object"] != "response" {
		t.Fatalf("unexpected Responses payload: %s", body)
	}
}

func TestBifrostExecutorResponsesStreaming(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		flusher := writer.(http.Flusher)
		events := []struct {
			name string
			data string
		}{
			{
				name: "response.created",
				data: `{"type":"response.created","sequence_number":0,"response":{"id":"resp-1","object":"response","created_at":1,"status":"in_progress","model":"native-model","output":[],"error":null}}`,
			},
			{
				name: "response.output_text.delta",
				data: `{"type":"response.output_text.delta","sequence_number":1,"item_id":"msg-1","output_index":0,"content_index":0,"delta":"hello"}`,
			},
			{
				name: "response.completed",
				data: `{"type":"response.completed","sequence_number":2,"response":{"id":"resp-1","object":"response","created_at":1,"completed_at":1,"status":"completed","model":"native-model","output":[],"error":null,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
			},
		}
		for _, event := range events {
			_, _ = fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event.name, event.data)
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	executor, err := NewExecutor(context.Background(), testProviders(upstream.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()
	stream, callErr := executor.Stream(context.Background(), "mock-openai", "native-model", "responses", []byte(`{"model":"standard","stream":true,"input":"hello"}`), 0)
	if callErr != nil {
		t.Fatal(callErr)
	}
	var events []StreamChunk
	for event := range stream {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		events = append(events, event)
	}
	if len(events) != 3 {
		t.Fatalf("unexpected Responses stream: %#v", events)
	}
	if events[0].Meaningful || !events[1].Meaningful || events[1].Event != "response.output_text.delta" {
		t.Fatalf("meaningful Responses event selection mismatch: %#v", events)
	}
}

func TestBifrostErrorClassification(t *testing.T) {
	for status, expected := range map[int]string{
		429: "429",
		401: "5xx",
		402: "5xx",
		403: "5xx",
		404: "404",
		410: "404",
		503: "5xx",
		504: "timeout",
	} {
		status := status
		got := classifyBifrostError(context.Background(), &schemas.BifrostError{StatusCode: &status})
		if got.Class != expected {
			t.Fatalf("status %d classified as %s, want %s", status, got.Class, expected)
		}
	}
}

func TestSanitizeStreamChunkBackfillsReasoningFromDetails(t *testing.T) {
	thought := "step-by-step thought"
	chunk := &schemas.BifrostStreamChunk{
		BifrostChatResponse: &schemas.BifrostChatResponse{
			ID:     "chat-1",
			Model:  "gemini-4-argon",
			Object: "chat.completion.chunk",
			Choices: []schemas.BifrostResponseChoice{{
				Index: 0,
				ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
					Delta: &schemas.ChatStreamResponseChoiceDelta{
						ReasoningDetails: []schemas.ChatReasoningDetails{{
							Index: 0,
							Type:  schemas.BifrostReasoningDetailsTypeText,
							Text:  &thought,
						}},
					},
				},
			}},
		},
	}
	data, _, meaningful, err := sanitizeStreamChunk(chunk, "gemini-4-argon")
	if err != nil {
		t.Fatal(err)
	}
	if !meaningful {
		t.Fatalf("expected chunk with reasoning_details text to be meaningful: %s", string(data))
	}
	var decoded struct {
		Choices []struct {
			Delta struct {
				Reasoning        string `json:"reasoning"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Choices) != 1 || decoded.Choices[0].Delta.Reasoning != thought || decoded.Choices[0].Delta.ReasoningContent != thought {
		t.Fatalf("expected reasoning and reasoning_content to be backfilled to %q, got %+v", thought, decoded.Choices)
	}
}

func TestBifrostExecutorRejectsDuplicateStandardProvider(t *testing.T) {
	providers := []ProviderConfig{
		{ID: "vertex-a", BaseProvider: "vertex", InferenceURL: "https://aiplatform.googleapis.com", VertexProjectID: "a", VertexRegion: "global"},
		{ID: "vertex-b", BaseProvider: "vertex", InferenceURL: "https://aiplatform.googleapis.com", VertexProjectID: "b", VertexRegion: "global"},
	}
	if _, err := NewExecutor(context.Background(), providers); err == nil || !strings.Contains(err.Error(), "same Bifrost provider key") {
		t.Fatalf("expected duplicate standard provider error, got %v", err)
	}
}

