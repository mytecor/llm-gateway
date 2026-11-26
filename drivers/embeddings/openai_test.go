package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/mytecor/llm-gateway/driverproto/embeddings"
)

func TestOpenAINativeBatch(t *testing.T) {
	const model = "google/embeddinggemma-2"
	expected := []any{
		"title: Listing | text: A large balcony",
		map[string]any{"content": []any{
			map[string]any{"type": "text", "text": "Balcony"},
			map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/jpeg;base64,YQ=="}},
			map[string]any{"type": "text", "text": "at sunset"},
		}},
		[]any{float64(2), float64(7), float64(8), float64(10)},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			_, _ = w.Write([]byte(`{"data":[{"id":"google/embeddinggemma-2"}]}`))
		case "/tokenize":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil ||
				body["model"] != model || body["content"] != "task: search result | query: " ||
				body["add_special"] != false || body["parse_special"] != false {
				t.Errorf("unexpected tokenization request: %v", body)
			}
			_, _ = w.Write([]byte(`{"tokens":[7,8]}`))
		case "/v1/embeddings":
			var body struct {
				Model    string `json:"model"`
				Input    []any  `json:"input"`
				Encoding string `json:"encoding_format"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil ||
				body.Model != model || body.Encoding != "float" || !reflect.DeepEqual(body.Input, expected) {
				t.Errorf("unexpected llama.cpp request: %+v", body)
			}
			rows := make([]map[string]any, 0, len(expected))
			for i := len(expected) - 1; i >= 0; i-- {
				values := make([]float32, 768)
				values[i], values[700] = 3, 4
				rows = append(rows, map[string]any{"index": i, "embedding": values})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": rows, "usage": map[string]int{"prompt_tokens": 24},
			})
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	exec := openAITestExecutor(t, server.URL)
	models, err := exec.models(context.Background(), "")
	if err != nil || len(models) != 1 || models[0].ID != model || !reflect.DeepEqual(models[0].EmbeddingDimensions, []int{128, 256, 512, 768}) {
		t.Fatalf("unexpected model catalog: %v, %v", models, err)
	}
	body := json.RawMessage(`{"model":"logical","items":[
		{"dimensions":128,"task_type":"RETRIEVAL_DOCUMENT","title":"Listing","parts":[{"text":"A large balcony"}]},
		{"dimensions":256,"parts":[{"text":"Balcony"},{"inlineData":{"mimeType":"image/jpeg","data":"YQ=="}},{"text":"at sunset"}]},
		{"dimensions":768,"task_type":"RETRIEVAL_QUERY","token_ids":[2,10]}]}`)
	result, failure := exec.do(context.Background(), command{Kind: "embeddings", Model: model, Body: body})
	if failure != nil {
		t.Fatal(failure)
	}
	var request embeddings.Request
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	response, err := request.ValidateResponse(result)
	if err != nil {
		t.Fatal(err)
	}
	rows := response.Embeddings
	if rows[0].Values[0] != 1 || rows[1].Values[1] != 1 || rows[2].Values[2] != 0.6 ||
		rows[0].Tokens+rows[1].Tokens+rows[2].Tokens != 24 {
		t.Fatalf("wrong restored order, normalization, or usage")
	}
}

func TestLlamaContextError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"error":{"code":500,"type":"server_error","message":"input (8502 tokens) is too large to process. increase the physical batch size (current batch size: 8192)"}}`))
	}))
	defer server.Close()
	exec := openAITestExecutor(t, server.URL)
	body := json.RawMessage(`{"model":"logical","items":[{"parts":[{"text":"too long"}],"dimensions":768}]}`)
	_, failure := exec.do(context.Background(), command{Kind: "embeddings", Model: "native", Body: body})
	if failure == nil || failure.Status != 400 || failure.Code != "context_length_exceeded" || failure.Scope != "request" {
		t.Fatalf("unexpected failure: %+v", failure)
	}
}

func openAITestExecutor(t *testing.T, url string) *executor {
	t.Helper()
	cfg, err := json.Marshal(config{InferenceURL: url})
	if err != nil {
		t.Fatal(err)
	}
	exec, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return exec
}
