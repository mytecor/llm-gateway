package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/gif"
	"image/png"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
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

func TestTruncateRetriesWithPrefixAndEOS(t *testing.T) {
	embeds, tokenizations := 0, 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tokenize":
			var body struct{ Content string }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Content != "title: T | text: long" {
				t.Error("tokenizer lost title or text", err)
			}
			tokenizations++
			ids := make([]int, embeddings.MaxTokens+10)
			for i := range ids {
				ids[i] = i + 10
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tokens": ids})
		case "/v1/embeddings":
			embeds++
			if embeds == 1 {
				w.WriteHeader(400)
				_, _ = w.Write([]byte(`{"error":{"code":"context_length_exceeded"}}`))
				return
			}
			var body struct{ Input [][]int }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Input) != 1 {
				t.Error("invalid token request", err)
				return
			}
			ids := body.Input[0]
			if len(ids) != embeddings.MaxTokens || ids[0] != 2 || ids[1] != 10 || ids[len(ids)-2] != embeddings.MaxTokens+7 || ids[len(ids)-1] != 1 {
				t.Error("incorrect truncated sequence")
			}
			v := make([]float32, 768)
			v[0] = 1
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"index": 0, "embedding": v}}, "usage": map[string]int{"prompt_tokens": len(ids)}})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
		}
	}))
	defer api.Close()
	executor := openAITestExecutor(t, api.URL)
	_, failure := executor.do(t.Context(), command{Kind: "embeddings", Model: "m", Body: json.RawMessage(`{"model":"m","items":[{"parts":[{"text":"long"}],"task_type":"RETRIEVAL_DOCUMENT","title":"T","truncate":true}]}`)})
	if failure != nil || embeds != 2 || tokenizations != 1 {
		t.Fatalf("retry failed: %+v calls %d/%d", failure, embeds, tokenizations)
	}
}

func TestGIFFirstFrameSentAsPNG(t *testing.T) {
	palette := color.Palette{color.RGBA{255, 0, 0, 255}, color.RGBA{0, 0, 255, 255}}
	first := image.NewPaletted(image.Rect(0, 0, 4, 4), palette)
	second := image.NewPaletted(first.Rect, palette)
	for i := range second.Pix {
		second.Pix[i] = 1
	}
	var raw bytes.Buffer
	if err := gif.EncodeAll(&raw, &gif.GIF{Image: []*image.Paletted{first, second}, Delay: []int{0, 0}}); err != nil {
		t.Fatal(err)
	}
	item := embeddings.Item{Parts: []embeddings.Part{{InlineData: &embeddings.Blob{MIMEType: "image/gif", Data: base64.StdEncoding.EncodeToString(raw.Bytes())}}}}
	for _, align := range []string{"", imageAlignEmbeddingGemma2} {
		e := executor{config: config{ImageAlign: align}}
		input, err := e.openAIInput(t.Context(), "native", item)
		if err != nil {
			t.Fatal(err)
		}
		parts := input.(map[string]any)["content"].([]openAIContent)
		data, ok := strings.CutPrefix(parts[0].Image["url"], "data:image/png;base64,")
		if !ok {
			t.Fatal("expected PNG")
		}
		decoded, err2 := png.Decode(base64.NewDecoder(base64.StdEncoding, strings.NewReader(data)))
		if err2 != nil {
			t.Fatal(err2)
		}
		r, _, b, _ := decoded.At(0, 0).RGBA()
		if r != 65535 || b != 0 {
			t.Fatal("expected red first frame")
		}
	}
}
