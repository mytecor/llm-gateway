package main

import (
	"context"
	"encoding/json"
	"math"
	"strings"

	"github.com/mytecor/llm-gateway/driverproto"
	"github.com/mytecor/llm-gateway/driverproto/embeddings"
)

var taskPrefixes = map[string]string{
	"RETRIEVAL_QUERY":      "task: search result | query: ",
	"SEMANTIC_SIMILARITY":  "task: sentence similarity | query: ",
	"CLASSIFICATION":       "task: classification | query: ",
	"CLUSTERING":           "task: clustering | query: ",
	"QUESTION_ANSWERING":   "task: question answering | query: ",
	"FACT_VERIFICATION":    "task: fact checking | query: ",
	"CODE_RETRIEVAL_QUERY": "task: code retrieval | query: ",
}

type openAIContent struct {
	Type  string            `json:"type"`
	Text  string            `json:"text,omitempty"`
	Image map[string]string `json:"image_url,omitempty"`
	Audio map[string]string `json:"input_audio,omitempty"`
	Video map[string]string `json:"input_video,omitempty"`
}

// The OpenAI response exposes aggregate usage, so record it once in the internal
// response. The gateway sums these counters for both billing and public usage.
func (e *executor) openAIEmbeddings(ctx context.Context, request embeddings.Request) (json.RawMessage, *driverproto.CallError) {
	inputs := make([]any, 0, len(request.Items))
	for _, item := range request.Items {
		input, failure := e.openAIInput(ctx, request.Model, item)
		if failure != nil {
			return nil, failure
		}
		inputs = append(inputs, input)
	}
	body, failure := e.post(ctx, "/v1/embeddings", map[string]any{
		"model": request.Model, "input": inputs, "encoding_format": "float",
	})
	if failure != nil {
		return nil, failure
	}
	var native struct {
		Data []struct {
			Index  *int      `json:"index"`
			Values []float32 `json:"embedding"`
		} `json:"data"`
		Usage struct {
			PromptTokens *int64 `json:"prompt_tokens"`
		} `json:"usage"`
	}
	invalid := fail(502, "invalid_embedding_response")
	if json.Unmarshal(body, &native) != nil || len(native.Data) != len(inputs) ||
		native.Usage.PromptTokens == nil || *native.Usage.PromptTokens <= 0 ||
		*native.Usage.PromptTokens > int64(len(inputs)*embeddings.MaxTokens) {
		return nil, invalid
	}
	response := embeddings.Response{Embeddings: make([]embeddings.Vector, len(inputs))}
	for _, row := range native.Data {
		if row.Index == nil || *row.Index < 0 || *row.Index >= len(inputs) || len(row.Values) != 768 {
			return nil, invalid
		}
		index := *row.Index
		if response.Embeddings[index].Values != nil {
			return nil, invalid
		}
		values := row.Values[:request.Items[index].Dimensions]
		var squaredNorm float64
		for _, value := range values {
			squaredNorm += float64(value) * float64(value)
		}
		norm := math.Sqrt(squaredNorm)
		if norm == 0 || math.IsInf(norm, 0) || math.IsNaN(norm) {
			return nil, invalid
		}
		for i, value := range values {
			values[i] = float32(float64(value) / norm)
		}
		response.Embeddings[index].Values = values
	}
	response.Embeddings[0].Tokens = *native.Usage.PromptTokens
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, invalid
	}
	return encoded, nil
}

func (e *executor) openAIInput(ctx context.Context, model string, item embeddings.Item) (any, *driverproto.CallError) {
	prefix := taskPrefixes[item.TaskType]
	if item.TaskType == "RETRIEVAL_DOCUMENT" {
		title := item.Title
		if title == "" {
			title = "none"
		}
		prefix = "title: " + title + " | text: "
	}
	if len(item.TokenIDs) != 0 {
		return e.openAITokens(ctx, model, item.TokenIDs, prefix)
	}
	var text strings.Builder
	text.WriteString(prefix)
	parts := make([]openAIContent, 0, len(item.Parts))
	hasMedia := false
	for _, part := range item.Parts {
		if part.Text != nil {
			text.WriteString(*part.Text)
			parts = append(parts, openAIContent{Type: "text", Text: *part.Text})
			continue
		}
		hasMedia = true
		blob := part.InlineData
		switch {
		case strings.HasPrefix(blob.MIMEType, "image/"):
			mimeType, data := blob.MIMEType, blob.Data
			if e.config.ImageAlign == imageAlignEmbeddingGemma2 {
				aligned, err := alignEmbeddingGemma2Image(blob.Data, blob.MIMEType)
				if err != nil {
					return nil, fail(400, "unsupported_media")
				}
				mimeType, data = "image/png", aligned
			}
			parts = append(parts, openAIContent{Type: "image_url", Image: map[string]string{"url": "data:" + mimeType + ";base64," + data}})
		case strings.HasPrefix(blob.MIMEType, "audio/"):
			parts = append(parts, openAIContent{Type: "input_audio", Audio: map[string]string{"data": blob.Data}})
		case strings.HasPrefix(blob.MIMEType, "video/"):
			parts = append(parts, openAIContent{Type: "input_video", Video: map[string]string{"data": blob.Data}})
		}
	}
	// Literal placeholders can change which tensors the server inserts.
	for _, marker := range []string{"<__media__>", "<|image|>", "<|audio|>", "<|video|>"} {
		if strings.Contains(text.String(), marker) {
			return nil, fail(400, "invalid_embedding_request")
		}
	}
	if hasMedia {
		return map[string]any{"content": parts}, nil
	}
	return text.String(), nil
}

func (e *executor) openAITokens(ctx context.Context, model string, ids []int, prefix string) (any, *driverproto.CallError) {
	// EmbeddingGemma 2 vocabulary and BOS ID, shared by its HF and GGUF exports.
	const vocabSize, bos = 262144, 2
	for _, id := range ids {
		if id >= vocabSize {
			return nil, fail(400, "invalid_embedding_request")
		}
	}
	if prefix == "" {
		return ids, nil
	}
	body, failure := e.post(ctx, "/tokenize", map[string]any{
		"model": model, "content": prefix, "add_special": false, "parse_special": false,
	})
	if failure != nil {
		return nil, failure
	}
	var tokenized struct {
		Tokens []int `json:"tokens"`
	}
	if json.Unmarshal(body, &tokenized) != nil || len(tokenized.Tokens) == 0 {
		return nil, fail(502, "invalid_embedding_response")
	}
	if len(ids)+len(tokenized.Tokens) > embeddings.MaxTokens {
		return nil, fail(400, "context_length_exceeded")
	}
	offset := 0
	if ids[0] == bos {
		offset = 1
	}
	tokens := make([]int, 0, len(ids)+len(tokenized.Tokens))
	tokens = append(tokens, ids[:offset]...)
	tokens = append(tokens, tokenized.Tokens...)
	return append(tokens, ids[offset:]...), nil
}
