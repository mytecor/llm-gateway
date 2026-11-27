package client

import (
	"context"
	"encoding/base64"
	"errors"

	"github.com/mytecor/llm-gateway/driverproto/embeddings"
)

type EmbeddingInput struct {
	Text  string
	Parts []Part
}
type EmbedRequest struct {
	Model, Text, TaskType, Title string
	Parts                        []Part
	Truncate                     bool
	Dimensions                   int
}
type EmbedBatchRequest struct {
	Model, TaskType, Title string
	Inputs                 []EmbeddingInput
	Truncate               bool
	Dimensions             int
}

func (c *Client) Embed(ctx context.Context, r EmbedRequest) ([]float32, error) {
	vectors, err := c.EmbedBatch(ctx, EmbedBatchRequest{Model: r.Model, TaskType: r.TaskType, Title: r.Title, Inputs: []EmbeddingInput{{Text: r.Text, Parts: r.Parts}}, Truncate: r.Truncate, Dimensions: r.Dimensions})
	if err != nil {
		return nil, err
	}
	return vectors[0], nil
}
func (c *Client) EmbedBatch(ctx context.Context, r EmbedBatchRequest) ([][]float32, error) {
	var inputs []embeddings.Content
	for _, item := range r.Inputs {
		var parts []embeddings.Part
		if item.Text != "" {
			parts = append(parts, embeddings.Part{Text: &item.Text})
		}
		for _, p := range item.Parts {
			switch {
			case p.Text != "" && len(p.Data) == 0 && p.ImageURL == "":
				parts = append(parts, embeddings.Part{Text: &p.Text})
			case len(p.Data) > 0 && p.Text == "" && p.ImageURL == "":
				parts = append(parts, embeddings.Part{InlineData: &embeddings.Blob{MIMEType: p.MIMEType, Data: base64.StdEncoding.EncodeToString(p.Data)}})
			default:
				return nil, errors.New("invalid embedding part")
			}
		}
		inputs = append(inputs, embeddings.Content{Parts: parts})
	}
	if len(inputs) == 0 || len(inputs) > embeddings.MaxItems {
		return nil, errors.New("expected 1 to 64 embedding inputs")
	}
	body := map[string]any{"input": inputs, "model": r.Model, "truncate": r.Truncate, "task_type": r.TaskType, "title": r.Title}
	if r.Dimensions != 0 {
		body["dimensions"] = r.Dimensions
	}
	var resp struct {
		Data []struct {
			Index     *int
			Embedding []float32
		}
	}
	if err := c.json(ctx, "POST", "/embeddings", body, &resp, nil); err != nil {
		return nil, err
	}
	vectors := make([][]float32, len(inputs))
	dims := r.Dimensions
	if dims == 0 {
		dims = 768
	}
	if len(resp.Data) != len(inputs) {
		return nil, errors.New("embedding count mismatch")
	}
	for _, row := range resp.Data {
		if row.Index == nil || *row.Index < 0 || *row.Index >= len(inputs) || vectors[*row.Index] != nil || len(row.Embedding) != dims {
			return nil, errors.New("invalid embedding index or dimension")
		}
		vectors[*row.Index] = row.Embedding
	}
	return vectors, nil
}
