package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
)

// Part is either text, a remote/batch-file image URL, or inline image bytes.
type Part struct {
	Text, ImageURL, MIMEType string
	Data                     []byte
}
type ChatRequest struct {
	Model, System, User string
	Parts               []Part
	MaxTokens           int
	Temperature         *float64
}
type ChatJSONRequest struct {
	Model, System, User string
	Parts               []Part
	MaxTokens           int
	Temperature         *float64
	SchemaName          string
	StrictSchema        json.RawMessage
}
type Completion struct{ Content, FinishReason string }
type chatChoice struct {
	Message struct {
		Content *string `json:"content"`
	} `json:"message"`
	FinishReason string `json:"finish_reason"`
}
type chatResponse struct {
	Choices []chatChoice `json:"choices"`
}

func (r chatResponse) completion() (Completion, error) {
	if len(r.Choices) != 1 || r.Choices[0].Message.Content == nil {
		return Completion{}, errors.New("gateway returned no chat content")
	}
	c := r.Choices[0]
	return Completion{*c.Message.Content, c.FinishReason}, nil
}
func (r ChatRequest) body() (map[string]any, error) {
	var msgs []map[string]any
	if r.System != "" {
		msgs = append(msgs, map[string]any{"role": "system", "content": r.System})
	}
	var content any = r.User
	if len(r.Parts) > 0 {
		var parts []map[string]any
		if r.User != "" {
			parts = append(parts, map[string]any{"type": "text", "text": r.User})
		}
		for _, p := range r.Parts {
			switch {
			case p.Text != "" && p.ImageURL == "" && len(p.Data) == 0:
				parts = append(parts, map[string]any{"type": "text", "text": p.Text})
			case p.Text == "" && ((p.ImageURL != "") != (len(p.Data) > 0)):
				image := p.ImageURL
				if len(p.Data) > 0 {
					if p.MIMEType == "" {
						return nil, errors.New("image requires MIME type")
					}
					image = "data:" + p.MIMEType + ";base64," + base64.StdEncoding.EncodeToString(p.Data)
				}
				parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]string{"url": image}})
			default:
				return nil, errors.New("invalid chat part")
			}
		}
		content = parts
	}
	msgs = append(msgs, map[string]any{"role": "user", "content": content})
	body := map[string]any{"model": r.Model, "messages": msgs}
	if r.MaxTokens != 0 {
		body["max_tokens"] = r.MaxTokens
	}
	if r.Temperature != nil {
		body["temperature"] = *r.Temperature
	}
	return body, nil
}
func strictFormat(name string, schema json.RawMessage) (any, error) {
	if name == "" || !json.Valid(schema) {
		return nil, errors.New("schema name and JSON schema required")
	}
	return map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": name, "strict": true, "schema": schema}}, nil
}
func (c *Client) chat(ctx context.Context, body map[string]any) (Completion, error) {
	var response chatResponse
	if err := c.json(ctx, "POST", "/chat/completions", body, &response, nil); err != nil {
		return Completion{}, err
	}
	return response.completion()
}
func (c *Client) Chat(ctx context.Context, r ChatRequest) (Completion, error) {
	body, err := r.body()
	if err != nil {
		return Completion{}, err
	}
	return c.chat(ctx, body)
}
func (c *Client) ChatJSON(ctx context.Context, r ChatJSONRequest) (json.RawMessage, error) {
	req := ChatRequest{Model: r.Model, System: r.System, User: r.User, Parts: r.Parts, MaxTokens: r.MaxTokens, Temperature: r.Temperature}
	body, err := req.body()
	if err != nil {
		return nil, err
	}
	body["response_format"], err = strictFormat(r.SchemaName, r.StrictSchema)
	if err != nil {
		return nil, err
	}
	result, err := c.chat(ctx, body)
	if err != nil {
		return nil, err
	}
	data := json.RawMessage(result.Content)
	if !json.Valid(data) {
		return nil, errors.New("invalid JSON chat response")
	}
	return data, nil
}
