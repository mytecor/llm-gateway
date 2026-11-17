// Package embeddings is the provider-independent, non-streaming embedding contract.
package embeddings

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
)

const (
	MaxHTTPBytes     = 32 << 20
	MaxCommandBytes  = 40 << 20
	MaxResponseBytes = 2 << 20
	MaxItems         = 32
	MaxTokens        = 8192
)

type Blob struct {
	MIMEType string `json:"mimeType"`
	Data     string `json:"data"`
}
type Part struct {
	Text       *string `json:"text,omitempty"`
	InlineData *Blob   `json:"inlineData,omitempty"`
}
type Content struct {
	Parts []Part `json:"parts"`
	Role  string `json:"role,omitempty"`
}

// Protobuf JSON accepts original snake_case field names as well as lowerCamel.
// GenAI SDKs can emit snake_case inside Content even with camelCase request
// fields. Normalize only supported aliases; never ignore unknown media fields.
func (p *Part) UnmarshalJSON(data []byte) error {
	var wire struct {
		Text        *string `json:"text"`
		Inline      *Blob   `json:"inlineData"`
		SnakeInline *Blob   `json:"inline_data"`
	}
	if err := Decode(data, &wire); err != nil {
		return err
	}
	if wire.Inline != nil && wire.SnakeInline != nil {
		return errors.New("duplicate inlineData aliases")
	}
	if wire.Inline == nil {
		wire.Inline = wire.SnakeInline
	}
	*p = Part{Text: wire.Text, InlineData: wire.Inline}
	return nil
}

func (b *Blob) UnmarshalJSON(data []byte) error {
	var wire struct {
		MIME      string `json:"mimeType"`
		SnakeMIME string `json:"mime_type"`
		Data      string `json:"data"`
	}
	if err := Decode(data, &wire); err != nil {
		return err
	}
	if wire.MIME != "" && wire.SnakeMIME != "" {
		return errors.New("duplicate mimeType aliases")
	}
	if wire.MIME == "" {
		wire.MIME = wire.SnakeMIME
	}
	// ProtoJSON bytes allow both alphabets, with or without padding. The
	// canonical contract always uses padded standard base64, so runtimes need
	// only one representation and strict decoders agree on the actual bytes.
	encoding := base64.StdEncoding
	if strings.ContainsAny(wire.Data, "-_") {
		encoding = base64.URLEncoding
	}
	if !strings.Contains(wire.Data, "=") {
		encoding = encoding.WithPadding(base64.NoPadding)
	}
	decoded, err := encoding.Strict().DecodeString(wire.Data)
	if err != nil {
		return errors.New("invalid media base64")
	}
	*b = Blob{MIMEType: wire.MIME, Data: base64.StdEncoding.EncodeToString(decoded)}
	return nil
}

type Item struct {
	Parts      []Part `json:"parts,omitempty"`
	TokenIDs   []int  `json:"token_ids,omitempty"`
	TaskType   string `json:"task_type,omitempty"`
	Title      string `json:"title,omitempty"`
	Dimensions int    `json:"dimensions"`
}
type Request struct {
	Model string `json:"model"`
	Items []Item `json:"items"`
}
type Vector struct {
	Values []float32 `json:"values"`
	Tokens int64     `json:"tokens"`
}
type Response struct {
	Embeddings []Vector `json:"embeddings"`
}
type Features struct{ Images, Audio, Video bool }

func Decode(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return errors.New("invalid embedding request fields")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}

func ValidDimension(n int) bool { return n == 768 || n == 512 || n == 256 || n == 128 }

func (r *Request) Validate() (Features, error) {
	var f Features
	if strings.TrimSpace(r.Model) == "" || len(r.Items) == 0 || len(r.Items) > MaxItems {
		return f, errors.New("model and 1..32 inputs are required")
	}
	for idx := range r.Items {
		i := &r.Items[idx]
		if i.Dimensions == 0 {
			i.Dimensions = 768
		}
		if !ValidDimension(i.Dimensions) {
			return f, errors.New("dimensions must be 128, 256, 512 or 768")
		}
		if (len(i.Parts) == 0) == (len(i.TokenIDs) == 0) {
			return f, errors.New("each input requires parts or token IDs, exclusively")
		}
		if len(i.Parts) > 128 || len(i.TokenIDs) > MaxTokens {
			return f, errors.New("input exceeds the element limit")
		}
		for _, id := range i.TokenIDs {
			if id < 0 {
				return f, errors.New("token IDs must be nonnegative integers")
			}
		}
		media := false
		for _, p := range i.Parts {
			if (p.Text == nil) == (p.InlineData == nil) {
				return f, errors.New("each part requires exactly text or inlineData; URLs and file references are unsupported")
			}
			if p.Text != nil {
				if *p.Text == "" {
					return f, errors.New("text parts cannot be empty")
				}
				continue
			}
			b := p.InlineData
			if b.Data == "" || base64.StdEncoding.DecodedLen(len(b.Data)) > MaxHTTPBytes {
				return f, errors.New("invalid media size")
			}
			if _, err := base64.StdEncoding.Strict().DecodeString(b.Data); err != nil {
				return f, errors.New("invalid media base64")
			}
			switch b.MIMEType {
			case "image/jpeg", "image/png", "image/webp":
				f.Images = true
			case "audio/wav", "audio/x-wav", "audio/mpeg", "audio/mp3", "audio/flac", "audio/ogg":
				f.Audio = true
			case "video/mp4", "video/webm":
				f.Video = true
			default:
				return f, errors.New("unsupported inline media MIME type")
			}
			media = true
		}
		switch i.TaskType {
		case "", "TASK_TYPE_UNSPECIFIED":
			i.TaskType = ""
		case "RETRIEVAL_QUERY", "RETRIEVAL_DOCUMENT", "SEMANTIC_SIMILARITY", "CLASSIFICATION", "CLUSTERING", "QUESTION_ANSWERING", "FACT_VERIFICATION", "CODE_RETRIEVAL_QUERY":
		default:
			return f, errors.New("unsupported task type")
		}
		if media && (i.TaskType != "" || i.Title != "") {
			return f, errors.New("task instructions and titles are supported only for text inputs")
		}
		if i.Title != "" && i.TaskType != "RETRIEVAL_DOCUMENT" {
			return f, errors.New("title requires RETRIEVAL_DOCUMENT")
		}
	}
	return f, nil
}

func ParseOpenAI(data []byte) (Request, string, error) {
	var wire struct {
		Model      string          `json:"model"`
		Input      json.RawMessage `json:"input"`
		Encoding   string          `json:"encoding_format"`
		Dimensions int             `json:"dimensions"`
		Task       string          `json:"task_type"`
		Title      string          `json:"title"`
		User       string          `json:"user"`
	}
	var r Request
	if err := Decode(data, &wire); err != nil {
		return r, "", err
	}
	if wire.Encoding == "" {
		wire.Encoding = "float"
	}
	if wire.Encoding != "float" && wire.Encoding != "base64" {
		return r, "", errors.New("encoding_format must be float or base64")
	}
	r.Model = wire.Model
	makeItem := func(raw json.RawMessage) (Item, error) {
		i := Item{Dimensions: wire.Dimensions, TaskType: wire.Task, Title: wire.Title}
		s := bytes.TrimSpace(raw)
		if len(s) == 0 {
			return i, errors.New("input is required")
		}
		switch s[0] {
		case '"':
			var t string
			if json.Unmarshal(s, &t) != nil {
				return i, errors.New("invalid text")
			}
			i.Parts = []Part{{Text: &t}}
		case '{':
			var c Content
			if err := Decode(s, &c); err != nil {
				return i, err
			}
			if c.Role != "" && c.Role != "user" {
				return i, errors.New("unsupported content role")
			}
			i.Parts = c.Parts
		case '[':
			if err := json.Unmarshal(s, &i.TokenIDs); err != nil {
				return i, errors.New("invalid token IDs")
			}
		default:
			return i, errors.New("unsupported input shape")
		}
		return i, nil
	}
	s := bytes.TrimSpace(wire.Input)
	if len(s) == 0 {
		return r, "", errors.New("input is required")
	}
	var entries []json.RawMessage
	if s[0] == '[' {
		if json.Unmarshal(s, &entries) != nil || len(entries) == 0 {
			return r, "", errors.New("input must not be empty")
		}
		first := bytes.TrimSpace(entries[0])
		if len(first) > 0 && first[0] >= '0' && first[0] <= '9' {
			entries = []json.RawMessage{s}
		} else {
			for _, e := range entries {
				e = bytes.TrimSpace(e)
				if len(e) == 0 || e[0] != first[0] {
					return r, "", errors.New("mixed input shapes are unsupported")
				}
			}
		}
	} else {
		entries = []json.RawMessage{s}
	}
	if len(entries) > MaxItems {
		return r, "", errors.New("too many inputs")
	}
	for _, e := range entries {
		i, err := makeItem(e)
		if err != nil {
			return r, "", err
		}
		r.Items = append(r.Items, i)
	}
	_, err := r.Validate()
	return r, wire.Encoding, err
}

type GoogleRequest struct {
	Model      string  `json:"model,omitempty"`
	Content    Content `json:"content"`
	Task       string  `json:"taskType,omitempty"`
	Title      string  `json:"title,omitempty"`
	Dimensions int     `json:"outputDimensionality,omitempty"`
}

func ParseGoogle(data []byte, logical string, batch bool) (Request, error) {
	r := Request{Model: logical}
	var requests []GoogleRequest
	if batch {
		var b struct {
			Requests []GoogleRequest `json:"requests"`
		}
		if err := Decode(data, &b); err != nil {
			return r, err
		}
		requests = b.Requests
	} else {
		var g GoogleRequest
		if err := Decode(data, &g); err != nil {
			return r, err
		}
		requests = []GoogleRequest{g}
	}
	for _, g := range requests {
		if g.Model != "" && strings.TrimPrefix(g.Model, "models/") != logical {
			return r, errors.New("body model must match the path model")
		}
		if g.Content.Role != "" && g.Content.Role != "user" {
			return r, errors.New("unsupported content role")
		}
		r.Items = append(r.Items, Item{Parts: g.Content.Parts, TaskType: g.Task, Title: g.Title, Dimensions: g.Dimensions})
	}
	_, err := r.Validate()
	return r, err
}

func (r Request) ValidateResponse(data []byte) (Response, error) {
	var response Response
	if len(data) > MaxResponseBytes || Decode(data, &response) != nil || len(response.Embeddings) != len(r.Items) {
		return response, errors.New("invalid embedding response count")
	}
	for idx, v := range response.Embeddings {
		if len(v.Values) != r.Items[idx].Dimensions || v.Tokens < 0 {
			return response, errors.New("invalid embedding dimensions or usage")
		}
		norm := 0.0
		for _, n := range v.Values {
			if math.IsNaN(float64(n)) || math.IsInf(float64(n), 0) {
				return response, errors.New("embedding contains non-finite values")
			}
			norm += float64(n) * float64(n)
		}
		if math.Abs(norm-1) > 0.002 {
			return response, errors.New("embedding must be L2-normalized")
		}
	}
	return response, nil
}

func (r Response) OpenAI(logical, encoding string) any {
	data := make([]any, len(r.Embeddings))
	var tokens int64
	for idx, v := range r.Embeddings {
		var value any = v.Values
		if encoding == "base64" {
			b := make([]byte, 4*len(v.Values))
			for j, n := range v.Values {
				binary.LittleEndian.PutUint32(b[j*4:], math.Float32bits(n))
			}
			value = base64.StdEncoding.EncodeToString(b)
		}
		data[idx] = map[string]any{"object": "embedding", "index": idx, "embedding": value}
		tokens += v.Tokens
	}
	return map[string]any{"object": "list", "model": logical, "data": data, "usage": map[string]int64{"prompt_tokens": tokens, "total_tokens": tokens}}
}
func (r Response) Google(batch bool) any {
	data := make([]any, len(r.Embeddings))
	var tokens int64
	for idx, v := range r.Embeddings {
		data[idx] = map[string]any{"values": v.Values}
		tokens += v.Tokens
	}
	if batch {
		return map[string]any{"embeddings": data}
	}
	return map[string]any{"embedding": data[0], "usageMetadata": map[string]int64{"promptTokenCount": tokens, "totalTokenCount": tokens}}
}

func Compatible(f Features, images, audio, video bool) bool {
	return (!f.Images || images) && (!f.Audio || audio) && (!f.Video || video)
}
