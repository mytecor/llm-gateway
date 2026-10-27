package model

import "encoding/json"

// StreamDelta is the semantic view of one upstream chunk. The original bytes
// remain on StreamEvent so unrecognized provider fields can still be relayed.
type StreamDelta struct {
	ID      string `json:"id"`
	Choices []struct {
		Delta        ChatDelta `json:"delta"`
		FinishReason string    `json:"finish_reason"`
	} `json:"choices"`
	Usage    *StreamUsage `json:"usage"`
	Response *struct {
		Usage *StreamUsage `json:"usage"`
	} `json:"response"`
}

type ChatDelta struct {
	Content          string           `json:"content"`
	Reasoning        string           `json:"reasoning"`
	ReasoningContent string           `json:"reasoning_content"`
	ToolCalls        []StreamToolCall `json:"tool_calls"`
}

type StreamToolCall struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type StreamUsage struct {
	PromptTokens     json.Number `json:"prompt_tokens"`
	CompletionTokens json.Number `json:"completion_tokens"`
	InputTokens      json.Number `json:"input_tokens"`
	OutputTokens     json.Number `json:"output_tokens"`
	CachedTokens     json.Number `json:"cached_tokens"`
}

func DecodeStreamDelta(data []byte) (StreamDelta, error) {
	var delta StreamDelta
	err := json.Unmarshal(data, &delta)
	return delta, err
}

func (d StreamDelta) Finished() bool {
	for _, choice := range d.Choices {
		if choice.FinishReason != "" {
			return true
		}
	}
	return false
}

func (d StreamDelta) Text() string {
	if len(d.Choices) == 0 {
		return ""
	}
	delta := d.Choices[0].Delta
	if delta.Reasoning != "" {
		return delta.Content + delta.Reasoning
	}
	return delta.Content + delta.ReasoningContent
}

func (d StreamDelta) UsageSummary() (UsageSummary, bool) {
	u := d.Usage
	if u == nil && d.Response != nil {
		u = d.Response.Usage
	}
	if u == nil {
		return UsageSummary{}, false
	}
	number := func(n json.Number) int64 { v, _ := n.Int64(); return v }
	return UsageSummary{
		Input:        number(u.PromptTokens) + number(u.InputTokens),
		Output:       number(u.CompletionTokens) + number(u.OutputTokens),
		CachedTokens: number(u.CachedTokens),
	}, true
}
