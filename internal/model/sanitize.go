package model

import (
	"bytes"
	"encoding/json"
	"strings"
)

func SanitizeJSON(data []byte, model string) ([]byte, error) {
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	delete(object, "extra_fields")
	delete(object, "provider_extra_fields")
	if model != "" {
		if _, exists := object["model"]; exists {
			object["model"] = model
		}
		if response, ok := object["response"].(map[string]any); ok {
			response["model"] = model
			delete(response, "extra_fields")
			delete(response, "provider_extra_fields")
		}
	}
	return json.Marshal(object)
}

// MeaningfulPayload reports whether an SSE data payload carries substantive model output.
func MeaningfulPayload(data []byte, event string) bool {
	if event != "" {
		switch event {
		case "response.output_text.delta", "response.reasoning_summary_text.delta",
			"response.function_call_arguments.delta", "response.custom_tool_call_input.delta":
			var object map[string]any
			if json.Unmarshal(data, &object) == nil {
				if delta, ok := object["delta"].(string); ok {
					return delta != ""
				}
			}
		}
		return false
	}
	var object struct {
		Choices []struct {
			Delta map[string]json.RawMessage `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal(data, &object) != nil {
		return false
	}
	for _, choice := range object.Choices {
		for _, key := range []string{"content", "reasoning", "reasoning_content", "tool_calls"} {
			raw := bytes.TrimSpace(choice.Delta[key])
			if key != "tool_calls" {
				var value string
				if json.Unmarshal(raw, &value) == nil && strings.TrimSpace(strings.ReplaceAll(value, "\u200b", "")) == "" {
					continue
				}
			}
			if len(raw) > 0 && !bytes.Equal(raw, []byte("null")) && !bytes.Equal(raw, []byte(`""`)) && !bytes.Equal(raw, []byte("[]")) {
				return true
			}
		}
	}
	return false
}

