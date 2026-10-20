package model

import (
	"encoding/json"
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
