package policy

import (
	"sort"
	"strings"

	"github.com/mytecor/llm-gateway/internal/model"
)

type Duration = model.Duration

type BackoffConfig struct {
	Type    string   `json:"type"`
	Initial Duration `json:"initial"`
	Max     Duration `json:"max"`
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// trimNonEmpty returns the input with each element TrimSpace'd and blanks
// removed. Used for lists that are config-optional in meaning (empty == not
// set) without confusing the executor with empty/whitespace entries.
func trimNonEmpty(values []string) []string {
	var out []string
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func parseErrorClasses(values []string) map[model.ErrorClass]bool {
	if len(values) == 0 {
		return allRetryableClasses()
	}
	classes := make(map[model.ErrorClass]bool, len(values))
	for _, value := range values {
		classes[model.ErrorClass(strings.ToLower(strings.TrimSpace(value)))] = true
	}
	return classes
}

func allRetryableClasses() map[model.ErrorClass]bool {
	return AllRetryableClasses()
}

func AllRetryableClasses() map[model.ErrorClass]bool {
	return map[model.ErrorClass]bool{
		model.ErrorTimeout: true, model.ErrorConnection: true, model.ErrorRateLimit: true,
		model.ErrorNotFound: true, model.ErrorUpstream: true, model.ErrorInvalid: true,
	}
}
