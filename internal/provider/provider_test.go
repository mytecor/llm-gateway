package provider

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProviderLegacySettingsNormalizeToCanonicalJSON(t *testing.T) {
	var p Provider
	if err := json.Unmarshal([]byte(`{"id":"p","maxConcurrent":3,"config":null,"inference_url":"https://example.invalid/v1"}`), &p); err != nil {
		t.Fatal(err)
	}
	if p.MaxConcurrent != 3 || p.Config["inference_url"] != "https://example.invalid/v1" {
		t.Fatalf("bad normalized provider: %+v", p)
	}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "maxConcurrent") || !strings.Contains(string(data), `"max_concurrent":3`) {
		t.Fatalf("noncanonical encoding: %s", data)
	}
}

func TestProviderRejectsConflictingAliases(t *testing.T) {
	var p Provider
	err := json.Unmarshal([]byte(`{"id":"p","max_concurrent":2,"maxConcurrent":3}`), &p)
	if err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatal("ambiguous provider accepted", err)
	}
}
