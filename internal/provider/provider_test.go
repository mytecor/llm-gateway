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

func TestProviderEnvelopeRejectsUnknownFields(t *testing.T) {
	for _, raw := range []string{
		`{"id":"p","request_timout":"1s"}`,
		`{"id":"p","custom_driver_option":true}`,
		`{"id":"p","session":{"turn_timout":"1m"}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var p Provider
			if err := json.Unmarshal([]byte(raw), &p); err == nil {
				t.Fatal("accepted an unknown provider envelope field")
			}
		})
	}
	var p Provider
	if err := json.Unmarshal([]byte(`{"id":"p","config":{"custom_driver_option":true}}`), &p); err != nil {
		t.Fatalf("opaque driver option was rejected: %v", err)
	}
}

func TestProviderLegacySettingsRejectConflictsWithConfig(t *testing.T) {
	var p Provider
	err := json.Unmarshal([]byte(`{"id":"p","inference_url":"https://old.invalid","config":{"inference_url":"https://new.invalid"}}`), &p)
	if err == nil || !strings.Contains(err.Error(), "config.inference_url") {
		t.Fatalf("expected a positioned conflict error, got %v", err)
	}
	if err := json.Unmarshal([]byte(`{"id":"p","headers":{"b":"2","a":"1"},"config":{"headers":{"a":"1","b":"2"}}}`), &p); err != nil {
		t.Fatalf("equal legacy and canonical settings were rejected: %v", err)
	}
}
