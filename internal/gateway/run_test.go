package gateway

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunUsageErrors(t *testing.T) {
	if err := Run([]string{}); err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Fatalf("expected usage error, got %v", err)
	}

	if err := Run([]string{"--config", "some.json", "invalid-cmd"}); err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Fatalf("expected usage error on invalid command, got %v", err)
	}
}

func TestRunCheckValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.json")
	cfgJSON := `{
		"host": "127.0.0.1",
		"port": 9208,
		"providers": [
			{"id": "a", "base_provider": "openai", "inference_url": "https://a.invalid"}
		],
		"routing_rules": [
			{"route": "standard", "action": "filter", "where": {"model": {"eq": "standard"}}},
			{"route": "standard", "action": "filter", "where": {"provider": {"in": ["a"]}}},
			{"route": "standard", "action": "map", "native": "native-model"},
			{"route": "standard", "action": "rank", "strategy": "priority"},
			{"route": "standard", "action": "race", "count": 1}
		]
	}`
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := Run([]string{"--config", cfgPath, "check"}); err != nil {
		t.Fatalf("expected check to succeed, got %v", err)
	}
}
