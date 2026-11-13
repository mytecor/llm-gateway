package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExplainConfigResolvesDeclarativeModelsAndTiers(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.json")
	cfgJSON := `{
		"providers": [
			{"id": "p-low", "priority": 10, "labels": {"pool": "gonka"}, "base_provider": "openai", "inference_url": "https://low.invalid"},
			{"id": "p-high", "priority": 30, "labels": {"pool": "gonka"}, "base_provider": "openai", "inference_url": "https://high.invalid"},
			{"id": "p-backup", "priority": 5, "base_provider": "openai", "inference_url": "https://backup.invalid"}
		],
		"models": {
			"standard": {
				"profile": "low-latency",
				"tiers": [
					{"targets": [{"select": {"pool": "gonka"}, "native": "ds-v4"}]},
					{"targets": [{"provider": "p-backup", "native": "gonka/ds-v4"}]}
				]
			}
		}
	}`
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	exp, err := ExplainConfig(cfgPath, false, "")
	if err != nil {
		t.Fatalf("ExplainConfig error: %v", err)
	}
	if len(exp.Providers) != 3 || len(exp.Models) != 1 {
		t.Fatalf("unexpected explanation shape: %+v", exp)
	}
	m := exp.Models[0]
	if m.Model != "standard" || m.Profile != "low-latency" || !m.AdaptiveTiming || m.SafeContinue {
		t.Fatalf("unexpected model summary: %+v", m)
	}
	if !m.Admission.Dynamic || m.Balance.Strategy != "expected-ttft" {
		t.Fatalf("unexpected admission/balance summary: %+v", m)
	}
	if len(m.Tiers) != 2 || len(m.Tiers[0].Targets) != 2 {
		t.Fatalf("unexpected tier expansion: %+v", m.Tiers)
	}
	if m.Tiers[0].Targets[0].Provider != "p-high" || m.Tiers[0].Targets[1].Provider != "p-low" {
		t.Fatalf("expected p-high before p-low by priority, got %+v", m.Tiers[0].Targets)
	}
	if !m.Tiers[1].ReschedulableOnly || m.Tiers[1].Targets[0].Provider != "p-backup" {
		t.Fatalf("unexpected Tier 1 summary: %+v", m.Tiers[1])
	}
}
