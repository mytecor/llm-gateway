package config

import (
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
)

func TestDeclarativeConfigPropagatesRouteTimeoutCeilingAcrossTiers(t *testing.T) {
	cfg := Config{
		Providers: []provider.Provider{
			{
				ID:             "gonka-a",
				Priority:       20,
				Labels:         map[string]string{"pool": "gonka"},
				RequestTimeout: Duration{Duration: 45 * time.Second},
				Config:         map[string]any{"inference_url": "https://gonka-a.invalid"},
			},
			{
				ID:       "hyperfusion",
				Priority: 10,
				Labels:   map[string]string{"pool": "backup"},
				Config:   map[string]any{"inference_url": "https://hyperfusion.invalid"},
			},
		},
		Models: map[string]policy.ModelSpec{
			"standard": {
				Profile: "high-reliability", // default RouteTimeout = 5m
				Tiers: []policy.TierSpec{
					{Targets: []policy.TargetSpec{{Select: map[string]string{"pool": "gonka"}, Native: "ds-v4"}}},
					{Targets: []policy.TargetSpec{{Provider: "hyperfusion", Native: "gonka/ds-v4"}}},
				},
			},
		},
	}

	compiled, err := PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := compiled.Providers["gonka-a"].RequestTimeout.Duration; got != 5*time.Minute {
		t.Fatalf("tier 0 provider ceiling = %v, want 5m", got)
	}
	if got := compiled.Providers["hyperfusion"].RequestTimeout.Duration; got != 5*time.Minute {
		t.Fatalf("tier 1 fallback provider ceiling = %v, want 5m", got)
	}
	plan := compiled.RoutingPlan()
	if got := plan.Providers["gonka-a"].AttemptTimeout; got != 45*time.Second {
		t.Fatalf("tier 0 AttemptTimeout = %v, want 45s", got)
	}
	if got := plan.Providers["hyperfusion"].AttemptTimeout; got != 0 {
		t.Fatalf("tier 1 unset AttemptTimeout = %v, want 0", got)
	}
}

func TestPrepareRejectsEmptyProviderLabel(t *testing.T) {
	cfg := testConfig()
	cfg.Providers[0].Labels = map[string]string{"pool": "   "}
	if _, err := PrepareAndCompile(cfg); err == nil {
		t.Fatal("expected error for blank provider label value")
	}
}
