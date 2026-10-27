package config

import (
	"strings"
	"testing"
)

func TestMetricsListenerRequiresNumericLoopback(t *testing.T) {
	for _, host := range []string{"", "127.0.0.1", "127.0.0.2", "::1"} {
		t.Run("accept_"+host, func(t *testing.T) {
			cfg := defaultConfig()
			cfg.MetricsHost = host
			if _, err := normalizeSettings(cfg); err != nil {
				t.Fatalf("loopback listener rejected: %v", err)
			}
		})
	}
	for _, host := range []string{"0.0.0.0", "::", "192.0.2.1", "localhost", "example.invalid"} {
		t.Run("reject_"+host, func(t *testing.T) {
			cfg := defaultConfig()
			cfg.MetricsHost = host
			if _, err := normalizeSettings(cfg); err == nil || !strings.Contains(err.Error(), "metrics_host") {
				t.Fatalf("expected metrics_host validation error, got %v", err)
			}
		})
	}
}

func TestCheckpointBoundsRejectNegativeValues(t *testing.T) {
	cfg := defaultConfig()
	cfg.SessionCheckpointLimit = -1
	if _, err := normalizeSettings(cfg); err == nil || !strings.Contains(err.Error(), "session_checkpoint_limit") {
		t.Fatalf("accepted negative checkpoint limit: %v", err)
	}
	cfg.SessionCheckpointLimit = 0
	cfg.SessionCheckpointRetention.Duration = -1
	if _, err := normalizeSettings(cfg); err == nil || !strings.Contains(err.Error(), "session_checkpoint_retention") {
		t.Fatalf("accepted negative checkpoint retention: %v", err)
	}
}
