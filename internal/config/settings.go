package config

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"
)

const defaultCatalogRefresh = 10 * time.Minute

// defaultStreamIdleTimeout bounds a silently stalled winner stream: any event
// (including provider keep-alives) re-arms the timer. The default is
// deliberately conservative because reasoning models may pause mid-stream
// without emitting events for a long time.
const defaultStreamIdleTimeout = 5 * time.Minute

func normalizeSettings(cfg Config) (Config, error) {
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == 0 {
		cfg.Port = 9208
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return Config{}, fmt.Errorf("port must be between 1 and 65535")
	}
	if err := validateMetricsBind(cfg.MetricsHost, cfg.MetricsPort); err != nil {
		return Config{}, err
	}
	if cfg.MetricsHost == "" {
		cfg.MetricsHost = "127.0.0.1"
	}
	if cfg.MetricsPort == 0 {
		cfg.MetricsPort = 9209
	}
	if cfg.MetricsHost == cfg.Host && cfg.MetricsPort == cfg.Port {
		return Config{}, errors.New("metrics listener must not share the API host:port")
	}
	if cfg.CatalogRefreshInterval.Duration == 0 {
		cfg.CatalogRefreshInterval.Duration = defaultCatalogRefresh
	}
	if cfg.CatalogRefreshInterval.Duration < time.Second {
		return Config{}, fmt.Errorf("catalog_refresh_interval must be at least 1s")
	}
	if cfg.StreamIdleTimeout.Duration == 0 {
		cfg.StreamIdleTimeout.Duration = defaultStreamIdleTimeout
	}
	if cfg.StreamIdleTimeout.Duration < time.Second {
		return Config{}, fmt.Errorf("stream_idle_timeout must be at least 1s")
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "silent"
	}
	switch cfg.LogLevel {
	case "silent", "error", "warn", "info", "debug", "trace":
	default:
		return Config{}, fmt.Errorf("unsupported log_level %q", cfg.LogLevel)
	}
	if len(cfg.Providers) == 0 {
		return Config{}, errors.New("at least one provider is required")
	}
	if cfg.SessionCheckpointRetention.Duration < 0 {
		return Config{}, errors.New("session_checkpoint_retention must not be negative")
	}
	if cfg.SessionCheckpointLimit < 0 {
		return Config{}, errors.New("session_checkpoint_limit must not be negative")
	}
	cfg.SessionFile = strings.TrimSpace(cfg.SessionFile)
	if cfg.SessionFile != "" {
		path, err := filepath.Abs(cfg.SessionFile)
		if err != nil {
			return Config{}, fmt.Errorf("resolve session_file: %w", err)
		}
		cfg.SessionFile = path
	}

	return cfg, nil
}

// validateMetricsBind validates and normalizes the metrics listener bind.
// The metrics endpoint is deliberately non-public: it defaults to a dedicated
// loopback interface (127.0.0.1:9209) distinct from the API port and must not
// share a port with the API listener.
func validateMetricsBind(host string, port int) error {
	if host != "" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("metrics_host must be a numeric loopback address")
		}
	}
	if port == 0 {
		port = 9209
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("metrics_port must be between 1 and 65535")
	}
	return nil
}
