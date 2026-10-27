package config

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

// CompileConfig compiles prepared providers without launching processes or making network calls.
func CompileConfig(prepared PreparedConfig) (*CompiledConfig, error) {
	cfg := prepared.config
	if len(cfg.Providers) == 0 {
		return nil, errors.New("prepare configuration before compilation")
	}

	compiled := &CompiledConfig{
		Raw:        cfg,
		Logger:     telemetry.NewGatewayLogger(cfg.LogLevel),
		Providers:  make(map[string]provider.Provider, len(cfg.Providers)),
		ClientKeys: make(map[string]string, len(cfg.ClientAPIKeys)),
	}
	for _, clientKey := range cfg.ClientAPIKeys {
		id := strings.TrimSpace(clientKey.ID)
		if id == "" {
			return nil, errors.New("every client key requires a non-empty id")
		}
		if _, duplicate := compiled.ClientKeys[id]; duplicate {
			return nil, fmt.Errorf("duplicate client key id %q", id)
		}
		if clientKey.Key == "" {
			return nil, fmt.Errorf("client key %q must not be empty", id)
		}
		compiled.ClientKeys[id] = clientKey.Key
	}
	for _, rawProvider := range cfg.Providers {
		id := strings.TrimSpace(rawProvider.ID)
		if _, exists := compiled.Providers[id]; exists {
			return nil, fmt.Errorf("duplicate provider id %q", id)
		}
		prepared := rawProvider
		if prepared.Description == nil {
			return nil, fmt.Errorf("provider %q: prepare driver capabilities before compilation", id)
		}

		compiled.Providers[prepared.ID] = prepared
	}

	result, err := policy.Compile(cfg.Rules, compiled.Providers)
	if err != nil {
		return nil, err
	}
	// A request must be allowed to run for the longest route that can use it.
	for _, route := range result.Routes {
		for _, target := range route.Pool {
			if p, ok := compiled.Providers[target.Provider]; ok && p.RequestTimeout.Duration < route.RouteTimeout {
				p.RequestTimeout.Duration = route.RouteTimeout
				compiled.Providers[target.Provider] = p
			}
		}
	}
	compiled.Routes = result.Routes
	compiled.Models = result.Models
	compiled.LogicalIDs = result.LogicalIDs
	return compiled, nil
}

// PrepareConfig performs environment-dependent normalization and driver discovery.
// Its returned configuration contains explicit descriptions consumed by compilation
// and startup, so neither phase needs a second discovery handshake.
func PrepareConfig(ctx context.Context, cfg Config) (PreparedConfig, error) {
	cfg, err := normalizeSettings(cfg)
	if err != nil {
		return PreparedConfig{}, err
	}
	cfg.Providers = append([]provider.Provider(nil), cfg.Providers...)
	seen := make(map[string]bool)
	for i, p := range cfg.Providers {
		id := strings.TrimSpace(p.ID)
		if seen[id] {
			return PreparedConfig{}, fmt.Errorf("duplicate provider id %q", id)
		}
		seen[id] = true
		prepared, err := provider.Prepare(ctx, p)
		if err != nil {
			return PreparedConfig{}, err
		}
		cfg.Providers[i] = prepared
	}
	return PreparedConfig{config: cfg}, nil
}

// PrepareAndCompile is a convenience for callers that need both phases.
// Use PrepareConfig and CompileConfig separately when controlling startup context.
func PrepareAndCompile(cfg Config) (*CompiledConfig, error) {
	prepared, err := PrepareConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	return CompileConfig(prepared)
}
