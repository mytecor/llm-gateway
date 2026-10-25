package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

func CompileConfig(cfg Config) (*CompiledConfig, error) {
	var err error
	cfg, err = normalizeSettings(cfg)
	if err != nil {
		return nil, err
	}

	compiled := &CompiledConfig{
		Raw:            cfg,
		Logger:         telemetry.NewGatewayLogger(cfg.LogLevel),
		Providers:      make(map[string]provider.Provider, len(cfg.Providers)),
		CatalogSources: make(map[string][]CatalogSource),
		ClientKeys:     make(map[string]string, len(cfg.ClientAPIKeys)),
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
		prepared, catalog, err := provider.Prepare(rawProvider)
		if err != nil {
			return nil, err
		}

		compiled.Providers[prepared.ID] = prepared
		if catalog != nil && catalog.URL != "" {
			compiled.CatalogSources[prepared.ID] = appendCatalogSource(
				compiled.CatalogSources[prepared.ID],
				CatalogSource{URL: catalog.URL, APIKey: catalog.APIKey, Explicit: catalog.Explicit},
			)
		}
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

func appendCatalogSource(sources []CatalogSource, candidate CatalogSource) []CatalogSource {
	for index, source := range sources {
		if source.URL != candidate.URL || source.APIKey != candidate.APIKey {
			continue
		}
		if candidate.Explicit && !source.Explicit {
			sources[index].Explicit = true
		}
		return sources
	}
	return append(sources, candidate)
}
