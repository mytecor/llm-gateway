package config

import (
	"github.com/mytecor/llm-gateway/internal/executor/external"

	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"
)

func defaultConfig() Config {
	var cfg Config
	cfg.Host = "127.0.0.1"
	cfg.Port = 9208
	cfg.LogLevel = "error"
	cfg.Providers = []provider.Provider{
		{ID: "a", Priority: 20, Config: map[string]any{"inference_url": "https://a.invalid"}},
		{ID: "b", Priority: 10, Config: map[string]any{"inference_url": "https://b.invalid"}},
		{ID: "c", Priority: 5, Config: map[string]any{"inference_url": "https://c.invalid"}},
	}
	cfg.Rules = policy.Rules{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a", "b"),
		policytest.MapRuleHelper("standard", "native-model"),
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 2),
		policytest.RetryRuleHelper("standard", "standard.retry", 1),
		policytest.FilterError("standard.retry", "429", "5xx", "timeout", "connection_error"),
		policytest.FilterProviderUnused("standard.retry", "a", "b", "c"),
		policytest.MapRuleHelper("standard.retry", "native-model"),
		policytest.RankRuleHelper("standard.retry"),
		policytest.RaceRuleHelper("standard.retry", 1),
	}
	return cfg
}

func catalogSources(cfg *CompiledConfig, id string) []*external.CatalogSource {
	if desc := cfg.Providers[id].Description; desc != nil && desc.Catalog != nil {
		return []*external.CatalogSource{desc.Catalog}
	}
	return nil
}
