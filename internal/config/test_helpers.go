package config

import (
	"fmt"
	"github.com/mytecor/llm-gateway/internal/executor/external"
	"testing"

	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
)

func TestConfig() Config {
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
		policy.FilterModel("standard", "standard"),
		policy.FilterProvider("standard", "a", "b"),
		policy.MapRuleHelper("standard", "native-model"),
		policy.RankRuleHelper("standard"),
		policy.RaceRuleHelper("standard", 2),
		policy.RetryRuleHelper("standard", "standard.retry", 1),
		policy.FilterError("standard.retry", "429", "5xx", "timeout", "connection_error"),
		policy.FilterProviderUnused("standard.retry", "a", "b", "c"),
		policy.MapRuleHelper("standard.retry", "native-model"),
		policy.RankRuleHelper("standard.retry"),
		policy.RaceRuleHelper("standard.retry", 1),
	}
	return cfg
}

func TestConfigWithRules(cfg Config, rules []policy.Rule) Config {
	cfg.Rules = rules
	return cfg
}

func RulesConfig(t testing.TB, n int, rules []policy.Rule) *CompiledConfig {
	t.Helper()
	cfg := TestConfig()
	providers := make([]provider.Provider, 0, n)
	for i := 0; i < n; i++ {
		letter := rune('a' + i)
		providers = append(providers, provider.Provider{
			ID:       string(letter),
			Priority: 100 - i,
			Config:   map[string]any{"inference_url": fmt.Sprintf("https://%c.invalid", letter)},
		})
	}
	cfg.Providers = providers
	cfg.Rules = rules
	compiled, err := PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func RaceOnlyConfig(t testing.TB) *CompiledConfig {
	t.Helper()
	compiled, err := PrepareAndCompile(TestConfigWithRules(TestConfig(), policy.EntryRules([]string{"a", "b"}, 2)))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func catalogSources(cfg *CompiledConfig, id string) []*external.CatalogSource {
	if desc := cfg.Providers[id].Description; desc != nil && desc.Catalog != nil {
		return []*external.CatalogSource{desc.Catalog}
	}
	return nil
}
