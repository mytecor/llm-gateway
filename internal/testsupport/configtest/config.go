package configtest

import (
	"fmt"
	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"
	"testing"

	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
)

func TestConfig() config.Config {
	var cfg config.Config
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

func TestConfigWithRules(cfg config.Config, rules []policy.Rule) config.Config {
	cfg.Rules = rules
	return cfg
}

func RulesConfig(t testing.TB, n int, rules []policy.Rule) *config.CompiledConfig {
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
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func RaceOnlyConfig(t testing.TB) *config.CompiledConfig {
	t.Helper()
	compiled, err := config.PrepareAndCompile(TestConfigWithRules(TestConfig(), policytest.EntryRules([]string{"a", "b"}, 2)))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}
