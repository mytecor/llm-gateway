package policy

import (
	"fmt"
	"testing"
	"time"
)

func FilterModel(route, model string) Rule {
	r := &FilterRule{}
	r.SetIdentity(route, "filter")
	r.Where.Model = &filterModelCond{Eq: model}
	return r
}

func FilterProvider(route string, providers ...string) Rule {
	r := &FilterRule{}
	r.SetIdentity(route, "filter")
	r.Where.Provider = &filterProviderCond{In: &providers}
	return r
}

func FilterProviderUnused(route string, in ...string) Rule {
	r := &FilterRule{}
	r.SetIdentity(route, "filter")
	r.Where.Provider = &filterProviderCond{In: &in, Unused: true}
	return r
}

func FilterError(route string, classes ...string) Rule {
	r := &FilterRule{}
	r.SetIdentity(route, "filter")
	r.Where.Error = &filterErrorCond{In: classes}
	return r
}

func MapRuleHelper(route, native string) Rule {
	r := &MapRule{Native: native}
	r.SetIdentity(route, "map")
	return r
}

func MapRuleDirect(route string, providers []string, nativeModel string, tier int) Rule {
	r := &MapRule{}
	r.SetIdentity(route, ActionMap)
	r.Providers = providers
	r.Native = nativeModel
	r.Tier = tier
	return r
}

func RankRuleHelper(route string) Rule {
	r := &RankRule{Strategy: "priority"}
	r.SetIdentity(route, "rank")
	return r
}

func BalanceRuleHelper(route, strategy string) Rule {
	r := &BalanceRule{Strategy: strategy}
	r.SetIdentity(route, "balance")
	return r
}

func BalanceRuleCustom(route string, fn func(*BalanceRule)) Rule {
	r := &BalanceRule{}
	r.SetIdentity(route, "balance")
	if fn != nil {
		fn(r)
	}
	return r
}

func RepetitionRuleHelper(route string) Rule {
	r := &RepetitionRule{}
	r.SetIdentity(route, "repetition")
	return r
}

func LeaseRuleCustom(route string, modify func(*LeaseRule)) Rule {
	r := &LeaseRule{}
	r.SetIdentity(route, "lease")
	if modify != nil {
		modify(r)
	}
	return r
}

func AffinityRuleCustom(route string, modify func(*AffinityRule)) Rule {
	r := &AffinityRule{}
	r.SetIdentity(route, "affinity")
	if modify != nil {
		modify(r)
	}
	return r
}

func SemaphoreRuleHelper(route string, maxCalls, maxInFlight, maxCallsPerProvider int) Rule {
	r := &SemaphoreRule{
		MaxCalls:            maxCalls,
		MaxInFlight:         maxInFlight,
		MaxCallsPerProvider: maxCallsPerProvider,
	}
	r.SetIdentity(route, "semaphore")
	return r
}

func RetryRuleBackoff(route, target string, attempts int, backoff *BackoffConfig) Rule {
	r := &RetryRule{Attempts: attempts, Backoff: backoff}
	r.SetIdentity(route, "retry")
	r.Target = target
	return r
}

func RaceRuleHelper(route string, count int) Rule {
	r := &RaceRule{Count: count}
	r.SetIdentity(route, "race")
	return r
}

func RetryRuleHelper(route, target string, attempts int) Rule {
	r := &RetryRule{Attempts: attempts}
	r.SetIdentity(route, "retry")
	r.Target = target
	return r
}

func ContinueRuleHelper(route string, idle time.Duration, reshare string) Rule {
	r := &ContinueRule{}
	r.SetIdentity(route, "continue")
	r.Idle = Duration{Duration: idle}
	r.Reshare = reshare
	return r
}

func ContinueRuleRetries(route string, idle time.Duration, reshare string, retries int) Rule {
	r := ContinueRuleHelper(route, idle, reshare).(*ContinueRule)
	r.Retries = retries
	return r
}

func ContinueRuleWait(route string, idle time.Duration, reshare string, retries int, wait time.Duration) Rule {
	r := ContinueRuleRetries(route, idle, reshare, retries).(*ContinueRule)
	r.Wait = Duration{Duration: wait}
	return r
}

func FallbackRuleHelper(route, target string) Rule {
	r := &FallbackRule{}
	r.SetIdentity(route, "fallback")
	r.Target = target
	return r
}

func HedgeRuleHelper(route string, after time.Duration, target string) Rule {
	r := &HedgeRule{}
	r.SetIdentity(route, "hedge")
	r.After = Duration{Duration: after}
	r.Target = target
	return r
}

func TimeoutRuleHelper(route string, duration time.Duration) Rule {
	r := &TimeoutRule{}
	r.SetIdentity(route, "timeout")
	r.Duration = Duration{Duration: duration}
	return r
}

func AdmissionRuleHelper(route string, maxInFlight, maxPending int, waitTimeout time.Duration) Rule {
	r := &AdmissionRule{}
	r.SetIdentity(route, "admission")
	r.MaxInFlight = maxInFlight
	r.MaxPending = &maxPending
	r.WaitTimeout = Duration{Duration: waitTimeout}
	return r
}

func TestConfig() Config {
	var cfg Config
	cfg.Host = "127.0.0.1"
	cfg.Port = 9208
	cfg.LogLevel = "error"
	cfg.Providers = []Provider{
		{ID: "a", BaseProvider: "openai", InferenceURL: "https://a.invalid", Priority: 20},
		{ID: "b", BaseProvider: "openai", InferenceURL: "https://b.invalid", Priority: 10},
		{ID: "c", BaseProvider: "openai", InferenceURL: "https://c.invalid", Priority: 5},
	}
	cfg.Rules = Rules{
		FilterModel("standard", "standard"),
		FilterProvider("standard", "a", "b"),
		MapRuleHelper("standard", "native-model"),
		RankRuleHelper("standard"),
		RaceRuleHelper("standard", 2),
		RetryRuleHelper("standard", "standard.retry", 1),
		FilterError("standard.retry", "429", "5xx", "timeout", "connection_error"),
		FilterProviderUnused("standard.retry", "a", "b", "c"),
		MapRuleHelper("standard.retry", "native-model"),
		RankRuleHelper("standard.retry"),
		RaceRuleHelper("standard.retry", 1),
	}
	return cfg
}

func TestConfigWithRules(cfg Config, rules []Rule) Config {
	cfg.Rules = rules
	return cfg
}

func EntryRules(providers []string, raceCount int) []Rule {
	return []Rule{
		FilterModel("standard", "standard"),
		FilterProvider("standard", providers...),
		MapRuleHelper("standard", "native-model"),
		RankRuleHelper("standard"),
		RaceRuleHelper("standard", raceCount),
	}
}

func RulesConfig(t testing.TB, n int, rules []Rule) *CompiledConfig {
	t.Helper()
	cfg := TestConfig()
	providers := make([]Provider, 0, n)
	for i := 0; i < n; i++ {
		letter := rune('a' + i)
		providers = append(providers, Provider{
			ID:           string(letter),
			BaseProvider: "openai",
			InferenceURL: fmt.Sprintf("https://%c.invalid", letter),
			Priority:     100 - i,
		})
	}
	cfg.Providers = providers
	cfg.Rules = rules
	compiled, err := CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func RaceOnlyConfig(t testing.TB) *CompiledConfig {
	t.Helper()
	compiled, err := CompileConfig(TestConfigWithRules(TestConfig(), EntryRules([]string{"a", "b"}, 2)))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}
