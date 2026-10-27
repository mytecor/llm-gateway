package policytest

import (
	"encoding/json"
	"github.com/mytecor/llm-gateway/internal/policy"
	"time"
)

func filter(route string, condition map[string]any) policy.Rule {
	data, err := json.Marshal(map[string]any{"route": route, "action": "filter", "where": condition})
	if err != nil {
		panic(err)
	}
	var rules policy.Rules
	if err := json.Unmarshal(append(append([]byte("["), data...), ']'), &rules); err != nil {
		panic(err)
	}
	return rules[0]
}

func FilterModel(route, model string) policy.Rule {
	return filter(route, map[string]any{"model": map[string]any{"eq": model}})
}
func FilterProvider(route string, providers ...string) policy.Rule {
	return filter(route, map[string]any{"provider": map[string]any{"in": providers}})
}
func FilterProviderUnused(route string, providers ...string) policy.Rule {
	return filter(route, map[string]any{"provider": map[string]any{"in": providers, "unused": true}})
}
func FilterError(route string, classes ...string) policy.Rule {
	return filter(route, map[string]any{"error": map[string]any{"in": classes}})
}

func MapRuleHelper(route, native string) policy.Rule {
	r := &policy.MapRule{Native: native}
	r.SetIdentity(route, "map")
	return r
}

func MapRuleDirect(route string, providers []string, nativeModel string, tier int) policy.Rule {
	r := &policy.MapRule{}
	r.SetIdentity(route, policy.ActionMap)
	r.Providers = providers
	r.Native = nativeModel
	r.Tier = tier
	return r
}

func RankRuleHelper(route string) policy.Rule {
	r := &policy.RankRule{Strategy: "priority"}
	r.SetIdentity(route, "rank")
	return r
}

func BalanceRuleHelper(route, strategy string) policy.Rule {
	r := &policy.BalanceRule{Strategy: strategy}
	r.SetIdentity(route, "balance")
	return r
}

func BalanceRuleCustom(route string, fn func(*policy.BalanceRule)) policy.Rule {
	r := &policy.BalanceRule{}
	r.SetIdentity(route, "balance")
	if fn != nil {
		fn(r)
	}
	return r
}

func RepetitionRuleHelper(route string) policy.Rule {
	r := &policy.RepetitionRule{}
	r.SetIdentity(route, "repetition")
	return r
}

func LeaseRuleCustom(route string, modify func(*policy.LeaseRule)) policy.Rule {
	r := &policy.LeaseRule{}
	r.SetIdentity(route, "lease")
	if modify != nil {
		modify(r)
	}
	return r
}

func AffinityRuleCustom(route string, modify func(*policy.AffinityRule)) policy.Rule {
	r := &policy.AffinityRule{}
	r.SetIdentity(route, "affinity")
	if modify != nil {
		modify(r)
	}
	return r
}

func SemaphoreRuleHelper(route string, maxCalls, maxInFlight, maxCallsPerProvider int) policy.Rule {
	r := &policy.SemaphoreRule{
		MaxCalls:            maxCalls,
		MaxInFlight:         maxInFlight,
		MaxCallsPerProvider: maxCallsPerProvider,
	}
	r.SetIdentity(route, "semaphore")
	return r
}

func RetryRuleBackoff(route, target string, attempts int, backoff *policy.BackoffConfig) policy.Rule {
	r := &policy.RetryRule{Attempts: attempts, Backoff: backoff}
	r.SetIdentity(route, "retry")
	r.Target = target
	return r
}

func RaceRuleHelper(route string, count int) policy.Rule {
	r := &policy.RaceRule{Count: count}
	r.SetIdentity(route, "race")
	return r
}

func RetryRuleHelper(route, target string, attempts int) policy.Rule {
	r := &policy.RetryRule{Attempts: attempts}
	r.SetIdentity(route, "retry")
	r.Target = target
	return r
}

func ContinueRuleHelper(route string, idle time.Duration, reshare string) policy.Rule {
	r := &policy.ContinueRule{}
	r.SetIdentity(route, "continue")
	r.Idle = policy.Duration{Duration: idle}
	r.Reshare = reshare
	return r
}

func ContinueRuleRetries(route string, idle time.Duration, reshare string, retries int) policy.Rule {
	r := ContinueRuleHelper(route, idle, reshare).(*policy.ContinueRule)
	r.Retries = retries
	return r
}

func ContinueRuleWait(route string, idle time.Duration, reshare string, retries int, wait time.Duration) policy.Rule {
	r := ContinueRuleRetries(route, idle, reshare, retries).(*policy.ContinueRule)
	r.Wait = policy.Duration{Duration: wait}
	return r
}

func FallbackRuleHelper(route, target string) policy.Rule {
	r := &policy.FallbackRule{}
	r.SetIdentity(route, "fallback")
	r.Target = target
	return r
}

func HedgeRuleHelper(route string, after time.Duration, target string) policy.Rule {
	r := &policy.HedgeRule{}
	r.SetIdentity(route, "hedge")
	r.After = policy.Duration{Duration: after}
	r.Target = target
	return r
}

func TimeoutRuleHelper(route string, duration time.Duration) policy.Rule {
	r := &policy.TimeoutRule{}
	r.SetIdentity(route, "timeout")
	r.Duration = policy.Duration{Duration: duration}
	return r
}

func AdmissionRuleHelper(route string, maxInFlight, maxPending int, waitTimeout time.Duration) policy.Rule {
	r := &policy.AdmissionRule{}
	r.SetIdentity(route, "admission")
	r.MaxInFlight = maxInFlight
	r.MaxPending = &maxPending
	r.WaitTimeout = policy.Duration{Duration: waitTimeout}
	return r
}

func EntryRules(providers []string, raceCount int) []policy.Rule {
	return []policy.Rule{
		FilterModel("standard", "standard"),
		FilterProvider("standard", providers...),
		MapRuleHelper("standard", "native-model"),
		RankRuleHelper("standard"),
		RaceRuleHelper("standard", raceCount),
	}
}
