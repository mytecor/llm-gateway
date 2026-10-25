package config

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

var (
	fallbackRule         = policy.FallbackRuleHelper
	filterModel          = policy.FilterModel
	filterProvider       = policy.FilterProvider
	filterProviderUnused = policy.FilterProviderUnused
	filterError          = policy.FilterError
	mapRule              = policy.MapRuleHelper
	rankRule             = policy.RankRuleHelper
	raceRule             = policy.RaceRuleHelper
	retryRule            = policy.RetryRuleHelper
	retryRuleBackoff     = policy.RetryRuleBackoff
	hedgeRule            = policy.HedgeRuleHelper
	timeoutRule          = policy.TimeoutRuleHelper
	semaphoreRule        = policy.SemaphoreRuleHelper
	leaseRule            = policy.LeaseRuleCustom
	affinityRule         = policy.AffinityRuleCustom
)

func boolPtr(v bool) *bool { return &v }
func targetIDs(targets []model.Target) string {
	ids := make([]string, 0, len(targets))
	for _, target := range targets {
		ids = append(ids, target.Provider)
	}
	return strings.Join(ids, ",")
}
func decodeRuleJSON(t *testing.T, data string) policy.Rule {
	t.Helper()
	var rules policy.Rules
	if err := json.Unmarshal([]byte("["+data+"]"), &rules); err != nil {
		t.Fatal(err)
	}
	return rules[0]
}
