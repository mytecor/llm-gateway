package config

import (
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"

	"encoding/json"
	"strings"
	"testing"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

var (
	fallbackRule         = policytest.FallbackRuleHelper
	filterModel          = policytest.FilterModel
	filterProvider       = policytest.FilterProvider
	filterProviderUnused = policytest.FilterProviderUnused
	filterError          = policytest.FilterError
	mapRule              = policytest.MapRuleHelper
	rankRule             = policytest.RankRuleHelper
	raceRule             = policytest.RaceRuleHelper
	retryRule            = policytest.RetryRuleHelper
	retryRuleBackoff     = policytest.RetryRuleBackoff
	hedgeRule            = policytest.HedgeRuleHelper
	timeoutRule          = policytest.TimeoutRuleHelper
	semaphoreRule        = policytest.SemaphoreRuleHelper
	leaseRule            = policytest.LeaseRuleCustom
	affinityRule         = policytest.AffinityRuleCustom
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
