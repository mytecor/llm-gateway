package policy

import (
	"strings"
	"testing"
	"time"
)

func mapRuleDirect(route string, providers []string, nativeModel string, tier int) Rule {
	r := &MapRule{}
	r.SetIdentity(route, ActionMap)
	r.Providers = providers
	r.Native = nativeModel
	r.Tier = tier
	return r
}

func TestAdmissionRuleCompileValidation(t *testing.T) {
	// 1. Negative wait_timeout rejected
	_, err := compileRules(
		FilterModel("standard", "standard"),
		mapRuleDirect("standard", []string{"a"}, "model-x", 0),
		&AdmissionRule{
			RuleBase:    RuleBase{Route: "standard", Action: ActionAdmission},
			MaxInFlight: 4,
			WaitTimeout: Duration{Duration: -time.Second},
		},
		RankRuleHelper("standard"),
		RaceRuleHelper("standard", 1),
	)
	if err == nil || !strings.Contains(err.Error(), "wait_timeout must not be negative") {
		t.Fatalf("expected negative wait_timeout error, got %v", err)
	}

	// 2. Negative map tier rejected
	_, err = compileRules(
		FilterModel("standard", "standard"),
		mapRuleDirect("standard", []string{"a"}, "model-x", -1),
		RankRuleHelper("standard"),
		RaceRuleHelper("standard", 1),
	)
	if err == nil || !strings.Contains(err.Error(), "tier must not be negative") {
		t.Fatalf("expected negative tier error, got %v", err)
	}

	// 3. Admission on subroute rejected
	_, err = compileRules(
		FilterModel("standard", "standard"),
		mapRuleDirect("standard", []string{"a"}, "model-x", 0),
		RankRuleHelper("standard"),
		RaceRuleHelper("standard", 1),
		RetryRuleHelper("standard", "sub", 2),
		FilterError("sub", "5xx"),
		FilterProviderUnused("sub", "b"),
		mapRuleDirect("sub", []string{"b"}, "model-x", 0),
		&AdmissionRule{
			RuleBase:    RuleBase{Route: "sub", Action: ActionAdmission},
			MaxInFlight: 2,
		},
		RankRuleHelper("sub"),
		RaceRuleHelper("sub", 1),
	)
	if err == nil || !strings.Contains(err.Error(), "subroute and must not set") {
		t.Fatalf("expected subroute admission rejection error, got %v", err)
	}

	// 4. Omitted maxPending defaults to 4 * maxInFlight
	compiled, err := compileRules(
		FilterModel("standard", "standard"),
		mapRuleDirect("standard", []string{"a"}, "model-x", 0),
		&AdmissionRule{
			RuleBase:    RuleBase{Route: "standard", Action: ActionAdmission},
			MaxInFlight: 3,
		},
		RankRuleHelper("standard"),
		RaceRuleHelper("standard", 1),
	)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	route := entryRoute(t, compiled, "standard")
	if route.Admission.MaxPending != 12 {
		t.Fatalf("expected default maxPending=12, got %d", route.Admission.MaxPending)
	}
}
