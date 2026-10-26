package policy

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestLegacyRuleAliasesProduceCanonicalRules(t *testing.T) {
	var rules Rules
	input := `[{"route":"main","action":"admission","maxInFlight":2,"maxPending":0,"waitTimeout":"1s"},{"route":"main","action":"retry","target":"backup","maxAttempts":2}]`
	if err := json.Unmarshal([]byte(input), &rules); err != nil {
		t.Fatal(err)
	}
	admission := rules[0].(*AdmissionRule)
	retry := rules[1].(*RetryRule)
	if admission.MaxInFlight != 2 || admission.MaxPending == nil || *admission.MaxPending != 0 || retry.Attempts != 2 {
		t.Fatalf("bad normalized rules: %+v %+v", admission, retry)
	}
	data, err := json.Marshal(rules)
	if err != nil {
		t.Fatal(err)
	}
	for _, legacy := range []string{"maxInFlight", "maxPending", "waitTimeout", "maxAttempts"} {
		if strings.Contains(string(data), legacy) {
			t.Fatalf("legacy alias escaped decode boundary: %s", data)
		}
	}
}

func TestConflictingRuleAliasesAreRejected(t *testing.T) {
	var rules Rules
	err := json.Unmarshal([]byte(`[{"route":"main","action":"retry","attempts":1,"maxAttempts":2}]`), &rules)
	if err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatal("ambiguous rule accepted", err)
	}
}
