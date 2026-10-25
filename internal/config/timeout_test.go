package config

import (
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/policy"
)

func TestTimeoutRuleExplicitIdleAndContinuePolicy(t *testing.T) {
	r := decodeRuleJSON(t, `{"route":"smart","action":"timeout","duration":"3m","idle":"5m"}`)
	timeouted, ok := r.(*policy.TimeoutRule)
	if !ok {
		t.Fatalf("decoded rule is %T, want *policy.TimeoutRule", r)
	}
	if timeouted.Duration.Duration != 3*time.Minute || timeouted.Idle.Duration != 5*time.Minute {
		t.Fatalf("decoded timeout rule mismatch: %#v", timeouted)
	}

	cfg := testConfig()
	cfg.Rules = []policy.Rule{
		filterModel("smart", "smart"),
		filterProvider("smart", "a"),
		mapRule("smart", "native-model"),
		rankRule("smart"),
		raceRule("smart", 1),
		&policy.RetryRule{RuleBase: policy.RuleBase{Route: "smart", Action: policy.ActionRetry}, Attempts: 2},
		timeouted,
	}
	compiled, err := CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := compiled.Providers["a"].RequestTimeout.Duration; got != 3*time.Minute {
		t.Fatalf("provider request_timeout should inherit max route duration 3m, got %v", got)
	}
	if compiled.Models["smart"].Continue.Enabled {
		t.Fatal("continue policy must not be enabled implicitly by retry without an explicit continue rule")
	}
	if got := compiled.Models["smart"].StreamIdleTimeout; got != 5*time.Minute {
		t.Fatalf("streamIdleTimeout = %v, want 5m from timeout.idle", got)
	}

	cfg.Rules = append(cfg.Rules, &policy.ContinueRule{
		RuleBase: policy.RuleBase{Route: "smart", Action: policy.ActionContinue},
		Retries:  2,
		Wait:     Duration{Duration: 5 * time.Minute},
	})
	compiled, err = CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	policy := compiled.Models["smart"].Continue
	if !policy.Enabled {
		t.Fatal("continue policy must be enabled when an explicit continue rule is declared")
	}
	if policy.Idle != 5*time.Minute {
		t.Fatalf("continue policy idle = %v, want 5m inherited from timeout.idle", policy.Idle)
	}
	if policy.Wait != 5*time.Minute {
		t.Fatalf("continue policy wait = %v, want 5m", policy.Wait)
	}
	if policy.Retries != 2 {
		t.Fatalf("continue policy retries = %d, want 2", policy.Retries)
	}
}
