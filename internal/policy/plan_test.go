package policy

import (
	"testing"
	"time"
)

func TestRuntimePlanHealthRetentionIncludesRecoveryRoutes(t *testing.T) {
	fallback := &CompiledRoute{Balance: BalanceConfig{Window: 24 * time.Hour}}
	retry := &CompiledRoute{Balance: BalanceConfig{Window: time.Hour}, FallbackTarget: fallback}
	entry := &CompiledRoute{Balance: BalanceConfig{Window: time.Minute}, RetryTarget: retry, HedgeTarget: fallback}
	plan := RuntimePlan{Models: map[string]*CompiledRoute{"standard": entry}}
	if got := plan.MaxBalanceWindow(); got != 24*time.Hour {
		t.Fatalf("health history excludes recovery routes: %s", got)
	}
}

func TestContinueActionCompilesBufferLimit(t *testing.T) {
	rules := standardEntry()
	continuation := decodeRuleJSON(t, `{"route":"standard","action":"continue","idle":"5s","max_buffer_bytes":1024}`)
	compiled := mustCompile(t, append(rules, continuation)...)
	if entryRoute(t, compiled, "standard").Continue.MaxBufferBytes != 1024 {
		t.Fatal("continue action lost its buffer limit")
	}
	continuation.(*ContinueRule).MaxBufferBytes = -1
	if _, err := compileRules(append(rules, continuation)...); err == nil {
		t.Fatal("accepted negative continuation buffer limit")
	}
}
