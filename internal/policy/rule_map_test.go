package policy

import (
	"strings"
	"testing"
	"time"
)

func TestMapRuleDecode(t *testing.T) {
	r := decodeRuleJSON(t, `{
		"route":"standard","action":"map","native":"deepseek-ai/DeepSeek-V4"
	}`)
	mapped, ok := r.(*MapRule)
	if !ok {
		t.Fatalf("decoded rule is %T, want *MapRule", r)
	}
	if mapped.route() != "standard" || mapped.action() != "map" {
		t.Fatalf("identity mismatch: %#v", mapped)
	}
	if mapped.Native != "deepseek-ai/DeepSeek-V4" {
		t.Fatalf("map fields mismatch: %#v", mapped)
	}
}

func TestMapRuleCompileBuildsPendingPool(t *testing.T) {
	result := mustCompile(t,
		filterModel("standard", "standard"),
		filterProvider("standard", "a", "b"),
		mapRule("standard", "deepseek-ai/DeepSeek-V4"),
		rankRule("standard"),
		raceRule("standard", 2),
	)
	route := entryRoute(t, result, "standard")
	if got := targetIDs(route.Pool); got != "a,b" {
		t.Fatalf("mapped pool mismatch: %s", got)
	}
	if route.Pool[0].Model != "deepseek-ai/DeepSeek-V4" {
		t.Fatalf("native id not preserved: %#v", route.Pool[0])
	}
}

func TestMapRuleDecodeRejectsForeignField(t *testing.T) {
	decodeRuleError(t, `{
		"route":"standard","action":"map",
		"native":"deepseek-ai/DeepSeek-V4","unknown_field":"x"
	}`, "unknown field \"unknown_field\"", `action "map"`)
}

func TestMapRuleRejectsMissingNative(t *testing.T) {
	_, err := compileRules(
		filterModel("standard", "standard"),
		filterProvider("standard", "a", "b"),
		mapRule("standard", ""),
		rankRule("standard"),
		raceRule("standard", 2),
	)
	if err == nil || !strings.Contains(err.Error(), "non-empty native model id") {
		t.Fatalf("expected missing native error, got %v", err)
	}
}

func TestMapRuleRejectsMissingProviderSelection(t *testing.T) {
	_, err := compileRules(
		filterModel("standard", "standard"),
		mapRule("standard", "native-model"),
		rankRule("standard"),
		raceRule("standard", 1),
	)
	if err == nil || !strings.Contains(err.Error(), "preceding filter provider") {
		t.Fatalf("expected missing provider selection error, got %v", err)
	}
}

func TestMapRuleRejectsSecondMapWithoutFilter(t *testing.T) {
	_, err := compileRules(
		filterModel("standard", "standard"),
		filterProvider("standard", "a", "b"),
		mapRule("standard", "native-x"),
		mapRule("standard", "native-y"),
		rankRule("standard"),
		raceRule("standard", 2),
	)
	if err == nil || !strings.Contains(err.Error(), "filter provider action after the previous map") {
		t.Fatalf("expected map-after-map error, got %v", err)
	}
}

func TestMapRuleRejectsUnknownProvider(t *testing.T) {
	_, err := compileRules(
		filterModel("standard", "standard"),
		filterProvider("standard", "ghost"),
		mapRule("standard", "native-model"),
		rankRule("standard"),
		raceRule("standard", 1),
	)
	if err == nil || !strings.Contains(err.Error(), "unknown provider") {
		t.Fatalf("expected unknown provider error, got %v", err)
	}
}

func TestMapRuleRejectsDuplicateProviderInPool(t *testing.T) {
	_, err := compileRules(
		filterModel("standard", "standard"),
		filterProvider("standard", "a"),
		mapRule("standard", "native-x"),
		filterProvider("standard", "a", "b"),
		mapRule("standard", "native-y"),
		rankRule("standard"),
		raceRule("standard", 2),
	)
	if err == nil || !strings.Contains(err.Error(), "more than once") {
		t.Fatalf("expected duplicate provider error, got %v", err)
	}
}

func TestMapRuleWithDirectProvidersAndTiers(t *testing.T) {
	rules := []Rule{
		FilterModel("standard", "standard"),
		mapRuleDirect("standard", []string{"a", "b"}, "model-x", 0),
		mapRuleDirect("standard", []string{"c"}, "model-y", 1),
		RankRuleHelper("standard"),
		RaceRuleHelper("standard", 1),
		&RetryRule{RuleBase: RuleBase{Route: "standard", Action: ActionRetry}},
		&TimeoutRule{RuleBase: RuleBase{Route: "standard", Action: ActionTimeout}, Duration: Duration{Duration: time.Minute}},
	}
	compiled, err := compileRules(rules...)
	if err != nil {
		t.Fatalf("compileRules failed: %v", err)
	}
	route := entryRoute(t, compiled, "standard")
	if len(route.Pool) != 3 {
		t.Fatalf("expected 3 targets, got %d", len(route.Pool))
	}
	if route.Pool[0].Provider != "a" || route.Pool[0].Tier != 0 || route.Pool[0].Model != "model-x" {
		t.Errorf("unexpected target 0: %#v", route.Pool[0])
	}
	if route.Pool[1].Provider != "b" || route.Pool[1].Tier != 0 || route.Pool[1].Model != "model-x" {
		t.Errorf("unexpected target 1: %#v", route.Pool[1])
	}
	if route.Pool[2].Provider != "c" || route.Pool[2].Tier != 1 || route.Pool[2].Model != "model-y" {
		t.Errorf("unexpected target 2: %#v", route.Pool[2])
	}
	if !route.Retry.Internal {
		t.Fatalf("expected internal retry enabled")
	}
}
