package policy

import (
	"strings"
	"testing"
)

func TestRankRuleDecode(t *testing.T) {
	r := decodeRuleJSON(t, `{"route":"standard","action":"rank","strategy":"priority"}`)
	ranked, ok := r.(*RankRule)
	if !ok {
		t.Fatalf("decoded rule is %T, want *RankRule", r)
	}
	if ranked.Strategy != "priority" {
		t.Fatalf("rank strategy mismatch: %#v", ranked)
	}
}

func TestRankRuleCompileOrdersByPriority(t *testing.T) {
	result := mustCompile(t,
		filterModel("standard", "standard"),
		filterProvider("standard", "c", "a", "b"),
		mapRule("standard", "native-model"),
		rankRule("standard"),
		raceRule("standard", 3),
	)
	route := entryRoute(t, result, "standard")
	if got := targetIDs(route.Pool); got != "a,b,c" {
		t.Fatalf("priority ranking mismatch: %s", got)
	}
}

func TestRankRuleDecodeRejectsForeignField(t *testing.T) {
	decodeRuleError(t, `{
		"route":"standard","action":"rank","strategy":"priority","count":2
	}`, "unknown field \"count\"", `action "rank"`)
}

func TestRankRuleRejectsUnsupportedStrategy(t *testing.T) {
	r := &RankRule{}
	r.setIdentity("standard", "rank")
	r.Strategy = "random"
	_, err := compileRules(
		filterModel("standard", "standard"),
		filterProvider("standard", "a"),
		mapRule("standard", "native-model"),
		r,
		raceRule("standard", 1),
	)
	if err == nil || !strings.Contains(err.Error(), "rank strategy") {
		t.Fatalf("expected unsupported strategy error, got %v", err)
	}
}

func TestRankRuleRequiresPrecedingMap(t *testing.T) {
	_, err := compileRules(
		filterModel("standard", "standard"),
		filterProvider("standard", "a"),
		rankRule("standard"),
		raceRule("standard", 1),
	)
	if err == nil || !strings.Contains(err.Error(), "preceding map") {
		t.Fatalf("expected map prerequisite error, got %v", err)
	}
}

func TestRankRulePreservesTierOrder(t *testing.T) {
	cfg := TestConfig()
	// Provider b has priority 100, provider a has priority 10
	cfg.Providers[1].Priority = 100 // b
	cfg.Providers[0].Priority = 10  // a
	cfg.Rules = []Rule{
		FilterModel("standard", "standard"),
		mapRuleDirect("standard", []string{"a"}, "model-x", 0),
		mapRuleDirect("standard", []string{"b"}, "model-y", 1),
		RankRuleHelper("standard"),
		RaceRuleHelper("standard", 1),
	}
	compiled, err := CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	route := compiled.Models["standard"]
	if len(route.Pool) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(route.Pool))
	}
	// a (Tier 0) must precede b (Tier 1) even though b has higher priority!
	if route.Pool[0].Provider != "a" || route.Pool[0].Tier != 0 {
		t.Fatalf("tier 0 must precede tier 1 in rank, got target 0: %#v", route.Pool[0])
	}
	if route.Pool[1].Provider != "b" || route.Pool[1].Tier != 1 {
		t.Fatalf("tier 1 must be second in rank, got target 1: %#v", route.Pool[1])
	}
}
