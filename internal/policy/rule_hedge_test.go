package policy

import "testing"

func TestHedgeRuleDecode(t *testing.T) {
	r := decodeRuleJSON(t, `{"route":"standard","action":"hedge","target":"standard.hedge"}`)
	hedged, ok := r.(*HedgeRule)
	if !ok || hedged.Target != "standard.hedge" {
		t.Fatalf("unexpected hedge: %#v", r)
	}
}

func TestHedgeRuleCompileResolvesTarget(t *testing.T) {
	result := mustCompile(t,
		filterModel("standard", "standard"), filterProvider("standard", "a", "b"),
		mapRule("standard", "native-model"), rankRule("standard"), raceRule("standard", 2),
		hedgeRule("standard", "standard.hedge"),
		filterProvider("standard.hedge", "b", "c"), mapRule("standard.hedge", "native-model"),
		rankRule("standard.hedge"), raceRule("standard.hedge", 1),
	)
	route := entryRoute(t, result, "standard")
	if !route.Hedge.Enabled || route.Hedge.Target != "standard.hedge" || route.HedgeTarget == nil {
		t.Fatalf("hedge target not resolved: %#v", route.Hedge)
	}
	if got := targetIDs(route.HedgeTarget.Pool); got != "b,c" {
		t.Fatalf("hedge pool: %s", got)
	}
}

func TestHedgeRuleDecodeRejectsForeignField(t *testing.T) {
	decodeRuleError(t, `{"route":"standard","action":"hedge","count":2}`, `unknown field "count"`, `action "hedge"`)
}

func TestHedgeRuleRejectsAfter(t *testing.T) {
	for _, value := range []string{`"3s"`, `"0s"`, `null`, `123`} {
		decodeRuleError(t, `{"route":"standard","action":"hedge","after":`+value+`}`,
			"remove after: hedge delay is calculated automatically", `action "hedge"`)
	}
}

func TestHedgeRuleWithoutTargetEnablesInternalHedge(t *testing.T) {
	result := mustCompile(t, append(standardEntry(), hedgeRule("standard", ""))...)
	route := entryRoute(t, result, "standard")
	if !route.Hedge.Internal || !route.Hedge.Enabled {
		t.Fatalf("expected adaptive internal hedge: %#v", route.Hedge)
	}
}
