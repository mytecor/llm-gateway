package policy

import (
	"encoding/json"
	"testing"

	"github.com/mytecor/llm-gateway/internal/executor/external"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/transform"
)

func transformRule(route string) Rule {
	return &TransformRule{RuleBase: RuleBase{Route: route, Action: ActionTransform}, Type: transform.SplitThinking}
}
func TestTransformRuleDecodesCompilesAndIsOptIn(t *testing.T) {
	decoded := decodeRuleJSON(t, `{"route":"standard","action":"transform","type":"split_thinking"}`)
	if _, ok := decoded.(*TransformRule); !ok {
		t.Fatalf("wrong rule: %T", decoded)
	}
	base := standardEntry()
	result := mustCompile(t, base...)
	if len(entryRoute(t, result, "standard").Transforms) != 0 {
		t.Fatal("implicit transform")
	}
	result = mustCompile(t, append(base, decoded)...)
	transforms := entryRoute(t, result, "standard").Transforms
	if len(transforms) != 1 || transforms[0] != transform.SplitThinking {
		t.Fatal(transforms)
	}
}
func TestTransformRuleRejectsAmbiguousPlacementAndUnsupportedTypes(t *testing.T) {
	for _, rules := range [][]Rule{
		append([]Rule{transformRule("standard")}, standardEntry()...),
		append(standardEntry(), transformRule("standard"), transformRule("standard")),
		append(standardEntry(), &TransformRule{RuleBase: RuleBase{Route: "standard", Action: ActionTransform}, Type: "unknown"}),
		append(append(standardEntry(), fallbackRule("standard", "sub")), append([]Rule{filterProvider("sub", "a"), mapRule("sub", "native-model"), raceRule("sub", 1)}, transformRule("sub"))...),
	} {
		if _, err := compileRules(rules...); err == nil {
			t.Fatal("invalid transform accepted")
		}
	}
	var rules Rules
	if json.Unmarshal([]byte(`[{"route":"x","action":"transform","type":"split_thinking","typo":true}]`), &rules) == nil {
		t.Fatal("unknown transform field accepted")
	}
}
func TestStatefulTransformFailsBeforeExecution(t *testing.T) {
	routes := map[string]*CompiledRoute{"agent": {Name: "agent", Entry: true, RaceCount: 1, Pool: []model.Target{{Provider: "native"}}, Transforms: []transform.Kind{transform.SplitThinking}}}
	providers := map[string]provider.Provider{"native": {Description: &external.Description{Capabilities: model.Capabilities{Stateful: true}}}}
	if err := validateSessionRoutes(routes, providers); err == nil {
		t.Fatal("stateful transform would bypass public session history")
	}
}
