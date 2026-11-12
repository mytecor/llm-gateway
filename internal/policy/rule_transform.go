package policy

import "github.com/mytecor/llm-gateway/internal/transform"

// TransformRule declares an opt-in output adapter for one logical model. Like
// stream policies, it belongs to the entry route after its race declaration.
type TransformRule struct {
	RuleBase
	Type transform.Kind `json:"type"`
}

func (r *TransformRule) apply(ctx *stageContext) error {
	if !ctx.st.sawRace {
		return ctx.errf("transform requires a preceding race action")
	}
	if !ctx.st.entrySet {
		return ctx.errf("transform must be declared on an entry route")
	}
	if r.Type != transform.SplitThinking {
		return ctx.errf("unsupported transform type %q", r.Type)
	}
	for _, kind := range ctx.plan.Transforms {
		if kind == r.Type {
			return ctx.errf("duplicate transform type %q", r.Type)
		}
	}
	ctx.plan.Transforms = append(ctx.plan.Transforms, r.Type)
	return nil
}
