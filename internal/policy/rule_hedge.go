package policy

import "strings"

// HedgeRule enables an adaptive latency hedge: an unused candidate in the
// current pool (without target), or an explicit target route.
type HedgeRule struct {
	RuleBase
	Target string `json:"target,omitempty"`
}

func (r *HedgeRule) apply(ctx *stageContext) error {
	if !ctx.st.sawRace {
		return ctx.errf("hedge requires a preceding race action in the same route")
	}
	target := strings.TrimSpace(r.Target)
	if target != "" && target == ctx.route {
		return ctx.errf("hedge must not target the route it belongs to (use a named subroute)")
	}
	ctx.plan.Hedge = HedgeConfig{Enabled: true, Target: target, Internal: target == ""}
	return nil
}
