package subsystem

import (
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

// RuntimeRules is the ordered set of event rules for one client run. Only
// explicitly enabled policies create instances. The first effect wins.
type RuntimeRules []policy.RuntimeRule

func NewRuntimeRules(continuation policy.ContinueConfig, repetition policy.RepetitionConfig) RuntimeRules {
	var rules RuntimeRules
	if repetition.Enabled {
		rules = append(rules, NewRepetitionDetector(repetition))
	}
	if continuation.Enabled {
		rules = append(rules, continueRuntimeRule{})
	}
	return rules
}

func (rules RuntimeRules) Evaluate(ctx policy.RuleContext, event policy.RuleEvent) policy.RuleDecision {
	for _, rule := range rules {
		if decision := rule.Evaluate(ctx, event); decision.Action != policy.RulePass {
			return decision
		}
	}
	return policy.RuleDecision{}
}

// continueRuntimeRule decides whether recovery or tool-call finalization is
// applicable. Retry budgets, recovery timers and resharing remain scheduler
// effects driven by the compiled ContinueConfig.
type continueRuntimeRule struct{}

func (continueRuntimeRule) Evaluate(ctx policy.RuleContext, event policy.RuleEvent) policy.RuleDecision {
	if ctx.Kind != model.RequestChat {
		return policy.RuleDecision{}
	}
	switch event.Kind {
	case policy.RuleFailure:
		if event.Error != nil && !ctx.Output.HasToolCalls {
			return policy.RuleDecision{Action: policy.RuleRecover, Rule: policy.ActionContinue, Error: event.Error}
		}
	case policy.RuleStreamEnd:
		if !ctx.Output.Finished && ctx.Output.CompleteToolCalls {
			return policy.RuleDecision{Action: policy.RuleFinishToolCalls, Rule: policy.ActionContinue}
		}
	}
	return policy.RuleDecision{}
}
