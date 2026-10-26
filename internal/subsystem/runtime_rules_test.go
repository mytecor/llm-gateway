package subsystem

import (
	"testing"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

func TestRuntimeRepetitionUsesDeltasAndResetsPerProvider(t *testing.T) {
	rules := NewRuntimeRules(policy.ContinueConfig{}, defaultRepetitionCfg())
	ctx := policy.RuleContext{
		Kind: model.RequestChat,
		OriginalRequest: func() model.ExecuteRequest {
			t.Fatal("repetition read message history instead of the delta")
			return model.ExecuteRequest{}
		},
	}
	feed := func(text string, want policy.RuleDecisionAction) {
		t.Helper()
		decision := rules.Evaluate(ctx, policy.RuleEvent{Kind: policy.RuleDelta, Delta: text})
		if decision.Action != want {
			t.Fatalf("delta %q: got %+v, want action %v", text, decision, want)
		}
		if want == policy.RuleStop && (decision.Error == nil || decision.Rule != policy.ActionRepetition) {
			t.Fatalf("loop decision lost its failure or rule identity: %+v", decision)
		}
	}
	feed("Tool call. Tool call. Tool cal", policy.RulePass)
	feed("l.", policy.RulePass)
	// Stream start clears the previous provider's three repetitions. The new
	// provider can emit the same phrase without inheriting its predecessor's loop.
	rules.Evaluate(ctx, policy.RuleEvent{Kind: policy.RuleStreamStart})
	feed("Tool call.", policy.RulePass)
	feed(" Tool call. Tool cal", policy.RulePass)
	feed("l. Tool call.", policy.RuleStop)
}

func TestRuntimeRepetitionStateIsPerRequest(t *testing.T) {
	first := NewRuntimeRules(policy.ContinueConfig{}, defaultRepetitionCfg())
	second := NewRuntimeRules(policy.ContinueConfig{}, defaultRepetitionCfg())
	ctx := policy.RuleContext{Kind: model.RequestChat}
	first.Evaluate(ctx, policy.RuleEvent{Kind: policy.RuleDelta, Delta: "Tool call. Tool call. Tool call."})
	event := policy.RuleEvent{Kind: policy.RuleDelta, Delta: " Tool call."}
	if decision := second.Evaluate(ctx, event); decision.Action != policy.RulePass {
		t.Fatalf("another request inherited the detector state: %+v", decision)
	}
	if decision := first.Evaluate(ctx, event); decision.Action != policy.RuleStop {
		t.Fatalf("interleaved request cleared the first detector: %+v", decision)
	}
}

func TestRuntimeContinueDecisions(t *testing.T) {
	failure := &model.CallError{Class: model.ErrorTimeout, Status: 504}
	tests := []struct {
		name    string
		enabled bool
		kind    model.RequestKind
		output  policy.RuleOutput
		event   policy.RuleEvent
		want    policy.RuleDecisionAction
	}{
		{name: "idle failure", enabled: true, kind: model.RequestChat,
			event: policy.RuleEvent{Kind: policy.RuleFailure, Error: failure}, want: policy.RuleRecover},
		{name: "disabled", kind: model.RequestChat,
			event: policy.RuleEvent{Kind: policy.RuleFailure, Error: failure}, want: policy.RulePass},
		{name: "responses", enabled: true, kind: model.RequestResponses,
			event: policy.RuleEvent{Kind: policy.RuleFailure, Error: failure}, want: policy.RulePass},
		{name: "partial tool call cannot replay", enabled: true, kind: model.RequestChat,
			output: policy.RuleOutput{HasToolCalls: true},
			event:  policy.RuleEvent{Kind: policy.RuleFailure, Error: failure}, want: policy.RulePass},
		{name: "missing finish with complete tools", enabled: true, kind: model.RequestChat,
			output: policy.RuleOutput{HasToolCalls: true, CompleteToolCalls: true},
			event:  policy.RuleEvent{Kind: policy.RuleStreamEnd}, want: policy.RuleFinishToolCalls},
		{name: "partial tools cannot finalize", enabled: true, kind: model.RequestChat,
			output: policy.RuleOutput{HasToolCalls: true},
			event:  policy.RuleEvent{Kind: policy.RuleStreamEnd}, want: policy.RulePass},
		{name: "finished tools need no synthesis", enabled: true, kind: model.RequestChat,
			output: policy.RuleOutput{Finished: true, HasToolCalls: true, CompleteToolCalls: true},
			event:  policy.RuleEvent{Kind: policy.RuleStreamEnd}, want: policy.RulePass},
		{name: "healthy delta", enabled: true, kind: model.RequestChat,
			event: policy.RuleEvent{Kind: policy.RuleDelta, Delta: "hello"}, want: policy.RulePass},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules := NewRuntimeRules(policy.ContinueConfig{Enabled: tt.enabled}, policy.RepetitionConfig{})
			decision := rules.Evaluate(policy.RuleContext{Kind: tt.kind, Output: tt.output}, tt.event)
			if decision.Action != tt.want {
				t.Fatalf("got %+v, want action %v", decision, tt.want)
			}
			if tt.want == policy.RuleRecover && decision.Error != failure {
				t.Fatal("recovery lost the original failure")
			}
		})
	}
}

func TestRuntimeRepetitionDisabledAndNonChat(t *testing.T) {
	loop := policy.RuleEvent{Kind: policy.RuleDelta, Delta: "Tool call. Tool call. Tool call. Tool call."}
	for _, enabled := range []bool{false, true} {
		for _, kind := range []model.RequestKind{model.RequestChat, model.RequestResponses} {
			cfg := defaultRepetitionCfg()
			cfg.Enabled = enabled
			rules := NewRuntimeRules(policy.ContinueConfig{}, cfg)
			decision := rules.Evaluate(policy.RuleContext{Kind: kind}, loop)
			want := policy.RulePass
			if enabled && kind == model.RequestChat {
				want = policy.RuleStop
			}
			if decision.Action != want {
				t.Fatalf("enabled=%v kind=%s: got %+v, want action %v", enabled, kind, decision, want)
			}
		}
	}
}
