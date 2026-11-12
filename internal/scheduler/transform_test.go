package scheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"
	"github.com/mytecor/llm-gateway/internal/transform"
)

func entryThinkingRule() policy.Rule {
	return &policy.TransformRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTransform}, Type: transform.SplitThinking}
}
func thinkingChunk(text string, finish string) model.StreamEvent {
	data, _ := json.Marshal(map[string]any{"id": "c", "choices": []any{map[string]any{"index": 0, "delta": map[string]any{"content": text}, "finish_reason": finish}}})
	return model.StreamEvent{Data: data, Meaningful: text != ""}
}
func TestEntryTransformationReachesFallbackBranches(t *testing.T) {
	rules := append(policytest.EntryRules([]string{"a"}, 1), entryThinkingRule(), policytest.FallbackRuleHelper("standard", "backup"), policytest.MapRuleDirect("backup", []string{"b"}, "native-model", 0), policytest.RaceRuleHelper("backup", 1))
	cfg := configtest.RulesConfig(t, 2, rules)
	runner := NewRunner(cfg.RoutingPlan(), subsystem.NewCatalog(cfg), &fakeExecutor{do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
		if target.Provider == "a" {
			return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503}
		}
		return []byte(`{"choices":[{"index":0,"message":{"content":"<think>fallback thought</think>{\"ok\":true}"},"finish_reason":"stop"}]}`), nil
	}})
	t.Cleanup(func() { _ = runner.Close() })
	result, err := runner.RunGeneration(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err != nil || result.Provider != "b" {
		t.Fatal(err)
	}
	var body struct {
		Choices []struct{ Message struct{ Content string } }
	}
	_ = json.Unmarshal(result.Body, &body)
	if body.Choices[0].Message.Content != `{"ok":true}` {
		t.Fatal(string(result.Body))
	}
	assertStreamRunReleased(t, runner, cfg)
}
func TestTransformationSurvivesContinuationWithFreshParser(t *testing.T) {
	cfg := streamRunConfig(t, []string{"a", "b"})
	cfg.Models["standard"].Transforms = []transform.Kind{transform.SplitThinking}
	executor := &fakeExecutor{stream: func(_ context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		out := make(chan model.StreamEvent, 3)
		if target.Provider == "a" {
			out <- thinkingChunk("<think>first thought</think>{\"ok\":", "")
			out <- model.StreamEvent{Err: &model.CallError{Class: model.ErrorConnection, Status: 502}}
		} else {
			out <- thinkingChunk("<think>second thought</think>true}", "")
			out <- thinkingChunk("", "stop")
		}
		close(out)
		return out, nil
	}}
	runner := NewRunner(cfg.RoutingPlan(), subsystem.NewCatalog(cfg), executor)
	t.Cleanup(func() { _ = runner.Close() })
	sink := &recordingStreamSink{}
	runner.RunGenerationStream(context.Background(), "standard", streamRunRequest(), time.Now(), sink)
	if sink.failed || !sink.completed {
		t.Fatalf("continuation failed: %v", sink.failure)
	}
	content, reason := "", ""
	for _, event := range sink.events {
		d, err := model.DecodeStreamDelta(event.Data)
		if err != nil {
			t.Fatal(err)
		}
		for _, choice := range d.Choices {
			content += choice.Delta.Content
			reason += choice.Delta.ReasoningContent
		}
	}
	if content != `{"ok":true}` || reason != "first thoughtsecond thought" {
		t.Fatalf("content=%q reasoning=%q", content, reason)
	}
	assertStreamRunReleased(t, runner, cfg)
}
