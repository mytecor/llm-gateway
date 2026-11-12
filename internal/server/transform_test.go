package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"
	"github.com/mytecor/llm-gateway/internal/transform"
)

func splitThinkingRule(route string) policy.Rule {
	return &policy.TransformRule{RuleBase: policy.RuleBase{Route: route, Action: policy.ActionTransform}, Type: transform.SplitThinking}
}
func thinkingExecutor(calls *atomic.Int32, finish string) *fakeExecutor {
	return &fakeExecutor{
		do: func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError) {
			calls.Add(1)
			return []byte(`{"id":"c","model":"native-model","choices":[{"index":0,"message":{"role":"assistant","content":"<think>private thought</think>{\"value\":1}","reasoning_details":[{"type":"reasoning.encrypted","signature":"opaque"}]},"finish_reason":"` + finish + `"}],"usage":{"prompt_tokens":3,"completion_tokens":5}}`), nil
		},
		stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			calls.Add(1)
			out := make(chan model.StreamEvent, 8)
			out <- strictChunk(map[string]any{"content": "<thi"}, nil)
			out <- strictChunk(map[string]any{"content": "nk>private thought</thi"}, nil)
			out <- strictChunk(map[string]any{"content": "nk>{\"value\":1}", "reasoning_details": []any{map[string]any{"type": "reasoning.encrypted", "signature": "opaque"}}}, nil)
			out <- strictChunk(map[string]any{}, finish)
			out <- model.StreamEvent{Data: []byte(`{"id":"c","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":5}}`)}
			close(out)
			return out, nil
		},
	}
}
func TestThinkingTransformIsScopedAndPrecedesStrictValidation(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, strict := range []bool{false, true} {
			t.Run(map[bool]string{false: "json", true: "sse"}[stream]+map[bool]string{false: "plain", true: "strict"}[strict], func(t *testing.T) {
				cfg := strictConfig()
				cfg.Rules = append(cfg.Rules, splitThinkingRule("standard"))
				cfg.Rules = append(cfg.Rules, policytest.FilterModel("untouched", "untouched"), policytest.MapRuleDirect("untouched", []string{"a"}, "native-model", 0), policytest.RaceRuleHelper("untouched", 1))
				var calls atomic.Int32
				api, _ := eventsTestServer(t, cfg, thinkingExecutor(&calls, "stop"))
				query := strictRequest(stream)
				if !strict {
					query = strings.Replace(query, `"strict":true`, `"strict":false`, 1)
				}
				resp, body := postCompletion(t, api.URL, query)
				if resp.StatusCode != 200 || calls.Load() != 1 || strings.Contains(body, "<think>") || !strings.Contains(body, "private thought") || !strings.Contains(body, "opaque") {
					t.Fatalf("transformed response: %d calls=%d %s", resp.StatusCode, calls.Load(), body)
				}
				if !stream {
					var result struct {
						Choices []struct {
							Message struct{ Content, ReasoningContent string }
						}
					}
					_ = json.Unmarshal([]byte(body), &result)
					if result.Choices[0].Message.Content != `{"value":1}` {
						t.Fatal(body)
					}
				}
				query = strings.Replace(query, `"model":"standard"`, `"model":"untouched"`, 1)
				resp, body = postCompletion(t, api.URL, query)
				if strict {
					if resp.StatusCode != 502 || calls.Load() != 4 {
						t.Fatalf("rule leaked to other model: %d %s", resp.StatusCode, body)
					}
				} else if resp.StatusCode != 200 || !strings.Contains(body, `\u003c`) && !strings.Contains(body, "<thi") {
					t.Fatalf("changed unconfigured route: %d %s", resp.StatusCode, body)
				}
			})
		}
	}
}

func TestCorrectionUsesNormalizedAssistantAndHidesRejectedThinking(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			cfg := strictConfig()
			cfg.Rules = append(cfg.Rules, splitThinkingRule("standard"))
			var calls atomic.Int32
			answer := func(req model.ExecuteRequest) string {
				if calls.Add(1) == 1 {
					return "<think>REJECTED_THOUGHT</think>{\"value\":-1}"
				}
				var body struct {
					Messages []struct{ Role, Content string }
				}
				if json.Unmarshal(req.Body, &body) != nil || len(body.Messages) != 3 || body.Messages[1].Content != `{"value":-1}` || !strings.Contains(body.Messages[2].Content, "minimum") {
					t.Error("correction did not receive normalized invalid assistant response")
				}
				return "<think>accepted thought</think>{\"value\":1}"
			}
			executor := &fakeExecutor{do: func(_ context.Context, _ model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
				return strictCompletion(answer(req)), nil
			}, stream: func(_ context.Context, _ model.Target, req model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
				out := make(chan model.StreamEvent, 2)
				out <- strictChunk(map[string]any{"content": answer(req)}, nil)
				out <- strictChunk(map[string]any{}, "stop")
				close(out)
				return out, nil
			}}
			api, _ := eventsTestServer(t, cfg, executor)
			resp, body := postCompletion(t, api.URL, strictRequest(stream))
			if resp.StatusCode != 200 || calls.Load() != 2 || strings.Contains(body, "REJECTED_THOUGHT") || !strings.Contains(body, "accepted thought") {
				t.Fatalf("hidden correction: %d %s", resp.StatusCode, body)
			}
		})
	}
}

func TestLengthFinishDoesNotRetryUnchangedGenerationBudget(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			cfg := strictConfig()
			cfg.Rules = append(cfg.Rules, splitThinkingRule("standard"), policytest.RetryRuleHelper("standard", "", 2))
			var calls atomic.Int32
			executor := &fakeExecutor{do: func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError) {
				calls.Add(1)
				return []byte(`{"choices":[{"index":0,"message":{"content":"<think>truncated reasoning"},"finish_reason":"length"}],"usage":{"prompt_tokens":3,"completion_tokens":5}}`), nil
			}, stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
				calls.Add(1)
				out := make(chan model.StreamEvent, 2)
				out <- strictChunk(map[string]any{"content": "<think>truncated reasoning"}, nil)
				out <- strictChunk(map[string]any{}, "length")
				close(out)
				return out, nil
			}}
			api, _ := eventsTestServer(t, cfg, executor)
			resp, body := postCompletion(t, api.URL, strictRequest(stream))
			if resp.StatusCode != http.StatusBadGateway || !strings.Contains(body, "structured_output_incomplete") || calls.Load() != 1 || strings.Contains(body, "truncated reasoning") {
				t.Fatalf("length handling: %d calls=%d %s", resp.StatusCode, calls.Load(), body)
			}
		})
	}
}
