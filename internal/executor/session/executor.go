// Package session translates OpenAI requests and responses at the edge of the
// protocol-independent session runtime. It is shared by ACP and web engines.
package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	runtime "github.com/mytecor/llm-gateway/internal/session"
)

type Resolver interface {
	Resolve(context.Context, string, string) (string, error)
}
type Executor struct {
	runtime *runtime.Runtime
	engine  runtime.Engine
}

func New(engine runtime.Engine, cfg runtime.Config) (*Executor, error) {
	r, err := runtime.New(engine, cfg)
	if err != nil {
		return nil, err
	}
	return &Executor{runtime: r, engine: engine}, nil
}
func (e *Executor) Close() error { return e.runtime.Close() }
func (e *Executor) prepare(ctx context.Context, target model.Target, req model.ExecuteRequest) (runtime.Request, *model.CallError) {
	var body struct {
		Messages   []runtime.Message      `json:"messages"`
		Tools      []runtime.FunctionTool `json:"tools"`
		ToolChoice json.RawMessage        `json:"tool_choice"`
		Effort     string                 `json:"reasoning_effort"`
		SessionID  string                 `json:"session_id"`
	}
	if req.Kind != model.RequestChat {
		return runtime.Request{}, &model.CallError{Class: model.ErrorInvalid, Status: 400, Scope: model.FailureScopeRequest, Replay: model.ReplayNotSent}
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return runtime.Request{}, callError(err, model.ReplayNotSent)
	}
	native := target.Model
	if resolver, ok := e.engine.(Resolver); ok {
		var err error
		native, err = resolver.Resolve(ctx, native, body.Effort)
		if err != nil {
			return runtime.Request{}, &model.CallError{Class: model.ErrorInvalid, Status: 400, Cause: err, Scope: model.FailureScopeRequest, Replay: model.ReplayNotSent}
		}
	} else if body.Effort != "" {
		return runtime.Request{}, &model.CallError{Class: model.ErrorInvalid, Status: 400, Scope: model.FailureScopeRequest, Replay: model.ReplayNotSent, Cause: errors.New("reasoning effort unsupported by this engine")}
	}
	conversation := req.Call.Conversation
	if conversation == "" {
		conversation = body.SessionID
	}
	return runtime.Request{Principal: req.Call.Principal, LogicalModel: req.Call.LogicalModel, Conversation: conversation, Model: native, CWD: req.Call.CWD, ReasoningEffort: body.Effort, Messages: body.Messages, Tools: body.Tools, ToolChoice: body.ToolChoice}, nil
}
func (e *Executor) Do(ctx context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
	request, err := e.prepare(ctx, target, req)
	if err != nil {
		return nil, err
	}
	result, runErr := e.runtime.Exchange(ctx, request, nil)
	if runErr != nil {
		return nil, callError(runErr, model.ReplayUnknown)
	}
	message := map[string]any{"role": "assistant", "content": result.Text}
	if len(result.ToolCalls) > 0 {
		message["tool_calls"] = result.ToolCalls
	}
	if result.Reasoning != "" {
		message["reasoning_content"] = result.Reasoning
	}
	payload := map[string]any{"id": completionID(req), "object": "chat.completion", "created": time.Now().Unix(), "model": target.Model, "choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": result.FinishReason}}}
	if result.Usage != nil {
		payload["usage"] = usage(result.Usage)
	}
	data, _ := json.Marshal(payload)
	return data, nil
}
func (e *Executor) Stream(ctx context.Context, target model.Target, req model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
	request, err := e.prepare(ctx, target, req)
	if err != nil {
		return nil, err
	}
	out := make(chan model.StreamEvent, 16)
	go func() {
		defer close(out)
		send := func(event model.StreamEvent) error {
			select {
			case out <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		id, created := completionID(req), time.Now().Unix()
		chunk := func(delta any, finish any, u *runtime.Usage, meaningful bool) error {
			payload := map[string]any{"id": id, "object": "chat.completion.chunk", "created": created, "model": target.Model, "choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}}
			if u != nil {
				payload["usage"] = usage(u)
			}
			data, _ := json.Marshal(payload)
			return send(model.StreamEvent{Data: data, Meaningful: meaningful})
		}
		if err := chunk(map[string]string{"role": "assistant"}, nil, nil, false); err != nil {
			return
		}
		index := 0
		hadOutput := false
		result, runErr := e.runtime.Exchange(ctx, request, func(event runtime.Event) error {
			switch event.Kind {
			case runtime.EventText, runtime.EventReasoning:
				if event.Text == "" {
					return nil
				}
				hadOutput = true
				key := "content"
				if event.Kind == runtime.EventReasoning {
					key = "reasoning_content"
				}
				return chunk(map[string]string{key: event.Text}, nil, nil, true)
			case runtime.EventToolCall:
				hadOutput = true
				call := event.ToolCall
				err := chunk(map[string]any{"tool_calls": []any{map[string]any{"index": index, "id": call.ID, "type": "function", "function": call.Function}}}, nil, nil, true)
				index++
				return err
			case runtime.EventHeartbeat:
				// Protocol-neutral heartbeat: never wins selection or enters history.
				return chunk(map[string]string{"reasoning_content": "\u200b"}, nil, nil, false)
			}
			return nil
		})
		if runErr != nil {
			_ = send(model.StreamEvent{Err: callError(runErr, model.ReplayUnknown)})
			return
		}
		if err := chunk(map[string]string{}, result.FinishReason, result.Usage, !hadOutput); err != nil {
			return
		}

	}()
	return out, nil
}
func completionID(req model.ExecuteRequest) string {
	if req.Call.ID != "" {
		return "chatcmpl-" + req.Call.ID
	}
	return fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
}
func usage(u *runtime.Usage) map[string]int64 {
	return map[string]int64{"prompt_tokens": u.Input, "completion_tokens": u.Output, "total_tokens": u.Input + u.Output}
}
func callError(err error, replay model.ReplaySafety) *model.CallError {
	result := &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: err, Replay: replay}
	var sessionErr *runtime.Error
	if errors.As(err, &sessionErr) {
		result.Status = sessionErr.Status
		result.Code = sessionErr.Code
		if sessionErr.Status < 500 {
			result.Scope = model.FailureScopeRequest
			result.Class = model.ErrorInvalid
			result.Replay = model.ReplayNotSent
		}
	}
	if errors.Is(err, context.Canceled) {
		result.Class = model.ErrorCancelled
		result.Status = 499
	}
	if errors.Is(err, context.DeadlineExceeded) {
		result.Class = model.ErrorTimeout
		result.Status = 504
	}
	return result
}

func (e *Executor) Models(ctx context.Context) ([]model.ModelInfo, error) {
	lister, ok := e.engine.(runtime.ModelLister)
	if !ok {
		return nil, nil
	}
	return lister.Models(ctx)
}
