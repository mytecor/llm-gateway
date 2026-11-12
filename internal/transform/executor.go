package transform

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/mytecor/llm-gateway/internal/model"
)

// Wrap decorates execution without taking ownership of next. Each upstream
// stream owns a new decoder; no parser state survives a retry or continuation.
func Wrap(next model.Executor, kinds []Kind) model.Executor {
	if len(kinds) == 0 {
		return next
	}
	return &executor{next: next, kinds: kinds}
}

type executor struct {
	next  model.Executor
	kinds []Kind
}

func (*executor) Close() error { return nil }
func (e *executor) Do(ctx context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
	body, err := e.next.Do(ctx, target, req)
	if err != nil || req.Kind != model.RequestChat {
		return body, err
	}
	for _, kind := range e.kinds {
		if kind != SplitThinking {
			return nil, invalidResponse()
		}
		body, err = splitResponse(body)
		if err != nil {
			return nil, err
		}
	}
	return body, nil
}
func (e *executor) Stream(ctx context.Context, target model.Target, req model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
	if req.Kind != model.RequestChat {
		return e.next.Stream(ctx, target, req)
	}
	ctx, cancel := context.WithCancel(ctx)
	upstream, err := e.next.Stream(ctx, target, req)
	if err != nil {
		cancel()
		return nil, err
	}
	out := make(chan model.StreamEvent, 1)
	go func() {
		defer close(out)
		defer cancel()
		decoders := make([]*streamDecoder, len(e.kinds))
		for i := range decoders {
			decoders[i] = newStreamDecoder()
		}
		send := func(event model.StreamEvent) bool {
			select {
			case out <- event:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for {
			select {
			case <-ctx.Done():
				return
			case event, open := <-upstream:
				if !open {
					for _, decoder := range decoders {
						if err := decoder.end(); err != nil {
							send(model.StreamEvent{Err: err})
							return
						}
					}
					return
				}
				if event.Err != nil {
					send(event)
					return
				}
				if event.Done {
					for _, decoder := range decoders {
						if terminal := decoder.end(); terminal != nil {
							send(model.StreamEvent{Err: terminal})
							return
						}
					}
					send(event)
					return
				}
				for i, kind := range e.kinds {
					if kind != SplitThinking {
						event = model.StreamEvent{Err: invalidResponse()}
						break
					}
					event, err = decoders[i].apply(event)
					if err != nil {
						event = model.StreamEvent{Err: err}
						break
					}
				}
				if !send(event) || event.Err != nil || event.Done {
					return
				}
			}
		}
	}()
	return out, nil
}

type rawObject = map[string]json.RawMessage

func invalidResponse() *model.CallError {
	return &model.CallError{Class: model.ErrorInvalid, Status: 502, Code: "invalid_upstream_response", Scope: model.FailureScopeProvider, Replay: model.ReplayCompleted}
}
func object(raw []byte) (rawObject, *model.CallError) {
	var value rawObject
	if json.Unmarshal(raw, &value) != nil || value == nil {
		return nil, invalidResponse()
	}
	return value, nil
}
func text(raw json.RawMessage) (string, *model.CallError) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return "", invalidResponse()
	}
	return value, nil
}
func writeText(object rawObject, key, value string) { object[key], _ = json.Marshal(value) }
func setReasoning(message rawObject, reasoning string) *model.CallError {
	if reasoning == "" {
		return nil
	}
	existing, err := text(message["reasoning_content"])
	if err != nil {
		return err
	}
	if existing == "" {
		existing, err = text(message["reasoning"])
		if err != nil {
			return err
		}
	}
	// Keep both established aliases equivalent. Opaque reasoning_details remain
	// untouched; inferred text never rewrites signatures or native detail records.
	writeText(message, "reasoning_content", existing+reasoning)
	writeText(message, "reasoning", existing+reasoning)
	return nil
}
func splitResponse(body []byte) ([]byte, *model.CallError) {
	root, err := object(body)
	if err != nil {
		return nil, err
	}
	var choices []rawObject
	if json.Unmarshal(root["choices"], &choices) != nil {
		return nil, invalidResponse()
	}
	changed := false
	for _, choice := range choices {
		message, err := object(choice["message"])
		if err != nil {
			return nil, err
		}
		original, err := text(message["content"])
		if err != nil {
			return nil, err
		}
		splitter := &thinkingSplitter{}
		content, reasoning := splitter.feed(original)
		finish, _ := text(choice["finish_reason"])
		tail, thought, err := splitter.finish(finish == "length")
		if err != nil {
			return nil, err
		}
		content += tail
		reasoning += thought
		if content == original && reasoning == "" {
			continue
		}
		writeText(message, "content", content)
		if err = setReasoning(message, reasoning); err != nil {
			return nil, err
		}
		choice["message"], _ = json.Marshal(message)
		changed = true
	}
	if !changed {
		return body, nil
	}
	root["choices"], _ = json.Marshal(choices)
	data, _ := json.Marshal(root)
	return data, nil
}

type choiceDecoder struct {
	splitter thinkingSplitter
	finished bool
}
type streamDecoder struct{ choices map[int]*choiceDecoder }

func newStreamDecoder() *streamDecoder { return &streamDecoder{choices: map[int]*choiceDecoder{}} }
func (d *streamDecoder) apply(event model.StreamEvent) (model.StreamEvent, *model.CallError) {
	if len(event.Data) == 0 {
		return event, nil
	}
	root, err := object(event.Data)
	if err != nil {
		return event, err
	}
	var choices []rawObject
	if json.Unmarshal(root["choices"], &choices) != nil {
		return event, invalidResponse()
	}
	changed := false
	for _, choice := range choices {
		var index int
		if json.Unmarshal(choice["index"], &index) != nil || index < 0 {
			return event, invalidResponse()
		}
		state := d.choices[index]
		if state == nil {
			state = &choiceDecoder{}
			d.choices[index] = state
		}
		var delta rawObject
		if raw := choice["delta"]; len(raw) > 0 && string(raw) != "null" {
			delta, err = object(raw)
			if err != nil {
				return event, err
			}
		} else {
			delta = rawObject{}
		}
		original, err := text(delta["content"])
		if err != nil {
			return event, err
		}
		finish, err := text(choice["finish_reason"])
		if err != nil {
			return event, err
		}
		if state.finished {
			if original != "" || finish != "" {
				return event, invalidResponse()
			}
			continue
		}
		content, reasoning := state.splitter.feed(original)
		if finish != "" {
			tail, thought, err := state.splitter.finish(finish == "length")
			if err != nil {
				return event, err
			}
			content += tail
			reasoning += thought
			state.finished = true
		}
		if content == original && reasoning == "" {
			continue
		}
		writeText(delta, "content", content)
		if err = setReasoning(delta, reasoning); err != nil {
			return event, err
		}
		choice["delta"], _ = json.Marshal(delta)
		changed = true
	}
	if changed {
		root["choices"], _ = json.Marshal(choices)
		event.Data, _ = json.Marshal(root)
		event.Meaningful = meaningful(event.Data)
	}
	return event, nil
}
func (d *streamDecoder) end() *model.CallError {
	for _, choice := range d.choices {
		if !choice.finished && choice.splitter.state == thinkingReasoning {
			return incompleteThinking()
		}
	}
	return nil
}
func meaningful(data []byte) bool {
	delta, err := model.DecodeStreamDelta(data)
	if err != nil {
		return false
	}
	for _, choice := range delta.Choices {
		text := choice.Delta.Content + choice.Delta.Reasoning + choice.Delta.ReasoningContent + choice.Delta.Refusal
		if strings.TrimSpace(strings.ReplaceAll(text, "\u200b", "")) != "" || len(choice.Delta.ToolCalls) > 0 {
			return true
		}
	}
	return false
}
