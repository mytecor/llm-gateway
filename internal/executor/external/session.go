package external

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/mytecor/llm-gateway/driverproto"
	"github.com/mytecor/llm-gateway/internal/session"
)

type nativeSession struct {
	process      *processTransport
	output       <-chan wireEvent
	spec         session.Spec
	init         wireEvent
	toolStrategy string
}

func (d *Driver) Start(ctx context.Context, spec session.Spec) (session.Session, error) {
	process, err := startProcess(d.command, d.env, spec.CWD)
	if err != nil {
		return nil, err
	}
	s := &nativeSession{process: process, output: process.events, spec: spec, toolStrategy: d.toolStrategy}
	system := spec.System
	if (d.toolStrategy == ToolStrategyPrompt || d.toolStrategy == ToolStrategyBoth) && len(spec.Tools) > 0 {
		system += "\n\n" + ToolInstructions(spec.Tools)
	}
	event, err := process.exchange(ctx, wireInbound{
		Type: "start", ProtocolVersion: driverproto.Version,
		Config: d.driverConfig, CWD: spec.CWD, Model: spec.Model,
		ReasoningEffort: spec.ReasoningEffort,
		System:          system, ResumeID: spec.ResumeID, Tools: spec.Tools,
	}, false)
	if err == nil {
		switch event.Type {
		case "session_not_found":
			err = session.ErrSessionNotFound
		case "error":
			err = errors.New("external driver: " + event.Error)
		case "ready":
			err = driverproto.CheckVersion(event.ProtocolVersion)
		default:
			err = errors.New("external driver did not become ready")
		}
	}
	if err != nil {
		_ = process.Close()
		return nil, err
	}
	s.init = event
	return s, nil
}

func (s *nativeSession) write(ctx context.Context, value any) error {
	return s.process.write(ctx, value)
}

func (s *nativeSession) Run(ctx context.Context, prompt string, emit func(session.Event) error) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	if s.init.Type != "" {
		if err := emit(session.Event{Kind: session.EventInit, ResumeID: s.init.ResumeID}); err != nil {
			return err
		}
		s.init = wireEvent{}
	}
	if err := s.write(ctx, wireInbound{Type: "prompt", Text: prompt}); err != nil {
		return err
	}
	callErrCh := make(chan error, 1)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-callErrCh:
			return err
		case event, ok := <-s.output:
			if !ok {
				return errors.New("external driver closed before completion")
			}
			switch event.Type {
			case "text", "reasoning":
				kind := session.EventText
				if event.Type == "reasoning" {
					kind = session.EventReasoning
				}
				if err := emit(session.Event{Kind: kind, Text: event.Text}); err != nil {
					return err
				}
			case "heartbeat":
				if err := emit(session.Event{Kind: session.EventHeartbeat}); err != nil {
					return err
				}
			case "usage":
				if err := emit(session.Event{Kind: session.EventUsage, Usage: &session.Usage{Input: event.Input, Output: event.Output}}); err != nil {
					return err
				}
			case "session_updated":
				if event.ResumeID == "" {
					return errors.New("external driver sent an empty session update")
				}
				if err := emit(session.Event{Kind: session.EventInit, ResumeID: event.ResumeID}); err != nil {
					return err
				}
			case "tool_call":
				if s.toolStrategy == ToolStrategyPrompt {
					continue
				}
				wg.Add(1)
				go func(id, name string, args json.RawMessage) {
					defer wg.Done()
					if err := s.call(ctx, id, name, args); err != nil {
						select {
						case callErrCh <- err:
						default:
						}
					}
				}(event.ID, event.Name, event.Arguments)
			case "message":
				if s.toolStrategy == ToolStrategyWire || s.toolStrategy == ToolStrategyMCP {
					continue
				}
				text, calls, err := DecodeToolResponse(event.Text)
				if err != nil {
					return err
				}
				if text != "" {
					if err = emit(session.Event{Kind: session.EventText, Text: text}); err != nil {
						return err
					}
				}
				for i, call := range calls {
					if err = s.call(ctx, fmt.Sprintf("%s/%d", event.ID, i), call.Name, call.Arguments); err != nil {
						return err
					}
				}
			case "done":
				return nil
			case "session_not_found":
				return session.ErrSessionNotFound
			case "error":
				return errors.New("external driver: " + event.Error)
			default:
				return fmt.Errorf("unknown external driver event %q", event.Type)
			}
		}
	}
}

func (s *nativeSession) call(ctx context.Context, id, name string, args json.RawMessage) error {
	if s.spec.CallTool == nil {
		return errors.New("tools are unavailable")
	}
	result, err := s.spec.CallTool(ctx, name, args)
	if err != nil {
		_ = s.write(ctx, wireInbound{Type: "tool_result", ID: id, Error: err.Error()})
		return err
	}
	return s.write(ctx, wireInbound{Type: "tool_result", ID: id, Content: result})
}

func (s *nativeSession) Close() error { return s.process.Close() }
