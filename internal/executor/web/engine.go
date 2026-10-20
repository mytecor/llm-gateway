// Package web connects a site-specific browser driver directly to the common
// session runtime using a small JSON-lines protocol, without ACP or HTTP hops.
package web

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/mytecor/llm-gateway/internal/session"
)

type Engine struct{ command []string }

func New(command []string) (*Engine, error) {
	if len(command) == 0 {
		return nil, errors.New("web driver command is required")
	}
	if _, err := exec.LookPath(command[0]); err != nil {
		return nil, err
	}
	return &Engine{command: append([]string(nil), command...)}, nil
}

type wireEvent struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	ResumeID  string          `json:"resume_id,omitempty"`
	Input     int64           `json:"input_tokens,omitempty"`
	Output    int64           `json:"output_tokens,omitempty"`
	Error     string          `json:"error,omitempty"`
}
type nativeSession struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	output   chan wireEvent
	closed   chan struct{}
	readDone chan struct{}
	waitDone chan struct{}
	once     sync.Once
	spec     session.Spec
	init     wireEvent
}

func (e *Engine) Start(ctx context.Context, spec session.Spec) (session.Session, error) {
	cmd := exec.Command(e.command[0], e.command[1:]...)
	cmd.Dir = spec.CWD
	// Drivers own browser credentials; stderr is deliberately excluded from logs.
	cmd.Stderr = io.Discard
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	s := &nativeSession{cmd: cmd, stdin: stdin, output: make(chan wireEvent, 32), closed: make(chan struct{}), readDone: make(chan struct{}), waitDone: make(chan struct{}), spec: spec}
	go func() {
		defer close(s.output)
		defer close(s.readDone)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 16<<20)
		for scanner.Scan() {
			var event wireEvent
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				event = wireEvent{Type: "error", Error: "invalid web driver event"}
			}
			select {
			case s.output <- event:
			case <-s.closed:
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case s.output <- wireEvent{Type: "error", Error: "web driver stream failed"}:
			case <-s.closed:
			}
		}
	}()
	go func() { <-s.readDone; _ = cmd.Wait(); close(s.waitDone) }()
	system := spec.System
	if len(spec.Tools) > 0 {
		system += "\n\n" + ToolInstructions(spec.Tools)
	}
	if err = s.write(ctx, map[string]any{"type": "start", "model": spec.Model, "system": system, "resume_id": spec.ResumeID, "tools": spec.Tools}); err != nil {
		_ = s.Close()
		return nil, err
	}
	select {
	case <-ctx.Done():
		_ = s.Close()
		return nil, ctx.Err()
	case event, ok := <-s.output:
		if !ok {
			_ = s.Close()
			return nil, errors.New("web driver closed during startup")
		}
		if event.Type == "session_not_found" {
			_ = s.Close()
			return nil, session.ErrSessionNotFound
		}
		if event.Type != "ready" {
			_ = s.Close()
			return nil, errors.New("web driver did not become ready")
		}
		s.init = event
		return s, nil
	}
}
func (s *nativeSession) write(ctx context.Context, value any) error {
	// Pipe writes can block if a broken driver stops consuming requests.
	done := make(chan error, 1)
	go func() { done <- json.NewEncoder(s.stdin).Encode(value) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = s.Close()
		return ctx.Err()
	case <-s.closed:
		return errors.New("web driver closed")
	}
}
func (s *nativeSession) Run(ctx context.Context, prompt string, emit func(session.Event) error) error {
	if s.init.Type != "" {
		if err := emit(session.Event{Kind: session.EventInit, ResumeID: s.init.ResumeID}); err != nil {
			return err
		}
		s.init = wireEvent{}
	}
	if err := s.write(ctx, map[string]string{"type": "prompt", "text": prompt}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-s.output:
			if !ok {
				return errors.New("web driver closed before completion")
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
			case "tool_call":
				if err := s.call(ctx, event.ID, event.Name, event.Arguments); err != nil {
					return err
				}
			case "message":
				// A driver for a text-only site can send the final model reply verbatim.
				// Gateway owns parsing and the pi tool bridge, rather than the driver.
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
				return errors.New("web driver: " + event.Error)
			default:
				return fmt.Errorf("unknown web driver event %q", event.Type)
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
		return err
	}
	return s.write(ctx, map[string]string{"type": "tool_result", "id": id, "content": result})
}
func (s *nativeSession) Close() error {
	s.once.Do(func() {
		close(s.closed)
		_ = s.stdin.Close()
		// Give browser drivers an opportunity to close their browser in a finally
		// block before escalating to a hard kill.
		_ = s.cmd.Process.Signal(os.Interrupt)
		select {
		case <-s.waitDone:
			return
		case <-time.After(500 * time.Millisecond):
			_ = s.cmd.Process.Kill()
		}
	})
	select {
	case <-s.waitDone:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("web driver did not stop")
	}
}
