package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type inboundMessage struct {
	Type            string      `json:"type"`
	Command         []string    `json:"command,omitempty"`
	CWD             string      `json:"cwd,omitempty"`
	Model           string      `json:"model,omitempty"`
	ReasoningEffort string      `json:"reasoning_effort,omitempty"`
	System          string      `json:"system,omitempty"`
	ResumeID        string      `json:"resume_id,omitempty"`
	Text            string      `json:"text,omitempty"`
	MCPServers      []MCPServer `json:"mcp_servers,omitempty"`
}

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := serve(ctx, args, os.Stdin, os.Stdout); err != nil {
		os.Exit(1)
	}
}

func serve(ctx context.Context, defaultCommand []string, r io.Reader, w io.Writer) error {
	var writeMu sync.Mutex
	bw := bufio.NewWriter(w)
	emit := func(ev Event) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		data, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		if _, err := bw.Write(append(data, '\n')); err != nil {
			return err
		}
		return bw.Flush()
	}

	cwd, _ := os.Getwd()
	var engine *Engine
	var sess *Session
	defer func() {
		if sess != nil {
			_ = sess.Close()
		}
	}()

	ensureEngine := func(cmd []string, dir string) (*Engine, error) {
		if engine != nil {
			return engine, nil
		}
		if len(cmd) == 0 {
			cmd = defaultCommand
		}
		if dir == "" {
			dir = cwd
		}
		var err error
		engine, err = NewEngine(cmd, dir)
		return engine, err
	}

	lines := make(chan []byte)
	scanErr := make(chan error, 1)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 4096), 16<<20)
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case lines <- line:
			case <-ctx.Done():
				return
			}
		}
		scanErr <- scanner.Err()
	}()

	for {
		var line []byte
		var ok bool
		select {
		case <-ctx.Done():
			return nil
		case line, ok = <-lines:
			if !ok {
				select {
				case err := <-scanErr:
					return err
				default:
					return nil
				}
			}
		}
		if len(line) == 0 {
			continue
		}
		var msg inboundMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			_ = emit(Event{Type: "error", Error: "invalid inbound ACP driver message"})
			return err
		}
		switch msg.Type {
		case "models":
			eng, err := ensureEngine(msg.Command, msg.CWD)
			if err != nil {
				_ = emit(Event{Type: "error", Error: err.Error()})
				continue
			}
			models, err := eng.Models(ctx)
			if err != nil {
				_ = emit(Event{Type: "error", Error: err.Error()})
				continue
			}
			_ = emit(Event{Type: "models", Models: models})
		case "start":
			if sess != nil {
				_ = emit(Event{Type: "error", Error: "ACP session already started"})
				continue
			}
			eng, err := ensureEngine(msg.Command, msg.CWD)
			if err != nil {
				_ = emit(Event{Type: "error", Error: err.Error()})
				return nil
			}
			resolvedModel, err := eng.Resolve(ctx, msg.Model, msg.ReasoningEffort)
			if err != nil {
				_ = emit(Event{Type: "error", Error: err.Error()})
				return nil
			}
			dir := msg.CWD
			if dir == "" {
				dir = cwd
			}
			var resumeID string
			sess, resumeID, err = eng.Start(ctx, Spec{
				CWD:        dir,
				Model:      resolvedModel,
				System:     msg.System,
				ResumeID:   msg.ResumeID,
				MCPServers: msg.MCPServers,
			})
			if err != nil {
				if errors.Is(err, errSessionNotFound) {
					_ = emit(Event{Type: "session_not_found"})
				} else {
					_ = emit(Event{Type: "error", Error: err.Error()})
				}
				return nil
			}
			_ = emit(Event{Type: "ready", ResumeID: resumeID})
		case "prompt":
			if sess == nil {
				_ = emit(Event{Type: "error", Error: "ACP session not started"})
				return nil
			}
			err := sess.RunTurn(ctx, msg.Text, emit)
			if err != nil {
				if errors.Is(err, errSessionNotFound) {
					_ = emit(Event{Type: "session_not_found"})
				} else {
					_ = emit(Event{Type: "error", Error: err.Error()})
				}
				return nil
			}
			_ = emit(Event{Type: "done"})
		default:
			_ = emit(Event{Type: "error", Error: "unknown ACP driver command: " + msg.Type})
		}
	}
}
