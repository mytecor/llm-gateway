package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mytecor/llm-gateway/driverproto"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

type functionTool = driverproto.FunctionTool
type toolDefinition = driverproto.FunctionDefinition

type inboundMessage = driverproto.Command[json.RawMessage]

type rawACPConfig struct {
	Command []string `json:"command,omitempty"`
}

func validateACPConfig(msg inboundMessage, defaultCommand []string, requireID bool) ([]string, string, map[string]any, error) {
	id := strings.TrimSpace(msg.ID)
	if requireID && id == "" {
		return nil, "", nil, errors.New("session providers require id")
	}
	if strings.TrimSpace(msg.ModelsURL) != "" {
		return nil, "", nil, fmt.Errorf("provider %q: HTTP settings are not supported by acp driver", id)
	}
	var raw rawACPConfig
	trimmedCfg := bytes.TrimSpace(msg.Config)
	if len(trimmedCfg) > 0 && !bytes.Equal(trimmedCfg, []byte("null")) {
		dec := json.NewDecoder(bytes.NewReader(trimmedCfg))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&raw); err != nil {
			return nil, "", nil, fmt.Errorf("provider %q: HTTP settings are not supported by acp driver (%w)", id, err)
		}
		if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return nil, "", nil, fmt.Errorf("provider %q: trailing JSON in config", id)
		}
	}
	cmd := raw.Command
	if len(cmd) == 0 {
		cmd = msg.Command
	}
	if len(cmd) == 0 {
		cmd = defaultCommand
	}
	if len(cmd) == 0 || strings.TrimSpace(cmd[0]) == "" {
		return nil, "", nil, errors.New("session providers require id and command")
	}
	ts := strings.ToLower(strings.TrimSpace(msg.ToolStrategy))
	if ts != "" && ts != "auto" && ts != "mcp" {
		return nil, "", nil, fmt.Errorf("provider %q: ACP driver supports only auto or mcp tool_strategy, got %q", id, strings.TrimSpace(msg.ToolStrategy))
	}
	return cmd, "mcp", map[string]any{"command": cmd}, nil
}

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) > 0 && args[0] == "__mcp-bridge" {
		if err := runMCPBridge(context.Background(), args[1:]); err != nil {
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := serve(ctx, args, os.Stdin, os.Stdout); err != nil {
		os.Exit(1)
	}
}

func serve(ctx context.Context, defaultCommand []string, r io.Reader, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	var writeMu sync.Mutex
	bw := bufio.NewWriter(w)
	emit := func(ev Event) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		if ev.Type == "ready" {
			ev.ProtocolVersion = driverproto.Version
		}
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
	var turnDone chan error
	defer func() {
		cancel()
		if sess != nil {
			_ = sess.Close()
		}
		if turnDone != nil {
			<-turnDone
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
		case err := <-turnDone:
			turnDone = nil
			if err != nil {
				if errors.Is(err, errSessionNotFound) {
					_ = emit(Event{Type: "session_not_found"})
				} else {
					_ = emit(Event{Type: "error", Error: err.Error()})
				}
				return nil
			}
			_ = emit(Event{Type: "done"})
			continue
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
		if err := driverproto.CheckVersion(msg.ProtocolVersion); err != nil {
			_ = emit(Event{Type: "error", Error: err.Error()})
			return nil
		}

		switch msg.Type {
		case "init", "describe":
			_, ts, normalizedCfg, err := validateACPConfig(msg, defaultCommand, true)
			if err != nil {
				_ = emit(Event{Type: "error", Error: err.Error()})
				return nil
			}
			_ = emit(Event{
				Type:         "ready",
				ToolStrategy: ts,
				Capabilities: &Capabilities{
					Chat:      true,
					Streaming: true,
					Tools:     true,
					Stateful:  true,
					Resume:    true,
				},
				Config: normalizedCfg,
			})
			return nil
		case "models":
			cmd := msg.Command
			if len(cmd) == 0 {
				var raw rawACPConfig
				_ = json.Unmarshal(msg.Config, &raw)
				cmd = raw.Command
			}
			eng, err := ensureEngine(cmd, msg.CWD)
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
			cmd := msg.Command
			if len(cmd) == 0 {
				var raw rawACPConfig
				_ = json.Unmarshal(msg.Config, &raw)
				cmd = raw.Command
			}
			eng, err := ensureEngine(cmd, msg.CWD)
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
			var bridgeTools []Tool
			for _, t := range msg.Tools {
				bridgeTools = append(bridgeTools, Tool{
					Name:        t.Function.Name,
					Description: t.Function.Description,
					InputSchema: t.Function.Parameters,
				})
			}
			var resumeID string
			sess, resumeID, err = eng.Start(ctx, Spec{
				CWD:      dir,
				Model:    resolvedModel,
				System:   msg.System,
				ResumeID: msg.ResumeID,
				Tools:    bridgeTools,
				Emit:     emit,
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
			if turnDone != nil {
				_ = emit(Event{Type: "error", Error: "ACP turn already in progress"})
				return nil
			}
			turnDone = make(chan error, 1)
			promptText := msg.Text
			go func() {
				turnDone <- sess.RunTurn(ctx, promptText, emit)
			}()
		case "tool_result":
			if sess == nil {
				_ = emit(Event{Type: "error", Error: "ACP session not started"})
				return nil
			}
			sess.ResolveToolCall(msg.ID, msg.Content, msg.Error)
		default:
			_ = emit(Event{Type: "error", Error: "unknown ACP driver command: " + msg.Type})
		}
	}
}
