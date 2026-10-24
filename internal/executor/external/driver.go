package external

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mytecor/llm-gateway/internal/mcpbridge"
	"github.com/mytecor/llm-gateway/internal/session"
)

const catalogTTL = time.Minute

type DriverEngine struct {
	driver       string
	command      []string
	defaultCWD   string
	toolStrategy string
	mu           sync.Mutex
	catalog      []session.ModelInfo
	discoveryMu  sync.Mutex
	catalogAt    time.Time
}

func NewDriver(cfg Config) (*DriverEngine, error) {
	driver := strings.ToLower(strings.TrimSpace(cfg.Driver))
	if driver == "" {
		driver = DriverCustom
	}
	strategy := strings.ToLower(strings.TrimSpace(cfg.ToolStrategy))

	var command []string
	switch driver {
	case DriverACP:
		if len(cfg.Command) == 0 || strings.TrimSpace(cfg.Command[0]) == "" {
			return nil, errors.New("ACP driver requires an agent command")
		}
		if _, err := exec.LookPath(cfg.Command[0]); err != nil {
			return nil, err
		}
		if strategy != "" && strategy != ToolStrategyAuto && strategy != ToolStrategyMCP {
			return nil, fmt.Errorf("ACP driver supports only auto or mcp tool_strategy, got %q", cfg.ToolStrategy)
		}
		strategy = ToolStrategyMCP
		acpBin, err := ResolveDriverBinary(DriverACP)
		if err != nil {
			return nil, err
		}
		command = append([]string{acpBin, "--"}, cfg.Command...)

	case DriverWeb:
		if len(cfg.Command) > 0 {
			if _, err := exec.LookPath(cfg.Command[0]); err != nil {
				return nil, err
			}
			command = append([]string(nil), cfg.Command...)
		} else if strings.TrimSpace(cfg.Profile) != "" {
			webBin, err := ResolveDriverBinary(DriverWeb)
			if err != nil {
				return nil, err
			}
			command = []string{webBin, "run", "--profile", cfg.Profile}
			if cfg.CDPURL != "" {
				command = append(command, "--cdp-url", cfg.CDPURL)
			}
			if cfg.Account != "" {
				command = append(command, "--account", cfg.Account)
			}
			if cfg.StateDir != "" {
				command = append(command, "--state-dir", cfg.StateDir)
			}
			if cfg.Heal {
				command = append(command, "--heal")
			}
		} else {
			return nil, errors.New("web driver requires profile or command")
		}
		if strategy == "" || strategy == ToolStrategyAuto {
			strategy = ToolStrategyBoth
		}

	case DriverCustom:
		if len(cfg.Command) == 0 || strings.TrimSpace(cfg.Command[0]) == "" {
			return nil, errors.New("custom driver command is required")
		}
		if _, err := exec.LookPath(cfg.Command[0]); err != nil {
			return nil, err
		}
		command = append([]string(nil), cfg.Command...)
		if strategy == "" || strategy == ToolStrategyAuto {
			strategy = ToolStrategyBoth
		}

	default:
		return nil, fmt.Errorf("unknown session driver %q", cfg.Driver)
	}

	if strategy != ToolStrategyMCP && strategy != ToolStrategyPrompt && strategy != ToolStrategyWire && strategy != ToolStrategyBoth {
		return nil, fmt.Errorf("unknown tool strategy %q", cfg.ToolStrategy)
	}

	return &DriverEngine{
		driver:       driver,
		command:      command,
		defaultCWD:   cfg.DefaultCWD,
		toolStrategy: strategy,
	}, nil
}

type wireModelInfo struct {
	ID               string   `json:"id"`
	Name             string   `json:"name,omitempty"`
	ReasoningEfforts []string `json:"reasoning_efforts,omitempty"`
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
	Models    []wireModelInfo `json:"models,omitempty"`
}

func (e *DriverEngine) Models(ctx context.Context) ([]session.ModelInfo, error) {
	if e.driver != DriverACP {
		return nil, nil
	}
	e.discoveryMu.Lock()
	defer e.discoveryMu.Unlock()
	e.mu.Lock()
	cached := append([]session.ModelInfo(nil), e.catalog...)
	fresh := !e.catalogAt.IsZero() && time.Since(e.catalogAt) < catalogTTL
	e.mu.Unlock()
	if fresh {
		return cached, nil
	}

	discoveryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	discovered, err := e.queryModels(discoveryCtx)
	if err != nil {
		if len(cached) > 0 {
			return cached, nil
		}
		return nil, err
	}
	e.mu.Lock()
	e.catalog = append([]session.ModelInfo(nil), discovered...)
	e.catalogAt = time.Now()
	e.mu.Unlock()
	return discovered, nil
}

func (e *DriverEngine) queryModels(ctx context.Context) ([]session.ModelInfo, error) {
	cmd := exec.CommandContext(ctx, e.command[0], e.command[1:]...)
	if e.defaultCWD != "" {
		cmd.Dir = e.defaultCWD
	}
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
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	defer func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	reqData, _ := json.Marshal(map[string]any{
		"type": "models",
		"cwd":  e.defaultCWD,
	})
	if _, err := stdin.Write(append(reqData, '\n')); err != nil {
		return nil, err
	}
	_ = stdin.Close()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("driver closed without returning models")
	}
	var ev wireEvent
	if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
		return nil, err
	}
	if ev.Type == "error" {
		return nil, errors.New(ev.Error)
	}
	if ev.Type != "models" {
		return nil, fmt.Errorf("unexpected driver response %q to models request", ev.Type)
	}
	result := make([]session.ModelInfo, 0, len(ev.Models))
	for _, m := range ev.Models {
		result = append(result, session.ModelInfo{
			ID:               m.ID,
			Name:             m.Name,
			ReasoningEfforts: m.ReasoningEfforts,
		})
	}
	return result, nil
}

func (e *DriverEngine) Resolve(ctx context.Context, model, effort string) (string, error) {
	if model == "default" {
		model = ""
	}
	if model == "" {
		if effort != "" {
			return "", errors.New("select a concrete model to choose effort")
		}
		return "", nil
	}
	if e.driver != DriverACP {
		if effort != "" {
			return "", errors.New("reasoning effort unsupported by this engine")
		}
		return model, nil
	}

	e.mu.Lock()
	models := append([]session.ModelInfo(nil), e.catalog...)
	e.mu.Unlock()
	if len(models) == 0 {
		discoveryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		discovered, err := e.Models(discoveryCtx)
		cancel()
		if err != nil {
			if effort != "" {
				return "", fmt.Errorf("reasoning effort discovery: %w", err)
			}
			return model, nil
		}
		models = discovered
	}
	for _, m := range models {
		if m.ID == model && len(m.ReasoningEfforts) > 0 {
			if effort == "" {
				return fmt.Sprintf("%s[%s]", model, m.ReasoningEfforts[0]), nil
			}
			if slices.Contains(m.ReasoningEfforts, effort) {
				return fmt.Sprintf("%s[%s]", model, effort), nil
			}
			return "", fmt.Errorf("reasoning effort %q is not available for model", effort)
		}
	}
	if effort != "" {
		return "", fmt.Errorf("reasoning effort %q is not available for model", effort)
	}
	return model, nil
}

type nativeSession struct {
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	bw           *bufio.Writer
	writeMu      sync.Mutex
	output       chan wireEvent
	closed       chan struct{}
	readDone     chan struct{}
	waitDone     chan struct{}
	once         sync.Once
	spec         session.Spec
	init         wireEvent
	facade       *mcpbridge.Facade
	toolStrategy string
}

func (e *DriverEngine) Start(ctx context.Context, spec session.Spec) (session.Session, error) {
	cmd := exec.Command(e.command[0], e.command[1:]...)
	cmd.Dir = spec.CWD
	// Drivers own browser/external credentials; stderr is deliberately excluded from logs.
	cmd.Stderr = io.Discard

	var facade *mcpbridge.Facade
	var mcpServers []mcpbridge.ServerDescriptor
	if e.toolStrategy == ToolStrategyMCP && len(spec.Tools) > 0 {
		var bridgeTools []mcpbridge.Tool
		for _, tool := range spec.Tools {
			bridgeTools = append(bridgeTools, mcpbridge.Tool{Name: tool.Function.Name, Description: tool.Function.Description, InputSchema: tool.Function.Parameters})
		}
		var err error
		facade, err = mcpbridge.NewFacade(context.Background(), bridgeTools, spec.CallTool)
		if err != nil {
			return nil, err
		}
		desc := facade.Descriptor()
		mcpServers = []mcpbridge.ServerDescriptor{desc}
		descJSON, _ := json.Marshal(mcpServers)
		cmd.Env = append(os.Environ(),
			"MCP_SERVERS="+string(descJSON),
			"MCP_SERVER_COMMAND="+desc.Command,
			"MCP_SERVER_ARGS="+strings.Join(desc.Args, " "),
		)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		if facade != nil {
			facade.Close()
		}
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		if facade != nil {
			facade.Close()
		}
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		if facade != nil {
			facade.Close()
		}
		return nil, err
	}

	s := &nativeSession{
		cmd:          cmd,
		stdin:        stdin,
		bw:           bufio.NewWriter(stdin),
		output:       make(chan wireEvent, 32),
		closed:       make(chan struct{}),
		readDone:     make(chan struct{}),
		waitDone:     make(chan struct{}),
		spec:         spec,
		facade:       facade,
		toolStrategy: e.toolStrategy,
	}

	go func() {
		defer close(s.output)
		defer close(s.readDone)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 16<<20)
		for scanner.Scan() {
			var event wireEvent
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				event = wireEvent{Type: "error", Error: "invalid external driver event"}
			}
			select {
			case s.output <- event:
			case <-s.closed:
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case s.output <- wireEvent{Type: "error", Error: "external driver stream failed"}:
			case <-s.closed:
			}
		}
	}()

	go func() { <-s.readDone; _ = cmd.Wait(); close(s.waitDone) }()

	system := spec.System
	if (e.toolStrategy == ToolStrategyPrompt || e.toolStrategy == ToolStrategyBoth) && len(spec.Tools) > 0 {
		system += "\n\n" + ToolInstructions(spec.Tools)
	}

	startPayload := map[string]any{
		"type":      "start",
		"cwd":       spec.CWD,
		"model":     spec.Model,
		"system":    system,
		"resume_id": spec.ResumeID,
		"tools":     spec.Tools,
	}
	if len(mcpServers) > 0 {
		startPayload["mcp_servers"] = mcpServers
	}

	if err = s.write(ctx, startPayload); err != nil {
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
			return nil, errors.New("external driver closed during startup")
		}
		if event.Type == "session_not_found" {
			_ = s.Close()
			return nil, session.ErrSessionNotFound
		}
		if event.Type == "error" {
			_ = s.Close()
			return nil, errors.New("external driver: " + event.Error)
		}
		if event.Type != "ready" {
			_ = s.Close()
			return nil, errors.New("external driver did not become ready")
		}
		s.init = event
		return s, nil
	}
}

func (s *nativeSession) write(ctx context.Context, value any) error {
	if ctx.Err() != nil {
		_ = s.Close()
		return ctx.Err()
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		select {
		case <-s.closed:
			done <- errors.New("external driver closed")
			return
		default:
		}
		if _, err := s.bw.Write(append(data, '\n')); err != nil {
			done <- err
			return
		}
		done <- s.bw.Flush()
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = s.Close()
		return ctx.Err()
	case <-s.closed:
		return errors.New("external driver closed")
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
				if err := s.call(ctx, event.ID, event.Name, event.Arguments); err != nil {
					return err
				}
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
		return err
	}
	return s.write(ctx, map[string]string{"type": "tool_result", "id": id, "content": result})
}

func (s *nativeSession) Close() error {
	s.once.Do(func() {
		close(s.closed)
		_ = s.stdin.Close()
		if s.facade != nil {
			s.facade.Close()
		}
		// Give external drivers an opportunity to clean up in a finally/deferred
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
	case <-eWait(s.waitDone):
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("external driver did not stop")
	}
}

func eWait(ch <-chan struct{}) <-chan struct{} {
	return ch
}
