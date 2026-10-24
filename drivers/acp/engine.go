// Package main implements the standalone ACP session driver over the gateway NDJSON wire protocol.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/dmora/agentrun"
	agentacp "github.com/dmora/agentrun/engine/acp"
)

const catalogTTL = time.Minute

var errSessionNotFound = errors.New("native session not found")

type ModelInfo struct {
	ID               string   `json:"id"`
	Name             string   `json:"name,omitempty"`
	ReasoningEfforts []string `json:"reasoning_efforts,omitempty"`
}

type MCPServer struct {
	Name    string   `json:"name"`
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

type Spec struct {
	CWD        string
	Model      string
	System     string
	ResumeID   string
	MCPServers []MCPServer
}

type Event struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ResumeID string `json:"resume_id,omitempty"`
	Input    int64  `json:"input_tokens,omitempty"`
	Output   int64  `json:"output_tokens,omitempty"`
	Error    string `json:"error,omitempty"`
	Models   []ModelInfo `json:"models,omitempty"`
}

type Engine struct {
	engine      agentrun.Engine
	cwd         string
	mu          sync.Mutex
	catalog     []agentrun.ModelInfo
	discoveryMu sync.Mutex
	catalogAt   time.Time
}

func NewEngine(command []string, cwd string) (*Engine, error) {
	if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
		return nil, errors.New("ACP command is required")
	}
	e := agentacp.NewEngine(
		agentacp.WithBinary(command[0]),
		agentacp.WithArgs(command[1:]...),
		agentacp.WithStderrWriter(io.Discard),
	)
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return &Engine{engine: e, cwd: cwd}, nil
}

func (e *Engine) Models(ctx context.Context) ([]ModelInfo, error) {
	e.discoveryMu.Lock()
	defer e.discoveryMu.Unlock()
	e.mu.Lock()
	models := append([]agentrun.ModelInfo(nil), e.catalog...)
	fresh := !e.catalogAt.IsZero() && time.Since(e.catalogAt) < catalogTTL
	e.mu.Unlock()
	if !fresh {
		lister, ok := e.engine.(agentrun.ModelLister)
		if !ok {
			return nil, errors.New("model discovery unsupported")
		}
		discoveryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		discovered, err := lister.ListModels(discoveryCtx, agentrun.Session{CWD: e.cwd})
		cancel()
		if err != nil && len(models) == 0 {
			return nil, err
		}
		if err == nil {
			models = discovered
			e.mu.Lock()
			e.catalog = append([]agentrun.ModelInfo(nil), models...)
			e.catalogAt = time.Now()
			e.mu.Unlock()
		}
	}
	result := []ModelInfo{}
	indices := map[string]int{}
	for _, m := range models {
		id, effort, bracket := splitEffort(m.ID)
		if !bracket {
			id = m.ID
		}
		i, ok := indices[id]
		if !ok {
			i = len(result)
			indices[id] = i
			result = append(result, ModelInfo{ID: id, Name: m.Name})
		}
		if bracket {
			result[i].ReasoningEfforts = append(result[i].ReasoningEfforts, effort)
		}
	}
	return result, nil
}

func (e *Engine) Resolve(ctx context.Context, model, effort string) (string, error) {
	if model == "default" {
		model = ""
	}
	if model == "" {
		if effort != "" {
			return "", errors.New("select a concrete model to choose effort")
		}
		return "", nil
	}
	e.mu.Lock()
	models := append([]agentrun.ModelInfo(nil), e.catalog...)
	e.mu.Unlock()
	if len(models) == 0 {
		discoveryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, err := e.Models(discoveryCtx)
		cancel()
		if err != nil {
			if effort != "" {
				return "", fmt.Errorf("reasoning effort discovery: %w", err)
			}
			return model, nil
		}
		e.mu.Lock()
		models = append([]agentrun.ModelInfo(nil), e.catalog...)
		e.mu.Unlock()
	}
	for _, m := range models {
		base, value, ok := splitEffort(m.ID)
		if ok && base == model && (effort == "" || value == effort) {
			return m.ID, nil
		}
	}
	if effort != "" {
		return "", fmt.Errorf("reasoning effort %q is not available for model", effort)
	}
	return model, nil
}

func splitEffort(id string) (string, string, bool) {
	if !strings.HasSuffix(id, "]") {
		return "", "", false
	}
	i := strings.LastIndexByte(id, '[')
	if i <= 0 || i == len(id)-2 {
		return "", "", false
	}
	base, value := id[:i], id[i+1:len(id)-1]
	if strings.ContainsAny(base, "[]") || strings.ContainsAny(value, "[]") || strings.TrimSpace(value) == "" {
		return "", "", false
	}
	return base, value, true
}

func (e *Engine) Start(ctx context.Context, spec Spec) (*Session, string, error) {
	options := map[string]string{agentrun.OptionHITL: string(agentrun.HITLOff)}
	if spec.ResumeID != "" {
		options[agentrun.OptionResumeID] = spec.ResumeID
	}
	if spec.System != "" {
		options[agentrun.OptionSystemPrompt] = spec.System
	}
	native := agentrun.Session{CWD: spec.CWD, Model: spec.Model, Options: options}
	for _, srv := range spec.MCPServers {
		native.MCPServers = append(native.MCPServers, agentrun.MCPServer{
			Name:    srv.Name,
			Command: srv.Command,
			Args:    srv.Args,
		})
	}
	proc, err := e.engine.Start(ctx, native)
	if err != nil {
		return nil, "", classifyNativeError(err)
	}
	s := &Session{process: proc}
	for {
		select {
		case <-ctx.Done():
			_ = s.Close()
			return nil, "", ctx.Err()
		case m, ok := <-proc.Output():
			if !ok {
				_ = s.Close()
				return nil, "", classifyNativeError(proc.Err())
			}
			if m.Type == agentrun.MessageError {
				_ = s.Close()
				return nil, "", classifyNativeError(errors.New(m.Content))
			}
			if m.Type == agentrun.MessageInit {
				if spec.Model != "" && m.Init != nil && m.Init.Model != "" && !e.acceptsModel(spec.Model, m.Init.Model) {
					_ = s.Close()
					return nil, "", errors.New("ACP selected an unexpected model")
				}
				return s, m.ResumeID, nil
			}
		}
	}
}

type Session struct {
	process agentrun.Process
	once    sync.Once
}

func (s *Session) RunTurn(ctx context.Context, prompt string, emit func(Event) error) error {
	sawText, sawThinking := false, false
	err := agentrun.RunTurn(ctx, s.process, prompt, func(m agentrun.Message) error {
		switch m.Type {
		case agentrun.MessageTextDelta:
			sawText = true
			return emit(Event{Type: "text", Text: m.Content})
		case agentrun.MessageText:
			if sawText {
				sawText = false
				return nil
			}
			return emit(Event{Type: "text", Text: m.Content})
		case agentrun.MessageThinkingDelta:
			sawThinking = true
			return emit(Event{Type: "reasoning", Text: m.Content})
		case agentrun.MessageThinking:
			if sawThinking {
				sawThinking = false
				return nil
			}
			return emit(Event{Type: "reasoning", Text: m.Content})
		case agentrun.MessageInit:
			if m.ResumeID == "" {
				return nil
			}
			return emit(Event{Type: "session_updated", ResumeID: m.ResumeID})
		case agentrun.MessageResult:
			if m.Usage == nil {
				return nil
			}
			return emit(Event{
				Type:   "usage",
				Input:  int64(m.Usage.InputTokens),
				Output: int64(m.Usage.OutputTokens),
			})
		case agentrun.MessageError:
			return classifyNativeError(errors.New(m.Content))
		default:
			return nil
		}
	})
	return classifyNativeError(err)
}

func (s *Session) Close() error {
	var err error
	s.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = s.process.Stop(ctx)
	})
	return err
}

func classifyNativeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, agentrun.ErrSessionNotFound) {
		return fmt.Errorf("%w: %v", errSessionNotFound, err)
	}
	for _, marker := range []string{"no conversation found", "session not found", "could not find session", "unknown session"} {
		if strings.Contains(strings.ToLower(err.Error()), marker) {
			return fmt.Errorf("%w: %v", errSessionNotFound, err)
		}
	}
	return err
}

func (e *Engine) acceptsModel(requested, selected string) bool {
	if requested == selected {
		return true
	}
	if base, _, ok := splitEffort(requested); ok && base == selected {
		return true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, m := range e.catalog {
		if m.ID == requested {
			for _, alias := range m.Aliases {
				if alias == selected {
					return true
				}
			}
		}
	}
	return false
}
