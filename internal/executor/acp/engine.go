// Package acp adapts agentrun's ACP implementation to the shared session runtime.
package acp

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
	"github.com/mytecor/llm-gateway/internal/session"
)

const catalogTTL = time.Minute

type Engine struct {
	engine      agentrun.Engine
	cwd         string
	mu          sync.Mutex
	catalog     []agentrun.ModelInfo
	discoveryMu sync.Mutex
	catalogAt   time.Time
}

func New(command []string, cwd string) (*Engine, error) {
	if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
		return nil, errors.New("ACP command is required")
	}
	e := agentacp.NewEngine(agentacp.WithBinary(command[0]), agentacp.WithArgs(command[1:]...), agentacp.WithStderrWriter(io.Discard))
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return &Engine{engine: e, cwd: cwd}, nil
}
func (e *Engine) Models(ctx context.Context) ([]session.ModelInfo, error) {
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
	result := []session.ModelInfo{}
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
			result = append(result, session.ModelInfo{ID: id, Name: m.Name})
		}
		if bracket {
			result[i].ReasoningEfforts = append(result[i].ReasoningEfforts, effort)
		}
	}
	return result, nil
}

// Resolve maps the public effort selector to the agent's native model ID.
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
func (e *Engine) Start(ctx context.Context, spec session.Spec) (session.Session, error) {
	options := map[string]string{agentrun.OptionHITL: string(agentrun.HITLOff)}
	if spec.ResumeID != "" {
		options[agentrun.OptionResumeID] = spec.ResumeID
	}
	if spec.System != "" {
		options[agentrun.OptionSystemPrompt] = spec.System
	}
	native := agentrun.Session{CWD: spec.CWD, Model: spec.Model, Options: options}
	var facade *mcpFacade
	if len(spec.Tools) > 0 {
		var err error
		facade, err = newMCPFacade(context.Background(), spec.Tools, spec.CallTool)
		if err != nil {
			return nil, err
		}
		native.MCPServers = []agentrun.MCPServer{facade.descriptor}
	}
	proc, err := e.engine.Start(ctx, native)
	if err != nil {
		if facade != nil {
			facade.close()
		}
		return nil, nativeError(err)
	}
	s := &nativeSession{process: proc, facade: facade}
	// Drain replayed history before exposing a resumed session. Do this before
	// Send starts: MCP calls and text must not race the initialization barrier.
	for {
		select {
		case <-ctx.Done():
			_ = s.Close()
			return nil, ctx.Err()
		case m, ok := <-proc.Output():
			if !ok {
				_ = s.Close()
				return nil, nativeError(proc.Err())
			}
			if m.Type == agentrun.MessageError {
				_ = s.Close()
				return nil, nativeError(errors.New(m.Content))
			}
			if m.Type == agentrun.MessageInit {
				if spec.Model != "" && m.Init != nil && m.Init.Model != "" && !e.acceptsModel(spec.Model, m.Init.Model) {
					_ = s.Close()
					return nil, errors.New("ACP selected an unexpected model")
				}
				s.init = session.Event{Kind: session.EventInit, ResumeID: m.ResumeID}
				return s, nil
			}
		}
	}
}

type nativeSession struct {
	process agentrun.Process
	facade  *mcpFacade
	init    session.Event
	once    sync.Once
}

func (s *nativeSession) Run(ctx context.Context, prompt string, emit func(session.Event) error) error {
	if s.init.Kind != "" {
		if err := emit(s.init); err != nil {
			return err
		}
		s.init = session.Event{}
	}
	sawText, sawThinking := false, false
	err := agentrun.RunTurn(ctx, s.process, prompt, func(m agentrun.Message) error {
		event := session.Event{}
		switch m.Type {
		case agentrun.MessageTextDelta:
			sawText = true
			event.Kind = session.EventText
			event.Text = m.Content
		case agentrun.MessageText:
			if sawText {
				sawText = false
				return nil
			}
			event.Kind = session.EventText
			event.Text = m.Content
		case agentrun.MessageThinkingDelta:
			sawThinking = true
			event.Kind = session.EventReasoning
			event.Text = m.Content
		case agentrun.MessageThinking:
			if sawThinking {
				sawThinking = false
				return nil
			}
			event.Kind = session.EventReasoning
			event.Text = m.Content
		case agentrun.MessageInit:
			event.Kind = session.EventInit
			event.ResumeID = m.ResumeID
		case agentrun.MessageResult:
			if m.Usage == nil {
				return nil
			}
			event.Kind = session.EventUsage
			event.Usage = &session.Usage{Input: int64(m.Usage.InputTokens), Output: int64(m.Usage.OutputTokens)}
		case agentrun.MessageError:
			return nativeError(errors.New(m.Content))
		default:
			return nil
		}
		return emit(event)
	})
	return nativeError(err)
}
func (s *nativeSession) Close() error {
	var err error
	s.once.Do(func() {
		if s.facade != nil {
			s.facade.close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = s.process.Stop(ctx)
	})
	return err
}
func nativeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, agentrun.ErrSessionNotFound) {
		return fmt.Errorf("%w: %v", session.ErrSessionNotFound, err)
	}
	for _, marker := range []string{"no conversation found", "session not found", "could not find session", "unknown session"} {
		if strings.Contains(strings.ToLower(err.Error()), marker) {
			return fmt.Errorf("%w: %v", session.ErrSessionNotFound, err)
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
