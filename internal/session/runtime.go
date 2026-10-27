package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Runtime serializes conversation exchanges and owns native sessions and turns.
// A successful Exchange may suspend a turn for tools; its context then detaches.
// Only turn timeout, shutdown, or a later exchange can cancel that suspended turn.
type Runtime struct {
	engine      Engine
	config      Config
	registry    *registry
	ctx         context.Context
	cancel      context.CancelFunc
	close       sync.Once
	janitorDone chan struct{}
}

func New(engine Engine, cfg Config) (*Runtime, error) {
	if engine == nil {
		return nil, errors.New("session engine is required")
	}
	if cfg.TurnTimeout <= 0 {
		cfg.TurnTimeout = 30 * time.Minute
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 20 * time.Second
	}
	if cfg.SessionTTL == 0 {
		cfg.SessionTTL = 10 * time.Minute
	}
	if cfg.SessionTTL < 0 {
		return nil, errors.New("session TTL must not be negative")
	}
	if !filepath.IsAbs(cfg.DefaultCWD) {
		return nil, errors.New("default CWD must be absolute")
	}
	for _, root := range cfg.AllowedRoots {
		if !filepath.IsAbs(root) {
			return nil, errors.New("allowed roots must be absolute")
		}
	}
	registry, err := newRegistry(cfg.SessionTTL, cfg.Store, cfg.StorePath)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Runtime{engine: engine, config: cfg, registry: registry, ctx: ctx, cancel: cancel, janitorDone: make(chan struct{})}
	go func() {
		defer close(r.janitorDone)
		interval := min(time.Minute, max(time.Second, cfg.SessionTTL/2))
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.registry.evictIdle()
			}
		}
	}()
	return r, nil
}
func (r *Runtime) Close() error {
	r.close.Do(func() { r.cancel(); <-r.janitorDone; r.registry.close() })
	return nil
}
func (r *Runtime) clear(s *sessionState, key string) error {
	s.stop()
	s.history = nil
	s.historyCount = 0
	s.historyHash = ""
	s.resumeID = ""
	s.toolHash = ""
	return r.registry.store.delete(key)
}
func (r *Runtime) Exchange(ctx context.Context, req Request, emit func(Event) error) (*Result, error) {
	if r.ctx.Err() != nil {
		return nil, &Error{Status: 503, Code: "runtime_closed"}
	}
	if req.Conversation == "" {
		return nil, invalid("session_required", errors.New("conversation ID is required"))
	}
	tools, hash, policy, err := normalizeTools(req.Tools, req.ToolChoice)
	if err != nil {
		return nil, invalid("invalid_tools", err)
	}
	messages, err := normalizeMessages(req.Messages)
	if err != nil {
		return nil, invalid("invalid_messages", err)
	}
	cwd := strings.TrimSpace(req.CWD)
	if cwd == "" {
		cwd = r.config.DefaultCWD
	}
	cwd, err = resolveCWD(cwd, r.config.AllowedRoots)
	if err != nil {
		return nil, invalid("invalid_cwd", err)
	}
	// Structured keys avoid collisions between user-supplied names and isolate
	// principals, providers, native models and effort selections in the store.
	keyParts := []string{req.Principal, req.Conversation, req.LogicalModel, req.Model, req.ReasoningEffort}
	if r.config.Provider != "" {
		keyParts = append(keyParts, r.config.Provider)
	}
	keyBytes, _ := json.Marshal(keyParts)
	key := string(keyBytes)
	lockCtx, stopLock := context.WithCancel(ctx)
	stopShutdown := context.AfterFunc(r.ctx, stopLock)
	defer stopLock()
	defer stopShutdown()
	state, err := r.registry.lock(lockCtx, key)
	if err != nil {
		return nil, err
	}
	defer state.unlock()
	if r.ctx.Err() != nil {
		return nil, &Error{Status: 503, Code: "runtime_closed"}
	}
	state.lastAccess = time.Now()
	if state.active != nil && state.active.ctx.Err() != nil {
		if err = r.clear(state, key); err != nil {
			return nil, err
		}
	}
	same := state.cwd == cwd && state.toolHash == hash && state.matchesHistoryPrefix(messages)
	isResult := messages[len(messages)-1].Role == "tool"
	var start func(bool, string) error
	resuming := false
	if isResult {
		if state.active == nil || !same {
			if state.active != nil && (state.cwd != cwd || state.toolHash != hash) {
				_ = r.clear(state, key)
			}
			return nil, invalid("invalid_tool_result", errors.New("tool result does not match a live turn, history and toolset"))
		}
		if err = state.active.resolve(messages[state.persistedHistoryCount():], policy); err != nil {
			return nil, invalid("invalid_tool_result", err)
		}
	} else {
		if state.active != nil && (same || (state.cwd == cwd && state.toolHash == hash && hasPrefix(state.history, messages))) {
			return nil, &Error{Status: 409, Code: "turn_pending"}
		}
		resume := state.process == nil && state.resumeID != "" && same
		resuming = resume
		delta := messages
		if same && (state.process != nil || resume) {
			delta = messages[state.persistedHistoryCount():]
		} else {
			if err = r.clear(state, key); err != nil {
				return nil, err
			}
		}
		for _, m := range delta {
			if m.Role == "tool" {
				return nil, invalid("invalid_tool_result", errors.New("unexpected tool result"))
			}
		}
		prompt, promptErr := toolTurnPrompt(delta, policy)
		if promptErr != nil {
			return nil, invalid("invalid_messages", promptErr)
		}
		preserveResumeOnStartError := false
		start = func(useResume bool, prompt string) error {
			preserveResumeOnStartError = useResume
			if err := ctx.Err(); err != nil {
				return err
			}
			turnCtx, cancel := context.WithTimeout(r.ctx, r.config.TurnTimeout)
			turn := newActiveTurn(turnCtx, policy)
			state.active = turn
			state.current.Store(turn)
			state.cwd = cwd
			state.toolHash = hash
			if state.process == nil {
				spec := Spec{CWD: cwd, Model: req.Model, ReasoningEffort: req.ReasoningEffort, System: systemPrompt(messages), Tools: tools}
				if useResume {
					spec.ResumeID = state.resumeID
				}
				spec.CallTool = func(callCtx context.Context, name string, args json.RawMessage) (string, error) {
					current := state.current.Load()
					if current == nil {
						return "", errors.New("no active turn")
					}
					known := false
					for _, tool := range tools {
						if tool.Function.Name == name {
							known = true
							break
						}
					}
					if !known {
						return "", fmt.Errorf("unknown tool %q", name)
					}
					return current.call(callCtx, name, args)
				}
				stopStartup := context.AfterFunc(ctx, func() { turn.cancel(ctx.Err()) })
				proc, startErr := r.engine.Start(turn.ctx, spec)
				stopStartup()
				if startErr != nil {
					close(turn.done)
					turn.cancel(startErr)
					cancel()
					state.active = nil
					state.current.Store(nil)
					return startErr
				}
				state.process = proc
			}
			// Keep the last checkpoint throughout startup: no prompt has been sent yet.
			// Once Run can submit work, an interrupted turn is no longer recoverable.
			if err := r.registry.store.delete(key); err != nil {
				if state.process != nil {
					_ = state.process.Close()
					state.process = nil
				}
				close(turn.done)
				turn.cancel(err)
				cancel()
				state.active = nil
				state.current.Store(nil)
				return err
			}
			preserveResumeOnStartError = false
			go turn.run(state.process, prompt)
			// A suspended turn has no HTTP/request goroutine to clean it up at expiry.
			go func() {
				<-turn.ctx.Done()
				cancel()
				if turn.completed.Load() {
					return
				}
				state.gate <- struct{}{}
				defer state.unlock()
				if state.active == turn {
					_ = r.clear(state, key)
				}
			}()
			return nil
		}
		err = start(resume, prompt)
		if err != nil && resume && errors.Is(err, ErrSessionNotFound) {
			resuming = false
			if clearErr := r.clear(state, key); clearErr != nil {
				return nil, clearErr
			}
			prompt, err = toolTurnPrompt(messages, policy)
			if err == nil {
				err = start(false, prompt)
			}
		}
		if err != nil {
			if !preserveResumeOnStartError {
				_ = r.clear(state, key)
			}
			return nil, err
		}
	}
	turn := state.active
	// The synchronous AfterFunc detach ensures a completed HTTP exchange cannot
	// cancel the pending tool turn when its request context is subsequently freed.
	attach := func(t *activeTurn) func() {
		detached := make(chan struct{})
		stop := context.AfterFunc(ctx, func() { t.cancel(ctx.Err()); close(detached) })
		return func() {
			if !stop() {
				<-detached
			}
		}
	}
	detach := attach(turn)
	defer func() { detach() }()
	result := &Result{FinishReason: "stop"}
	var text, reasoning strings.Builder
	lastEvent := time.Now()
	var heartbeat <-chan time.Time
	if emit != nil && r.config.HeartbeatInterval > 0 {
		ticker := time.NewTicker(r.config.HeartbeatInterval)
		defer ticker.Stop()
		heartbeat = ticker.C
	}
	onEvent := func(e Event) error {
		lastEvent = time.Now()
		if e.Kind == EventInit && e.ResumeID != "" {
			state.resumeID = e.ResumeID
		}
		if e.Kind == EventText {
			text.WriteString(e.Text)
		}
		if e.Kind == EventReasoning {
			reasoning.WriteString(e.Text)
		}
		if e.Usage != nil {
			result.Usage = e.Usage
		}
		if emit != nil {
			return emit(e)
		}
		return nil
	}
	failed := func(err error) (*Result, error) { _ = r.clear(state, key); return nil, err }
	for {
		select {
		case <-heartbeat:
			if time.Since(lastEvent) >= r.config.HeartbeatInterval {
				if err = onEvent(Event{Kind: EventHeartbeat}); err != nil {
					return failed(err)
				}
			}
		case <-ctx.Done():
			return failed(ctx.Err())
		case <-turn.ctx.Done():
			return failed(context.Cause(turn.ctx))
		case event := <-turn.events:
			if event.message != nil {
				if err = onEvent(*event.message); err != nil {
					return failed(err)
				}
			}
			if event.call != nil {
				// Drain already queued content before closing this response segment.
				for draining := true; draining; {
					select {
					case next := <-turn.events:
						if next.message != nil {
							if err = onEvent(*next.message); err != nil {
								return failed(err)
							}
						}
						if next.call != nil {
							result.ToolCalls = append(result.ToolCalls, *next.call)
						}
						if next.done {
							return failed(errors.New("turn completed while tools are pending"))
						}
					default:
						draining = false
					}
				}
				result.ToolCalls = append([]ToolCall{*event.call}, result.ToolCalls...)
				for _, call := range result.ToolCalls {
					if policy.Mode == "none" || (policy.Mode == "named" && policy.Name != call.Function.Name) {
						return failed(&Error{Status: 502, Code: "tool_choice_unsatisfied", Cause: errors.New("pending call conflicts with current tool_choice")})
					}
				}
				for _, call := range result.ToolCalls {
					turn.deliver(call.ID)
					call := call
					if err = onEvent(Event{Kind: EventToolCall, ToolCall: &call}); err != nil {
						return failed(err)
					}
				}
				result.FinishReason = "tool_calls"
			}
			if event.done {
				if event.err != nil {
					if resuming && !isResult && errors.Is(event.err, ErrSessionNotFound) && text.Len() == 0 && reasoning.Len() == 0 && len(result.ToolCalls) == 0 {
						resuming = false
						detach()
						if err = r.clear(state, key); err != nil {
							return nil, err
						}
						prompt, promptErr := toolTurnPrompt(messages, policy)
						if promptErr != nil {
							return nil, promptErr
						}
						if err = start(false, prompt); err != nil {
							return failed(err)
						}
						turn = state.active
						detach = attach(turn)
						result = &Result{FinishReason: "stop"}
						text.Reset()
						reasoning.Reset()
						continue
					}
					return failed(event.err)
				}
				if policy.Mode == "required" || policy.Mode == "named" {
					return failed(&Error{Status: 502, Code: "tool_choice_unsatisfied", Cause: errors.New("turn finished without required tool call")})
				}
				turn.completed.Store(true)
				state.active = nil
				state.current.Store(nil)
				turn.cancel(context.Canceled)
			}
			if event.call != nil || event.done {
				result.Text = text.String()
				result.Reasoning = reasoning.String()
				state.history = append(append([]transcriptMessage(nil), messages...), transcriptMessage{Role: "assistant", Content: result.Text, ToolCalls: result.ToolCalls})
				state.historyCount = len(state.history)
				state.historyHash = transcriptHash(state.history)
				state.lastAccess = time.Now()
				if state.active == nil && state.resumeID != "" {
					if err = r.registry.store.put(key, persistedSession{ResumeID: state.resumeID, CWD: cwd, HistoryCount: state.historyCount, HistoryHash: state.historyHash, ToolHash: hash}); err != nil {
						return failed(err)
					}
				}
				return result, nil
			}
		}
	}
}
func toolTurnPrompt(messages []transcriptMessage, policy toolPolicy) (string, error) {
	prompt, err := turnPrompt(messages)
	if err != nil {
		return "", err
	}
	switch policy.Mode {
	case "required":
		prompt += "\n\nCall at least one provided function tool before answering."
	case "named":
		prompt += fmt.Sprintf("\n\nCall the provided function tool %q before answering.", policy.Name)
	case "none":
		prompt += "\n\nDo not call function tools for this response."
	}
	return prompt, nil
}
func resolveCWD(cwd string, roots []string) (string, error) {
	if !filepath.IsAbs(cwd) {
		return "", errors.New("working directory must be absolute")
	}
	resolved, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return "", err
	}
	if len(roots) == 0 {
		return resolved, nil
	}
	for _, root := range roots {
		root, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(root, resolved)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return resolved, nil
		}
	}
	return "", errors.New("working directory is outside allowed roots")
}
