package session

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

// Exchange holds the conversation gate across private native turns. Its owner
// must serialize Run/Commit/Abort, and must terminate it before closing Runtime.
// Only the accepted public transcript is checkpointed; native history stays in
// the agent/browser session addressed by resumeID.
type Exchange struct {
	mu       sync.Mutex
	runtime  *Runtime
	state    *sessionState
	key      string
	messages []transcriptMessage
	turns    int
	done     bool
}

func (r *Runtime) identity(req Request) (string, string, error) {
	cwd := strings.TrimSpace(req.CWD)
	if cwd == "" {
		cwd = r.config.DefaultCWD
	}
	cwd, err := resolveCWD(cwd, r.config.AllowedRoots)
	if err != nil {
		return "", "", invalid("invalid_cwd", err)
	}
	parts := []string{req.Principal, req.Conversation, req.LogicalModel, req.Model, req.ReasoningEffort}
	if r.config.Provider != "" {
		parts = append(parts, r.config.Provider)
	}
	key, _ := json.Marshal(parts)
	return cwd, string(key), nil
}

func (r *Runtime) Begin(ctx context.Context, req Request) (*Exchange, error) {
	if req.Conversation == "" {
		return nil, invalid("session_required", errors.New("conversation ID is required"))
	}
	messages, err := normalizeMessages(req.Messages)
	if err != nil {
		return nil, invalid("invalid_messages", err)
	}
	_, key, err := r.identity(req)
	if err != nil {
		return nil, err
	}
	lockCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer cancel()
	defer stop()
	state, err := r.registry.lock(lockCtx, key)
	if err != nil {
		return nil, err
	}
	if err = r.ctx.Err(); err != nil {
		state.unlock()
		return nil, err
	}
	return &Exchange{runtime: r, state: state, key: key, messages: messages}, nil
}

func (x *Exchange) Commit(content string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.done {
		return nil
	}
	if x.state.active != nil || x.turns == 0 {
		return errors.New("exchange has no completed response")
	}
	s := x.state
	s.history = append(append([]transcriptMessage(nil), x.messages...), transcriptMessage{Role: "assistant", Content: content})
	s.historyCount, s.historyHash = len(s.history), transcriptHash(s.history)
	s.lastAccess = time.Now()
	if s.resumeID != "" {
		if err := x.runtime.registry.store.put(x.key, persistedSession{ResumeID: s.resumeID, CWD: s.cwd, HistoryCount: s.historyCount, HistoryHash: s.historyHash, ToolHash: s.toolHash}); err != nil {
			return err
		}
	}
	x.done = true
	s.unlock()
	return nil
}

func (x *Exchange) Abort() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.done {
		return nil
	}
	err := x.runtime.clear(x.state, x.key)
	x.done = true
	x.state.unlock()
	return err
}
