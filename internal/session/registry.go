package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

type sessionState struct {
	gate chan struct{}
	// waiters is protected by registry.mu, including callers waiting for gate.
	waiters                              int
	process                              Session
	active                               *activeTurn
	current                              atomic.Pointer[activeTurn]
	history                              []transcriptMessage
	historyCount                         int
	historyHash, resumeID, cwd, toolHash string
	lastAccess                           time.Time
}

func (s *sessionState) persistedHistoryCount() int {
	if s.history != nil {
		return len(s.history)
	}
	return s.historyCount
}
func (s *sessionState) matchesHistoryPrefix(messages []transcriptMessage) bool {
	if s.history != nil {
		return hasPrefix(messages, s.history)
	}
	return s.historyCount > 0 && s.historyCount <= len(messages) && s.historyHash != "" && transcriptHash(messages[:s.historyCount]) == s.historyHash
}
func transcriptHash(messages []transcriptMessage) string {
	data, _ := json.Marshal(messages)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func (s *sessionState) stop() {
	s.current.Store(nil)
	if s.active != nil {
		s.active.cancel(context.Canceled)
	}
	if s.process != nil {
		_ = s.process.Close()
		s.process = nil
	}
	if s.active != nil {
		<-s.active.done
		s.active = nil
	}
}

type registry struct {
	mu       sync.Mutex
	sessions map[string]*sessionState
	ttl      time.Duration
	store    *sessionStore
	closed   bool
}

func newRegistry(ttl time.Duration, store *sessionStore, path string) (*registry, error) {
	if store == nil {
		var err error
		store, err = newSessionStore(path)
		if err != nil {
			return nil, err
		}
	}
	return &registry{sessions: map[string]*sessionState{}, ttl: ttl, store: store}, nil
}
func (r *registry) lock(ctx context.Context, key string) (*sessionState, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, &Error{Status: 503, Code: "runtime_closed"}
	}
	s := r.sessions[key]
	if s == nil {
		s = &sessionState{gate: make(chan struct{}, 1), lastAccess: time.Now()}
		if saved, ok := r.store.get(key); ok {
			s.resumeID, s.cwd, s.toolHash = saved.ResumeID, saved.CWD, saved.ToolHash
			s.historyCount, s.historyHash = saved.HistoryCount, saved.HistoryHash
		}
		r.sessions[key] = s
	}
	s.waiters++
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		s.waiters--
		r.mu.Unlock()
	}()
	select {
	case s.gate <- struct{}{}:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *sessionState) unlock() { <-s.gate }
func (r *registry) evictIdle() {
	r.store.prune()
	if r.ttl <= 0 {
		return
	}
	r.mu.Lock()
	states := make(map[string]*sessionState, len(r.sessions))
	for key, s := range r.sessions {
		states[key] = s
	}
	r.mu.Unlock()
	for key, s := range states {
		r.mu.Lock()
		if r.sessions[key] != s || s.waiters > 0 {
			r.mu.Unlock()
			continue
		}
		select {
		case s.gate <- struct{}{}:
			r.mu.Unlock()
			if time.Since(s.lastAccess) > r.ttl {
				// A pending tool exchange stays alive until the turn deadline. Idle TTL
				// only releases completed native sessions, not client-side tool work.
				if s.active == nil {
					s.stop()
					s.history = nil
					// A waiter can arrive while Close is stopping the native session.
					// Keep its exact state until that waiter has acquired the gate;
					// otherwise a new caller could create a parallel session for key.
					r.mu.Lock()
					if r.sessions[key] == s && s.waiters == 0 {
						delete(r.sessions, key)
					}
					r.mu.Unlock()
				}
			}
			s.unlock()
		default:
			r.mu.Unlock()
		}
	}
}
func (r *registry) close() {
	r.mu.Lock()
	r.closed = true
	states := make([]*sessionState, 0, len(r.sessions))
	for _, s := range r.sessions {
		states = append(states, s)
	}
	r.sessions = make(map[string]*sessionState)
	r.mu.Unlock()
	for _, s := range states {
		s.gate <- struct{}{}
		s.stop()
		s.history = nil
		s.unlock()
	}
}
