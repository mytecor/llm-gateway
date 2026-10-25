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
	gate                                 chan struct{}
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
}

func newRegistry(ttl time.Duration, store *sessionStore, path string) (*registry, error) {
	if store == nil {
		var err error
		store, err = newSessionStore(path)
		if err != nil {
			return nil, err
		}
	}
	r := &registry{sessions: map[string]*sessionState{}, ttl: ttl, store: store}
	for key, saved := range store.recordsSnapshot() {
		r.sessions[key] = &sessionState{gate: make(chan struct{}, 1), resumeID: saved.ResumeID, cwd: saved.CWD, historyCount: saved.HistoryCount, historyHash: saved.HistoryHash, toolHash: saved.ToolHash, lastAccess: time.Now()}
	}
	return r, nil
}
func (r *registry) lock(ctx context.Context, key string) (*sessionState, error) {
	r.mu.Lock()
	s := r.sessions[key]
	if s == nil {
		s = &sessionState{gate: make(chan struct{}, 1), lastAccess: time.Now()}
		r.sessions[key] = s
	}
	r.mu.Unlock()
	select {
	case s.gate <- struct{}{}:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *sessionState) unlock() { <-s.gate }
func (r *registry) evictIdle() {
	if r.ttl <= 0 {
		return
	}
	r.mu.Lock()
	states := make([]*sessionState, 0, len(r.sessions))
	for _, s := range r.sessions {
		states = append(states, s)
	}
	r.mu.Unlock()
	for _, s := range states {
		select {
		case s.gate <- struct{}{}:
			if time.Since(s.lastAccess) > r.ttl {
				// A pending tool exchange stays alive until the turn deadline. Idle TTL
				// only releases completed native sessions, not client-side tool work.
				if s.active == nil {
					s.stop()
					s.history = nil
				}
			}
			s.unlock()
		default:
		}
	}
}
func (r *registry) close() {
	r.mu.Lock()
	states := make([]*sessionState, 0, len(r.sessions))
	for _, s := range r.sessions {
		states = append(states, s)
	}
	r.mu.Unlock()
	for _, s := range states {
		s.gate <- struct{}{}
		s.stop()
		s.unlock()
	}
}
