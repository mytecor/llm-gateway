package session

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestRegistryLoadsOnlyRequestedCheckpoint(t *testing.T) {
	store, err := NewStore("")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if err := store.put(fmt.Sprint(i), testCheckpoint()); err != nil {
			t.Fatal(err)
		}
	}
	r, err := newRegistry(time.Minute, store, "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if len(r.sessions) != 0 {
		t.Fatal("eagerly materialized completed checkpoints")
	}
	s, err := r.lock(context.Background(), "42")
	if err != nil {
		t.Fatal(err)
	}
	defer s.unlock()
	if len(r.sessions) != 1 || s.resumeID != "native" || s.historyCount != 2 {
		t.Fatalf("lazy checkpoint restore failed: %+v", s)
	}
}

func TestRegistryEvictionReclaimsEntriesAndResumes(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "file"}[disk], func(t *testing.T) {
			path := ""
			if disk {
				path = filepath.Join(t.TempDir(), "sessions.json")
			}
			engine := &fakeEngine{}
			r := newRuntime(t, engine, Config{StorePath: path})
			req := request("hello")
			first, err := r.Exchange(context.Background(), req, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, state := range r.registry.sessions {
				state.lastAccess = time.Now().Add(-time.Hour)
			}
			r.registry.evictIdle()
			if len(r.registry.sessions) != 0 || engine.closed != 1 {
				t.Fatalf("eviction retained runtime state: %d, native closes: %d", len(r.registry.sessions), engine.closed)
			}
			req.Messages = append(req.Messages, message("assistant", first.Text), message("user", "next"))
			if _, err := r.Exchange(context.Background(), req, nil); err != nil {
				t.Fatal(err)
			}
			if len(engine.specs) != 2 || engine.specs[1].ResumeID != "native-session" || engine.prompts[1] != "next" {
				t.Fatalf("idle eviction lost resumable metadata: %+v, %v", engine.specs, engine.prompts)
			}
		})
	}
}

type blockingCloseSession struct {
	entered chan struct{}
	finish  chan struct{}
}

func (s *blockingCloseSession) Run(context.Context, string, func(Event) error) error { return nil }
func (s *blockingCloseSession) Close() error {
	close(s.entered)
	<-s.finish
	return nil
}

func TestRegistryEvictionKeepsStateForQueuedWaiter(t *testing.T) {
	r, err := newRegistry(time.Minute, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	s, err := r.lock(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	process := &blockingCloseSession{entered: make(chan struct{}), finish: make(chan struct{})}
	defer func() {
		select {
		case <-process.finish:
		default:
			close(process.finish)
		}
	}()
	s.process = process
	s.lastAccess = time.Now().Add(-time.Hour)
	s.unlock()
	evicted := make(chan struct{})
	go func() { r.evictIdle(); close(evicted) }()
	<-process.entered
	acquired := make(chan *sessionState, 1)
	go func() {
		state, err := r.lock(context.Background(), "key")
		if err == nil {
			acquired <- state
		}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		r.mu.Lock()
		queued := s.waiters == 1
		r.mu.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	close(process.finish)
	<-evicted
	state := <-acquired
	defer state.unlock()
	if state != s || r.sessions["key"] != s {
		t.Fatal("eviction replaced state still owned by a queued waiter")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := r.lock(ctx, "key"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("parallel caller bypassed session gate: %v", err)
	}
	r.mu.Lock()
	waiters := s.waiters
	r.mu.Unlock()
	if waiters != 0 {
		t.Fatal("cancelled waiter retained its reservation")
	}
}

func TestRegistryEvictionPreservesPendingToolTurn(t *testing.T) {
	r := newRuntime(t, &fakeEngine{}, Config{})
	if _, err := r.Exchange(context.Background(), request("call"), nil); err != nil {
		t.Fatal(err)
	}
	for _, state := range r.registry.sessions {
		state.lastAccess = time.Now().Add(-time.Hour)
	}
	r.registry.evictIdle()
	if len(r.registry.sessions) != 1 {
		t.Fatal("evicted native session while tool results were pending")
	}
}
