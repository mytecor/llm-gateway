package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type persistedSession struct {
	ResumeID     string    `json:"resume_id"`
	CWD          string    `json:"cwd"`
	HistoryCount int       `json:"history_count"`
	HistoryHash  string    `json:"history_hash"`
	ToolHash     string    `json:"tool_hash,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
}

type persistedSessions struct {
	Version  int                         `json:"version"`
	Sessions map[string]persistedSession `json:"sessions"`
}

// Store persists completed session checkpoints across gateway restarts and can
// be shared safely across multiple stateful provider runtimes.
type Store = sessionStore

// StoreOptions bounds completed checkpoint metadata independently of native
// process idle TTL. Zero values use seven days and 10,000 checkpoints.
type StoreOptions struct {
	Retention  time.Duration
	MaxRecords int
}

const (
	DefaultCheckpointRetention = 7 * 24 * time.Hour
	DefaultCheckpointLimit     = 10000
)

type sessionStore struct {
	mu         sync.Mutex
	path       string
	records    map[string]persistedSession
	retention  time.Duration
	maxRecords int
	now        func() time.Time
}

func NewStore(path string) (*Store, error) {
	return newSessionStore(path)
}

func NewStoreWithOptions(path string, opts StoreOptions) (*Store, error) {
	return newSessionStoreWithOptions(path, opts)
}

func newSessionStore(path string) (*sessionStore, error) {
	return newSessionStoreWithOptions(path, StoreOptions{})
}

func newSessionStoreWithOptions(path string, opts StoreOptions) (*sessionStore, error) {
	if opts.Retention < 0 || opts.MaxRecords < 0 {
		return nil, errors.New("checkpoint retention and limit must not be negative")
	}
	if opts.Retention == 0 {
		opts.Retention = DefaultCheckpointRetention
	}
	if opts.MaxRecords == 0 {
		opts.MaxRecords = DefaultCheckpointLimit
	}
	s := &sessionStore{path: path, records: make(map[string]persistedSession), retention: opts.Retention, maxRecords: opts.MaxRecords, now: time.Now}
	if path == "" {
		return s, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, fmt.Errorf("read: %w", err)
	}
	var stored persistedSessions
	if err := json.Unmarshal(data, &stored); err != nil {
		return s, fmt.Errorf("decode: %w", err)
	}
	if stored.Version != 2 && stored.Version != 3 {
		return s, fmt.Errorf("unsupported version %d", stored.Version)
	}
	// Version 2 files have no checkpoint timestamps. The file modification
	// time gives them a stable age across restarts, without rewriting the file
	// merely because a resume was attempted.
	legacyTime := s.now()
	if info, err := os.Stat(path); err == nil {
		legacyTime = info.ModTime()
	}
	for key, record := range stored.Sessions {
		if key != "" && record.ResumeID != "" && filepath.IsAbs(record.CWD) && record.HistoryCount > 0 && record.HistoryHash != "" {
			if record.UpdatedAt.IsZero() {
				record.UpdatedAt = legacyTime
			}
			s.records[key] = record
		}
	}
	s.pruneLocked(s.now())
	if len(s.records) != len(stored.Sessions) {
		if err := s.writeLocked(); err != nil {
			return nil, fmt.Errorf("prune checkpoints: %w", err)
		}
	}
	return s, nil
}

func (s *sessionStore) get(key string) (persistedSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[key]
	if ok && !record.UpdatedAt.Add(s.retention).After(s.now()) {
		delete(s.records, key)
		return persistedSession{}, false
	}
	return record, ok
}

func (s *sessionStore) put(key string, record persistedSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record.UpdatedAt = s.now()
	s.records[key] = record
	s.pruneLocked(record.UpdatedAt)
	return s.writeLocked()
}

func (s *sessionStore) delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[key]; !ok {
		return nil
	}
	delete(s.records, key)
	return s.writeLocked()
}

// prune also runs during idle eviction, so expired in-memory metadata is
// collected even when the provider no longer receives completed turns.
func (s *sessionStore) prune() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(s.now())
}

func (s *sessionStore) pruneLocked(now time.Time) {
	for key, record := range s.records {
		if !record.UpdatedAt.Add(s.retention).After(now) {
			delete(s.records, key)
		}
	}
	if len(s.records) <= s.maxRecords {
		return
	}
	keys := make([]string, 0, len(s.records))
	for key := range s.records {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := s.records[keys[i]].UpdatedAt, s.records[keys[j]].UpdatedAt
		if a.Equal(b) {
			return keys[i] < keys[j]
		}
		return a.Before(b)
	})
	for _, key := range keys[:len(keys)-s.maxRecords] {
		delete(s.records, key)
	}
}

func (s *sessionStore) writeLocked() error {
	if s.path == "" {
		return nil
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}
	data, err := json.MarshalIndent(persistedSessions{Version: 3, Sessions: s.records}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".sessions-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("set permissions: %w", err)
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("replace: %w", err)
	}
	return nil
}
