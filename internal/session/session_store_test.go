package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testCheckpoint() persistedSession {
	return persistedSession{ResumeID: "native", CWD: "/tmp", HistoryCount: 2, HistoryHash: "hash"}
}

func TestCheckpointStoreRetentionAndLimit(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "file"}[disk], func(t *testing.T) {
			path := ""
			if disk {
				path = filepath.Join(t.TempDir(), "sessions.json")
			}
			store, err := NewStoreWithOptions(path, StoreOptions{Retention: time.Hour, MaxRecords: 2})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().Add(-30 * time.Minute)
			store.now = func() time.Time { return now }
			for _, key := range []string{"oldest", "middle", "newest"} {
				if err := store.put(key, testCheckpoint()); err != nil {
					t.Fatal(err)
				}
				now = now.Add(time.Minute)
			}
			if len(store.records) != 2 {
				t.Fatalf("checkpoint count = %d, want 2", len(store.records))
			}
			if _, ok := store.get("oldest"); ok {
				t.Fatal("retained checkpoint beyond count limit")
			}
			if saved, ok := store.get("newest"); !ok || saved.UpdatedAt.IsZero() {
				t.Fatal("missing newest checkpoint or timestamp")
			}
			if disk {
				reloaded, err := NewStoreWithOptions(path, StoreOptions{Retention: time.Hour, MaxRecords: 2})
				if err != nil || len(reloaded.records) != 2 {
					t.Fatalf("reload: %v, store=%+v", err, reloaded)
				}
			}
			now = now.Add(time.Hour)
			store.prune()
			if len(store.records) != 0 {
				t.Fatalf("retained expired checkpoints: %v", store.records)
			}
		})
	}
}

func TestCheckpointLookupExpiresWithoutSweeping(t *testing.T) {
	store, err := NewStoreWithOptions("", StoreOptions{Retention: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	store.now = func() time.Time { return now }
	if err := store.put("key", testCheckpoint()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if _, ok := store.get("key"); ok || len(store.records) != 0 {
		t.Fatal("expired metadata remained available")
	}
}

func TestCheckpointStoreLoadsVersionTwoWithStableAge(t *testing.T) {
	for _, age := range []time.Duration{time.Hour, 8 * 24 * time.Hour} {
		t.Run(age.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sessions.json")
			data, err := json.Marshal(persistedSessions{Version: 2, Sessions: map[string]persistedSession{"key": testCheckpoint()}})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			modified := time.Now().Add(-age).Truncate(time.Second)
			if err := os.Chtimes(path, modified, modified); err != nil {
				t.Fatal(err)
			}
			store, err := NewStore(path)
			if err != nil {
				t.Fatal(err)
			}
			saved, ok := store.get("key")
			if age < DefaultCheckpointRetention {
				if !ok || !saved.UpdatedAt.Equal(modified) {
					t.Fatalf("legacy checkpoint age changed: %+v", saved)
				}
			} else if ok {
				t.Fatal("expired legacy checkpoint restored")
			}
		})
	}
}

func TestCheckpointStoreRejectsNegativeBounds(t *testing.T) {
	for _, opts := range []StoreOptions{{Retention: -time.Second}, {MaxRecords: -1}} {
		if _, err := NewStoreWithOptions("", opts); err == nil {
			t.Fatalf("accepted invalid bounds %+v", opts)
		}
	}
}

func TestCheckpointStorePrunesExistingFileToConfiguredLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	records := make(map[string]persistedSession)
	for i, key := range []string{"old", "middle", "new"} {
		record := testCheckpoint()
		record.UpdatedAt = time.Now().Add(time.Duration(i-3) * time.Minute)
		records[key] = record
	}
	data, err := json.Marshal(persistedSessions{Version: 3, Sessions: records})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	store, err := NewStoreWithOptions(path, StoreOptions{MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.get("new"); !ok || len(store.records) != 1 {
		t.Fatal("load retained too many or discarded the newest checkpoint")
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved persistedSessions
	if err := json.Unmarshal(data, &saved); err != nil || len(saved.Sessions) != 1 {
		t.Fatalf("disk checkpoint file was not bounded: %s (%v)", data, err)
	}
}
