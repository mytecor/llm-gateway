package subsystem

import (
	"math"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
)

func TestScoreHistoryBoundedWithoutBalanceReads(t *testing.T) {
	now := time.Unix(1000000, 0)
	store := NewScoreStore(func() time.Time { return now }, NewProviderCapacity(nil))
	for range 100000 {
		store.Observe("busy", nil, time.Second)
	}
	score := store.providers["busy"]
	if len(score.buckets) > maxScoreBuckets {
		t.Fatalf("history grew with request count: %d buckets", len(score.buckets))
	}
	var total uint64
	for _, bucket := range score.buckets {
		total += bucket.total
	}
	if total != 100000 {
		t.Fatalf("bucket aggregation lost samples: %d", total)
	}
	initialBuckets := len(score.buckets)
	for range 3600 {
		now = now.Add(time.Second)
		store.Observe("busy", nil, time.Second)
	}
	if len(score.buckets) != initialBuckets {
		t.Fatal("history grew with uptime without a balance reader")
	}
	if got := store.Health("busy", time.Minute, 0.2); got != 1 {
		t.Fatalf("healthy traffic after ring rotations: %v", got)
	}
}

func TestScoreWindowsDoNotDiscardEachOthersEvidence(t *testing.T) {
	now := time.Unix(1000000, 0)
	store := NewScoreStoreWithOptions(func() time.Time { return now }, NewProviderCapacity(nil), ScoreStoreOptions{MaxWindow: 10 * time.Minute})
	store.Observe("a", &model.CallError{Class: model.ErrorTimeout}, 0)
	now = now.Add(8 * time.Minute)
	store.Observe("a", nil, time.Second)
	for range 2 {
		if got := store.Health("a", time.Minute, 0.9); got != 1 {
			t.Fatalf("short window contains expired error: %v", got)
		}
		want := 1 - 0.5/0.9
		if got := store.Health("a", 10*time.Minute, 0.9); math.Abs(got-want) > 1e-9 {
			t.Fatalf("short window discarded long-window history: got %v, want %v", got, want)
		}
	}
}

func TestScoreRetentionExpiresWithoutReads(t *testing.T) {
	now := time.Unix(1000000, 0)
	store := NewScoreStore(func() time.Time { return now }, NewProviderCapacity(nil))
	store.Observe("a", &model.CallError{Class: model.ErrorTimeout}, 0)
	now = now.Add(store.retention + store.bucketWidth)
	store.Observe("a", nil, time.Second)
	if got := store.Health("a", store.retention, 0.2); got != 1 {
		t.Fatalf("expired evidence survived retention: %v", got)
	}
	now = now.Add(store.retention)
	if got := store.Health("a", store.retention, 0.2); got != 1 {
		t.Fatalf("empty window health: %v", got)
	}
}

func TestScoreLongRetentionStillHasFixedBucketLimit(t *testing.T) {
	now := time.Unix(1000000, 0)
	store := NewScoreStoreWithOptions(func() time.Time { return now }, NewProviderCapacity(nil), ScoreStoreOptions{MaxWindow: 24 * time.Hour})
	for range 2000 {
		store.Observe("a", nil, time.Second)
		now = now.Add(time.Minute)
	}
	if store.retention != 24*time.Hour || len(store.providers["a"].buckets) > maxScoreBuckets {
		t.Fatalf("long-window history is not bounded: retention=%s buckets=%d", store.retention, len(store.providers["a"].buckets))
	}
}
