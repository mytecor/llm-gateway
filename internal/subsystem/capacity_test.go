package subsystem

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

func TestCapacityPermitReleaseIsIdempotentAndUpdatesMetrics(t *testing.T) {
	metrics := telemetry.NewMetrics()
	c := NewProviderCapacity(metrics)
	c.SetLimit("a", 1)
	permit := c.Acquire("a")
	if permit == nil || c.Acquire("a") != nil {
		t.Fatal("hard provider limit not enforced")
	}
	var before bytes.Buffer
	_ = metrics.WriteExposition(&before)
	if !strings.Contains(before.String(), `llm_requests_in_flight{provider="a"} 1`) {
		t.Fatal("metrics disagree with capacity")
	}
	changed := c.Changed()
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(permit.Release)
	}
	wg.Wait()
	select {
	case <-changed:
	default:
		t.Fatal("release did not wake capacity waiters")
	}
	var after bytes.Buffer
	_ = metrics.WriteExposition(&after)
	if c.Count("a") != 0 || !strings.Contains(after.String(), `llm_requests_in_flight{provider="a"} 0`) {
		t.Fatal("permit released more than once or metrics drifted")
	}
	next := c.Acquire("a")
	if next == nil {
		t.Fatal("released slot was unavailable")
	}
	next.Release()
}

func TestCapacityAIMD(t *testing.T) {
	c := NewProviderCapacity(nil)
	c.SetLimit("a", 8)
	if got := c.Limit("a"); got != 8 {
		t.Fatalf("initial limit = %d, want 8", got)
	}
	// Congestion halves effective limit down to floor 1.
	c.ObserveOutcome("a", &model.CallError{Class: model.ErrorRateLimit, Status: 429})
	if got := c.Limit("a"); got != 4 {
		t.Fatalf("after 429 limit = %d, want 4", got)
	}
	c.ObserveOutcome("a", &model.CallError{Class: model.ErrorTimeout, Status: 504})
	if got := c.Limit("a"); got != 2 {
		t.Fatalf("after 504 limit = %d, want 2", got)
	}
	// 4 consecutive successes gently recover +1 without exceeding configured ceiling.
	for range 4 {
		c.ObserveOutcome("a", nil)
	}
	if got := c.Limit("a"); got != 3 {
		t.Fatalf("after 4 successes limit = %d, want 3", got)
	}
}
