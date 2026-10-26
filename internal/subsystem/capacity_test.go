package subsystem

import (
	"bytes"
	"strings"
	"sync"
	"testing"

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
