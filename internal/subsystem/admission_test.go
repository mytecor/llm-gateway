package subsystem

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

func TestAdmissionControllerLimitsAndQueues(t *testing.T) {
	metrics := telemetry.NewMetrics()
	ac := NewAdmissionController(metrics, time.Now)
	admPolicy := policy.AdmissionConfig{
		MaxInFlight: 2,
		MaxPending:  2,
		WaitTimeout: 100 * time.Millisecond,
	}

	ctx := context.Background()
	rel1, err := ac.Admit(ctx, "standard", admPolicy, time.Time{})
	if err != nil {
		t.Fatalf("first admit failed: %v", err)
	}
	defer rel1()

	rel2, err := ac.Admit(ctx, "standard", admPolicy, time.Time{})
	if err != nil {
		t.Fatalf("second admit failed: %v", err)
	}
	defer rel2()

	// Third request must queue, then succeed when slot is released
	done := make(chan struct{})
	go func() {
		rel3, err := ac.Admit(ctx, "standard", admPolicy, time.Time{})
		if err != nil {
			t.Errorf("queued admit failed: %v", err)
			return
		}
		rel3()
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	rel1()

	select {
	case <-done:
		// Succeeded
	case <-time.After(time.Second):
		t.Fatal("queued request was not woken up after release")
	}
}

func TestAdmissionControllerRejectsOnQueueFull(t *testing.T) {
	ac := NewAdmissionController(telemetry.NewMetrics(), time.Now)
	admPolicy := policy.AdmissionConfig{
		MaxInFlight: 1,
		MaxPending:  1,
		WaitTimeout: 50 * time.Millisecond,
	}
	ctx := context.Background()
	rel1, err := ac.Admit(ctx, "standard", admPolicy, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	defer rel1()

	// Waiter 1 in queue
	go func() {
		_, _ = ac.Admit(ctx, "standard", admPolicy, time.Time{})
	}()
	time.Sleep(10 * time.Millisecond)

	// Waiter 2 exceeds maxPending=1 -> fast reject
	_, err = ac.Admit(ctx, "standard", admPolicy, time.Time{})
	if err == nil || err.Status != http.StatusTooManyRequests || err.Code != "route_queue_full" || err.RetryAfter <= 0 {
		t.Fatalf("expected 429 route_queue_full with RetryAfter, got %#v", err)
	}
}

func TestAdmissionWaitTimeoutIsRateLimitWithRetryAfter(t *testing.T) {
	ac := NewAdmissionController(telemetry.NewMetrics(), time.Now)
	admPolicy := policy.AdmissionConfig{
		MaxInFlight: 1,
		MaxPending:  1,
		WaitTimeout: 25 * time.Millisecond,
	}
	rel, callErr := ac.Admit(context.Background(), "standard", admPolicy, time.Time{})
	if callErr != nil {
		t.Fatal(callErr)
	}
	defer rel()

	_, callErr = ac.Admit(context.Background(), "standard", admPolicy, time.Time{})
	if callErr == nil || callErr.Class != model.ErrorRateLimit || callErr.Status != http.StatusTooManyRequests || callErr.Code != "route_queue_wait_timeout" || callErr.RetryAfter <= 0 {
		t.Fatalf("expected 429 route_queue_wait_timeout with RetryAfter, got %#v", callErr)
	}
}

func TestAdmissionControllerClientDisconnectUnblocksQueue(t *testing.T) {
	ac := NewAdmissionController(telemetry.NewMetrics(), time.Now)
	admPolicy := policy.AdmissionConfig{
		MaxInFlight: 1,
		MaxPending:  5,
		WaitTimeout: time.Second,
	}
	ctx := context.Background()
	rel1, err := ac.Admit(ctx, "standard", admPolicy, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	defer rel1()

	cancelCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan *model.CallError, 1)
	go func() {
		_, err := ac.Admit(cancelCtx, "standard", admPolicy, time.Time{})
		errCh <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil || err.Class != model.ErrorCancelled {
			t.Fatalf("expected cancelled error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled context did not unblock admit")
	}
}

func TestAdmissionWaitConsumesLogicalDeadline(t *testing.T) {
	ac := NewAdmissionController(telemetry.NewMetrics(), time.Now)
	admPolicy := policy.AdmissionConfig{MaxInFlight: 1, MaxPending: 1, WaitTimeout: time.Second}
	release, callErr := ac.Admit(context.Background(), "standard", admPolicy, time.Time{})
	if callErr != nil {
		t.Fatal(callErr)
	}
	defer release()

	started := time.Now()
	_, callErr = ac.Admit(context.Background(), "standard", admPolicy, started.Add(30*time.Millisecond))
	if callErr == nil || callErr.Class != model.ErrorTimeout {
		t.Fatalf("expected logical deadline timeout while queued, got %#v", callErr)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("admission ignored logical deadline and waited %s", elapsed)
	}
}

func TestPriorityHandoffAndReserve(t *testing.T) {
	for _, limit := range []int{1, 2} {
		ac := NewAdmissionController(nil, nil)
		p := policy.AdmissionConfig{MaxInFlight: limit, ReserveInteractive: 1, MaxPending: 4, WaitTimeout: 2 * time.Second}
		admit := func(priority model.Priority) (func(), *model.CallError) {
			return ac.Admit(model.WithPriority(t.Context(), priority), "test", p, time.Time{})
		}
		held, err := admit(model.PriorityBackground)
		if err != nil {
			t.Fatal(err)
		}
		type granted struct {
			p       model.Priority
			release func()
		}
		grants := make(chan granted, 3)
		queue := func(priority model.Priority, count int) {
			go func() {
				rel, err := admit(priority)
				if err != nil {
					return
				}
				grants <- granted{priority, rel}
			}()
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				ac.mu.Lock()
				n := ac.routes["test"].pending
				ac.mu.Unlock()
				if n == count {
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Fatal("request did not queue", priority)
		}
		queue(model.PriorityBackground, 1)
		var reserved func()
		if limit == 2 {
			reserved, err = admit(model.PriorityInteractive)
			if err != nil {
				t.Fatal(err)
			}
			defer reserved()
		}
		queue(model.PriorityNormal, 2)
		queue(model.PriorityInteractive, 3)
		held()
		for _, want := range []model.Priority{model.PriorityInteractive, model.PriorityNormal, model.PriorityBackground} {
			// At limit two a background request needs both reserved and occupied slots free.
			if limit == 2 && want == model.PriorityBackground {
				ac.mu.Lock()
				count := ac.routes["test"].inFlight
				ac.mu.Unlock()
				if count != 1 {
					t.Fatalf("expected background waiter to leave the reserved slot free, in-flight=%d", count)
				}
				reserved()
			}
			select {
			case g := <-grants:
				if g.p != want {
					t.Fatalf("got %v before %v", g.p, want)
				}
				g.release()
			case <-time.After(time.Second):
				t.Fatalf("no grant for %v", want)
			}
		}
	}
}
