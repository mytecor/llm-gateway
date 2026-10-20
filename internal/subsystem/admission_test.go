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
	if err == nil || err.Status != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %#v", err)
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
