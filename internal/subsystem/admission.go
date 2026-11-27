package subsystem

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

type AdmissionController struct {
	mu      sync.Mutex
	routes  map[string]*admissionQueue
	metrics *telemetry.Metrics
	now     func() time.Time
}

type admissionQueue struct {
	inFlight int
	limit    int
	reserve  int
	pending  int
	waiters  [3][]chan struct{}
}

func NewAdmissionController(metrics *telemetry.Metrics, now func() time.Time) *AdmissionController {
	if now == nil {
		now = time.Now
	}
	return &AdmissionController{
		routes:  make(map[string]*admissionQueue),
		metrics: metrics,
		now:     now,
	}
}

func (ac *AdmissionController) Admit(ctx context.Context, route string, policy policy.AdmissionConfig, deadline time.Time) (func(), *model.CallError) {
	if policy.MaxInFlight <= 0 {
		return func() {}, nil
	}
	ac.mu.Lock()
	q, ok := ac.routes[route]
	if !ok {
		q = &admissionQueue{}
		ac.routes[route] = q
	}
	q.limit = policy.MaxInFlight

	q.reserve = policy.ReserveInteractive
	priority := model.RequestPriority(ctx)
	ac.dispatch(q, route, priority)
	if q.available(priority) {
		q.inFlight++
		ac.dispatch(q, route, model.PriorityBackground)
		if ac.metrics != nil {
			ac.metrics.ObserveRouteInFlight(route, q.inFlight)
		}
		ac.mu.Unlock()
		return ac.makeReleaser(route), nil
	}

	// Queue check
	if policy.MaxPending <= 0 || q.pending >= policy.MaxPending {
		ac.mu.Unlock()
		if ac.metrics != nil {
			ac.metrics.ObserveRouteQueueRejection(route, "max_pending")
		}
		return nil, &model.CallError{
			Code:       "route_queue_full",
			Class:      model.ErrorRateLimit,
			Status:     http.StatusTooManyRequests,
			RetryAfter: time.Second,
			Cause:      fmt.Errorf("route %q queue is full (max_pending=%d)", route, policy.MaxPending),
		}
	}

	ch := make(chan struct{}, 1)
	q.waiters[priority] = append(q.waiters[priority], ch)
	q.pending++
	waitStart := ac.now()
	if ac.metrics != nil {
		ac.metrics.ObserveRoutePending(route, priority.String(), len(q.waiters[priority]))
	}
	ac.mu.Unlock()

	waitTimeout := policy.WaitTimeout
	if waitTimeout <= 0 {
		waitTimeout = 30 * time.Second
	}
	deadlineLimited := false
	if !deadline.IsZero() {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			ac.mu.Lock()
			ac.removeWaiter(q, route, priority, ch)
			ac.mu.Unlock()
			return nil, model.RouteTimeoutError()
		}
		if remaining < waitTimeout {
			waitTimeout = remaining
			deadlineLimited = true
		}
	}
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()

	select {
	case <-ch:
		if ac.metrics != nil {
			ac.metrics.ObserveRouteQueueWait(route, priority.String(), ac.now().Sub(waitStart))
		}
		return ac.makeReleaser(route), nil

	case <-ctx.Done():
		ac.mu.Lock()
		ac.removeWaiter(q, route, priority, ch)
		ac.mu.Unlock()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, model.RouteTimeoutError()
		}
		return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}

	case <-timer.C:
		ac.mu.Lock()
		ac.removeWaiter(q, route, priority, ch)
		ac.mu.Unlock()
		if deadlineLimited {
			return nil, model.RouteTimeoutError()
		}
		if ac.metrics != nil {
			ac.metrics.ObserveRouteQueueRejection(route, "wait_timeout")
		}
		return nil, &model.CallError{
			Code:       "route_queue_wait_timeout",
			Class:      model.ErrorRateLimit,
			Status:     http.StatusTooManyRequests,
			RetryAfter: 3 * time.Second,
			Cause:      fmt.Errorf("route %q queue wait timeout (%s) exceeded", route, waitTimeout),
		}
	}
}

func (ac *AdmissionController) removeWaiter(q *admissionQueue, route string, priority model.Priority, target chan struct{}) {
	for i, w := range q.waiters[priority] {
		if w == target {
			q.waiters[priority] = slices.Delete(q.waiters[priority], i, i+1)
			q.pending--
			break
		}
	}
	if ac.metrics != nil {
		ac.metrics.ObserveRoutePending(route, priority.String(), len(q.waiters[priority]))
	}
	// If a slot was sent to target right before cancel/timeout, pass it along
	select {
	case <-target:
		ac.releaseSlotLocked(q, route)
	default:
	}
}

func (ac *AdmissionController) releaseSlotLocked(q *admissionQueue, route string) {
	q.inFlight--
	ac.dispatch(q, route, model.PriorityBackground)
	ac.metrics.ObserveRouteInFlight(route, q.inFlight)
}

func (q *admissionQueue) available(p model.Priority) bool {
	limit := q.limit
	if p == model.PriorityBackground && limit > q.reserve {
		limit -= q.reserve
	}
	return q.inFlight < limit
}

func (ac *AdmissionController) dispatch(q *admissionQueue, route string, minimum model.Priority) {
	for p := model.PriorityInteractive; p >= minimum; p-- {
		for len(q.waiters[p]) > 0 && q.available(p) {
			next := q.waiters[p][0]
			q.waiters[p][0] = nil
			q.waiters[p] = q.waiters[p][1:]
			q.pending--
			q.inFlight++
			ac.metrics.ObserveRoutePending(route, p.String(), len(q.waiters[p]))
			ac.metrics.ObserveRouteInFlight(route, q.inFlight)
			next <- struct{}{}
		}
	}
}

func (ac *AdmissionController) makeReleaser(route string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			ac.mu.Lock()
			defer ac.mu.Unlock()
			q, ok := ac.routes[route]
			if !ok {
				return
			}
			ac.releaseSlotLocked(q, route)
		})
	}
}
