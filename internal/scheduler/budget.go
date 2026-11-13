package scheduler

import (
	"time"

	"github.com/mytecor/llm-gateway/internal/policy"
)

// RequestBudget enforces request-wide safety and latency bounds across the
// entire execution graph of one client request (all tiers, retries, fallbacks,
// hedges and stream continuations). Owned by the scheduling goroutine.
type RequestBudget struct {
	deadline            time.Time
	maxPhysicalCalls    int
	maxInFlight         int
	maxHedges           int
	maxCallsPerProvider int
	calls               int
	hedges              int
	perProvider         map[string]int
}

func newRequestBudget(entry *policy.CompiledRoute, deadline time.Time) RequestBudget {
	budget := RequestBudget{
		deadline:    deadline,
		perProvider: make(map[string]int),
	}
	if entry != nil {
		budget.maxPhysicalCalls = entry.Semaphore.MaxCalls
		budget.maxInFlight = entry.Semaphore.MaxInFlight
		budget.maxHedges = entry.Semaphore.MaxHedges
		budget.maxCallsPerProvider = entry.Semaphore.MaxCallsPerProvider
	}
	return budget
}

func (b *RequestBudget) expired() bool {
	return !b.deadline.IsZero() && !time.Now().Before(b.deadline)
}

func (b *RequestBudget) callsExhausted() bool {
	return b.maxPhysicalCalls > 0 && b.calls >= b.maxPhysicalCalls
}

func (b *RequestBudget) boundedCalls() bool {
	return b != nil && (b.maxPhysicalCalls > 0 || b.maxCallsPerProvider > 0 || b.maxHedges > 0)
}

func (b *RequestBudget) canHedge() bool {
	if b.expired() || b.callsExhausted() {
		return false
	}
	return b.maxHedges <= 0 || b.hedges < b.maxHedges
}

func (b *RequestBudget) acquire(provider string, inFlight int, isHedge bool) bool {
	if b.callsExhausted() {
		return false
	}
	if b.maxInFlight > 0 && inFlight >= b.maxInFlight {
		return false
	}
	if b.maxCallsPerProvider > 0 && b.perProvider[provider] >= b.maxCallsPerProvider {
		return false
	}
	if isHedge && b.maxHedges > 0 && b.hedges >= b.maxHedges {
		return false
	}
	b.calls++
	b.perProvider[provider]++
	if isHedge {
		b.hedges++
	}
	return true
}

func (b *RequestBudget) hasCallBudget(entry *policy.CompiledRoute) bool {
	if b.callsExhausted() {
		return false
	}
	if b.maxCallsPerProvider <= 0 {
		return true
	}
	seen := make(map[*policy.CompiledRoute]bool)
	var available func(*policy.CompiledRoute) bool
	available = func(route *policy.CompiledRoute) bool {
		if route == nil || seen[route] {
			return false
		}
		seen[route] = true
		for _, target := range route.Pool {
			if b.perProvider[target.Provider] < b.maxCallsPerProvider {
				return true
			}
		}
		return available(route.RetryTarget) || available(route.FallbackTarget) || available(route.HedgeTarget)
	}
	return available(entry)
}
