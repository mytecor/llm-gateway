package policy

import (
	"time"
)

type AdmissionConfig struct {
	MaxInFlight int
	MaxPending  int
	WaitTimeout time.Duration
}

// AdmissionRule declares the admission and queueing policy on a route.
// It bounds concurrent in-flight requests and sets queueing limits for pending requests.
type AdmissionRule struct {
	RuleBase
	MaxInFlight int      `json:"max_in_flight,omitempty"`
	MaxPending  *int     `json:"max_pending,omitempty"`
	WaitTimeout Duration `json:"wait_timeout,omitempty"`
}

func (r *AdmissionRule) apply(ctx *stageContext) error {
	maxInFlight := r.MaxInFlight

	maxPending := -1
	if r.MaxPending != nil {
		maxPending = *r.MaxPending
	}
	waitTimeout := r.WaitTimeout.Duration

	if maxInFlight < 0 {
		return ctx.errf("admission max_in_flight must not be negative")
	}
	if maxPending < -1 {
		return ctx.errf("admission max_pending must not be negative")
	}
	if waitTimeout < 0 {
		return ctx.errf("admission wait_timeout must not be negative")
	}
	if waitTimeout == 0 {
		waitTimeout = 30 * time.Second
	}
	if maxPending < 0 {
		if maxInFlight > 0 {
			maxPending = 4 * maxInFlight
		} else {
			maxPending = 16
		}
	}
	ctx.plan.Admission = AdmissionConfig{
		MaxInFlight: maxInFlight,
		MaxPending:  maxPending,
		WaitTimeout: waitTimeout,
	}
	return nil
}
