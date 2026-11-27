package policy

import (
	"time"
)

type AdmissionConfig struct {
	ReserveInteractive int
	MaxInFlight        int
	MaxPending         int
	WaitTimeout        time.Duration
	Dynamic            bool
}

// AdmissionRule declares the admission and queueing policy on a route.
// It bounds concurrent in-flight requests and sets queueing limits for pending requests.
type AdmissionRule struct {
	RuleBase
	ReserveInteractive int      `json:"reserve_interactive,omitempty"`
	MaxInFlight        int      `json:"max_in_flight,omitempty"`
	MaxPending         *int     `json:"max_pending,omitempty"`
	WaitTimeout        Duration `json:"wait_timeout,omitempty"`
	Dynamic            bool     `json:"dynamic,omitempty"`
}

func (r *AdmissionRule) apply(ctx *stageContext) error {
	maxInFlight := r.MaxInFlight

	maxPending := -1
	if r.MaxPending != nil {
		maxPending = *r.MaxPending
	}
	waitTimeout := r.WaitTimeout.Duration

	if r.ReserveInteractive < 0 {
		return ctx.errf("reserve_interactive must not be negative")
	}
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
		MaxInFlight:        maxInFlight,
		ReserveInteractive: r.ReserveInteractive,
		MaxPending:         maxPending,
		WaitTimeout:        waitTimeout,
		Dynamic:            r.Dynamic,
	}
	return nil
}
