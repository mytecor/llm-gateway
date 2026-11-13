package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

type halfOpenState struct {
	domain        string // "auth" or "inference"
	probeInFlight bool
	epoch         uint64
}

type halfOpenKeyLease struct {
	key    cooldownKey
	epoch  uint64
	domain string
}

type probeLease struct {
	active   bool
	provider string
	leases   []halfOpenKeyLease
}

// failureDomain classifies provider-facing errors into failure domains distinct
// from route admission queue overload.
func failureDomain(callErr *model.CallError) string {
	if callErr == nil || callErr.Class == model.ErrorCancelled {
		return ""
	}
	if isAuthBillingStatus(callErr.Status) {
		return "auth"
	}
	return "inference"
}

func isAuthBillingStatus(status int) bool {
	return status == 401 || status == 402 || status == 403
}

func halfOpenBusyError() *model.CallError {
	return &model.CallError{
		Class:  model.ErrorUpstream,
		Status: 503,
		Cause:  errors.New("provider half-open probe already in flight"),
	}
}

func matchesCooldownKey(key cooldownKey, provider, nativeModel string) bool {
	if key.provider != provider {
		return false
	}
	return key.model == "" || key.model == nativeModel
}

// promoteExpiredCoolingLocked transitions any expired cooldown entry into
// half-open state so at most one probe attempt is admitted next. Caller must hold r.mu.
func (r *Runner) promoteExpiredCoolingLocked(now time.Time) {
	for key, until := range r.cooling {
		if !now.Before(until) {
			delete(r.cooling, key)
			domain := "inference"
			if key.model == "" {
				domain = "auth"
			}
			r.probeSeq++
			r.halfOpen[key] = &halfOpenState{
				domain: domain,
				epoch:  r.probeSeq,
			}
		}
	}
}

// unavailableLocked reports whether (provider, model) is currently excluded by
// active cooldown OR an already running half-open recovery probe. Caller must hold r.mu.
func (r *Runner) unavailableLocked(providerID, nativeModel string, now time.Time) bool {
	if r.coolingLocked(providerID, nativeModel, now) {
		return true
	}
	for key, st := range r.halfOpen {
		if matchesCooldownKey(key, providerID, nativeModel) && st.probeInFlight {
			return true
		}
	}
	return false
}

func (r *Runner) poolHasHalfOpenProbeInFlight(pool []model.Target) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.promoteExpiredCoolingLocked(now)
	for _, target := range pool {
		for key, st := range r.halfOpen {
			if matchesCooldownKey(key, target.Provider, target.Model) && st.probeInFlight {
				return true
			}
		}
	}
	return false
}

func (r *Runner) targetEligibleLocked(providerID, nativeModel string, now time.Time) bool {
	return !r.unavailableLocked(providerID, nativeModel, now)
}

func (r *Runner) targetEligible(providerID, nativeModel string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.promoteExpiredCoolingLocked(now)
	return r.targetEligibleLocked(providerID, nativeModel, now)
}

// acquireHalfOpenProbe reserves the single half-open probe slot for (provider,
// nativeModel) when the pair is in half-open state. A running probe always blocks
// another attempt. allowFailOpen only permits bypassing an active cooldown when
// no other candidate or fallback exists; it never bypasses a reserved probe.
func (r *Runner) acquireHalfOpenProbe(ctx context.Context, providerID, nativeModel string, allowFailOpen bool) (probeLease, bool) {
	r.mu.Lock()
	now := r.now()
	r.promoteExpiredCoolingLocked(now)

	var matchedKeys []cooldownKey
	domain := "inference"
	for key := range r.cooling {
		if matchesCooldownKey(key, providerID, nativeModel) {
			if now.Before(r.cooling[key]) && !allowFailOpen {
				r.mu.Unlock()
				return probeLease{}, false
			}
		}
	}
	for key, st := range r.halfOpen {
		if !matchesCooldownKey(key, providerID, nativeModel) {
			continue
		}
		if st.probeInFlight {
			r.mu.Unlock()
			return probeLease{}, false
		}
		if !st.probeInFlight {
			matchedKeys = append(matchedKeys, key)
			if st.domain == "auth" {
				domain = "auth"
			}
		}
	}

	lease := probeLease{provider: providerID}
	for _, key := range matchedKeys {
		st := r.halfOpen[key]
		r.probeSeq++
		st.probeInFlight = true
		st.epoch = r.probeSeq
		lease.leases = append(lease.leases, halfOpenKeyLease{key: key, epoch: st.epoch, domain: st.domain})
	}
	lease.active = len(lease.leases) > 0
	r.mu.Unlock()

	if lease.active {
		r.metrics.ObserveHalfOpenProbe(providerID, domain, "admitted")
		telemetry.LogEvent(ctx, r.logger, slog.LevelInfo, "half_open_probe",
			"provider", providerID,
			"native_model", nativeModel,
			"domain", domain,
		)
	}
	return lease, true
}

// releaseHalfOpenProbe returns a half-open probe slot if the probe was aborted
// without reaching a terminal provider outcome (cancelled, loser, prefailed).
func (r *Runner) releaseHalfOpenProbe(lease probeLease) {
	if !lease.active {
		return
	}
	var releasedDomains []string
	r.mu.Lock()
	for _, kl := range lease.leases {
		if st, ok := r.halfOpen[kl.key]; ok && st.probeInFlight && st.epoch == kl.epoch {
			st.probeInFlight = false
			releasedDomains = append(releasedDomains, kl.domain)
		}
	}
	r.mu.Unlock()
	for _, d := range releasedDomains {
		r.metrics.ObserveHalfOpenProbe(lease.provider, d, "released")
	}
}

func (r *Runner) hasHealthyOrHalfOpenIdlePool(pool []model.Target) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.promoteExpiredCoolingLocked(now)
	for _, target := range pool {
		if !r.unavailableLocked(target.Provider, target.Model, now) {
			return true
		}
	}
	return false
}

// isRetryableProviderError mirrors policy.AllRetryableClasses for cooldown bookkeeping.
func isRetryableProviderError(callErr *model.CallError) bool {
	return callErr != nil && policy.AllRetryableClasses()[callErr.Class]
}
