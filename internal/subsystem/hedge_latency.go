package subsystem

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/mytecor/llm-gateway/internal/telemetry"
)

const (
	hedgeWarmup    = 20
	hedgeRetention = 5 * time.Minute
)

type hedgeProfileKey struct {
	provider, native string
	stream           bool
}

type hedgeProfile struct {
	last      time.Time
	warmup    [hedgeWarmup]time.Duration
	count     int
	threshold time.Duration
}

// HedgeLatencyStore learns response-selection p95 separately from balance's
// provider-wide EWMA. It uses bounded per-profile state and no background worker.
// Incomplete observations can classify a threshold exceedance without pretending
// that cancellation or a timeout was a successful response.
type HedgeLatencyStore struct {
	mu        sync.Mutex
	now       func() time.Time
	metrics   *telemetry.Metrics
	profiles  map[hedgeProfileKey]*hedgeProfile
	nextSweep time.Time
}

func NewHedgeLatencyStore(now func() time.Time, metrics *telemetry.Metrics) *HedgeLatencyStore {
	if now == nil {
		now = time.Now
	}
	return &HedgeLatencyStore{now: now, metrics: metrics, profiles: make(map[hedgeProfileKey]*hedgeProfile)}
}

// HedgeObservation is one dispatch's immutable threshold snapshot. Completion,
// loser cancellation and deadline handling may race; only the first report wins.
type HedgeObservation struct {
	once      sync.Once
	store     *HedgeLatencyStore
	key       hedgeProfileKey
	profile   *hedgeProfile
	Threshold time.Duration // zero means this dispatch is still warming up
}

func (s *HedgeLatencyStore) remove(key hedgeProfileKey) {
	delete(s.profiles, key)
	if s.metrics != nil {
		s.metrics.ObserveHedgeDelay(key.provider, key.native, key.stream, 0)
	}
}

// Begin snapshots the threshold after permits have been obtained. Stale profiles
// are evicted lazily, including profiles belonging to targets no longer routed.
func (s *HedgeLatencyStore) Begin(provider, native string, stream bool) *HedgeObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if !now.Before(s.nextSweep) {
		for key, profile := range s.profiles {
			if now.Sub(profile.last) >= hedgeRetention {
				s.remove(key)
			}
		}
		s.nextSweep = now.Add(time.Minute)
	}
	key := hedgeProfileKey{provider, native, stream}
	profile := s.profiles[key]
	if profile != nil && now.Sub(profile.last) >= hedgeRetention {
		s.remove(key)
		profile = nil
	}
	if profile == nil {
		profile = &hedgeProfile{last: now}
		s.profiles[key] = profile
	}
	return &HedgeObservation{store: s, key: key, profile: profile, Threshold: profile.threshold}
}

// Success records an exact TTFT (streaming) or response duration (non-streaming).
func (o *HedgeObservation) Success(elapsed time.Duration) { o.observe(elapsed, false) }

// Censor records a timeout or race-loser cancellation: only an elapsed duration
// strictly above the dispatch's threshold reveals which side of p95 it lies on.
func (o *HedgeObservation) Censor(elapsed time.Duration) { o.observe(elapsed, true) }

// Discard makes client cancellations and unrelated failures permanently neutral.
func (o *HedgeObservation) Discard() { o.once.Do(func() {}) }

func (o *HedgeObservation) observe(elapsed time.Duration, censored bool) {
	o.once.Do(func() {
		if elapsed <= 0 || (censored && (o.Threshold <= 0 || elapsed <= o.Threshold)) {
			return
		}
		s := o.store
		s.mu.Lock()
		defer s.mu.Unlock()
		profile := s.profiles[o.key]
		if profile != o.profile {
			return
		} // a newer generation replaced this attempt
		now := s.now()
		if now.Sub(profile.last) >= hedgeRetention {
			s.remove(o.key)
			return
		}
		if profile.threshold == 0 {
			if censored {
				return
			}
			profile.warmup[profile.count] = elapsed
			profile.count++
			profile.last = now
			if profile.count < hedgeWarmup {
				return
			}
			sort.Slice(profile.warmup[:], func(i, j int) bool { return profile.warmup[i] < profile.warmup[j] })
			profile.threshold = profile.warmup[18] // ceil(0.95 * 20), one-based
		} else {
			// An attempt launched during warmup has no comparable threshold snapshot.
			if o.Threshold <= 0 {
				return
			}
			overdue := 0.0
			if elapsed > o.Threshold {
				overdue = 1
			}
			value := float64(profile.threshold) * math.Exp(0.1*(overdue-0.05))
			const maxDuration = time.Duration(1<<63 - 1)
			switch {
			case value >= float64(maxDuration):
				profile.threshold = maxDuration
			case value < 1:
				profile.threshold = time.Nanosecond
			default:
				profile.threshold = time.Duration(value)
			}
			profile.last = now
		}
		if s.metrics != nil {
			s.metrics.ObserveHedgeDelay(o.key.provider, o.key.native, o.key.stream, profile.threshold)
		}
	})
}
