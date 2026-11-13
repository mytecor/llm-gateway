package subsystem

import (
	"math"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

func (p *providerScore) mode(stream bool) *modeScore {
	if stream {
		return &p.stream
	}
	return &p.nonStream
}

func (p *providerScore) modeLatency(stream bool) (float64, bool) {
	ms := p.mode(stream)
	if ms.latencyOK && ms.latency > 0 {
		return ms.latency, true
	}
	// Fallback to untyped legacy observations only if no mode-specific samples exist yet.
	if p.legacy.latencyOK && p.legacy.latency > 0 {
		return p.legacy.latency, true
	}
	return 0, false
}

func sumWindowBuckets(buckets []scoreBucket, now, cutoff time.Time) (total, failures uint64) {
	for _, bucket := range buckets {
		if bucket.latest.After(cutoff) && !bucket.latest.After(now) {
			total += bucket.total
			failures += bucket.failures
		}
	}
	return total, failures
}

// LatencyOfMode returns the EWMA latency (in seconds) recorded for stream or
// non-stream requests against provider, falling back to the mode baseline or 1s.
func (s *ScoreStore) LatencyOfMode(provider string, stream bool) float64 {
	return s.latencyOfMode(provider, stream)
}

// KnownLatencyOfMode returns the observed EWMA latency for (provider, stream)
// only if real latency samples exist for that provider, avoiding synthetic defaults
// on cold-start providers.
func (s *ScoreStore) KnownLatencyOfMode(provider string, stream bool) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	score, ok := s.providers[provider]
	if !ok {
		return 0, false
	}
	lat, has := score.modeLatency(stream)
	if !has || lat <= 0 {
		return 0, false
	}
	return time.Duration(lat * float64(time.Second)), true
}

func (s *ScoreStore) latencyOfMode(provider string, stream bool) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if score, ok := s.providers[provider]; ok {
		if lat, ok := score.modeLatency(stream); ok {
			return lat
		}
	}
	maxLatency := 0.0
	haveBaseline := false
	for _, other := range s.providers {
		if lat, ok := other.modeLatency(stream); ok && lat > maxLatency {
			maxLatency = lat
			haveBaseline = true
		}
	}
	if haveBaseline {
		return maxLatency
	}
	return 1.0
}

// HealthForMode computes [0,1] health using observations from the given
// transport mode (stream vs non-stream).
func (s *ScoreStore) HealthForMode(provider string, stream bool, window time.Duration, errorBudget float64) float64 {
	return s.healthForMode(provider, stream, window, errorBudget)
}

// HealthyForMode reports whether provider is within its error budget for the
// given transport mode.
func (s *ScoreStore) HealthyForMode(provider string, stream bool, window time.Duration, errorBudget float64) bool {
	return s.healthForMode(provider, stream, window, errorBudget) > 0
}

func (s *ScoreStore) healthForMode(provider string, stream bool, window time.Duration, errorBudget float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	score, ok := s.providers[provider]
	if !ok {
		return 1
	}
	now := s.now()
	if window <= 0 || window > s.retention {
		window = s.retention
	}
	cutoff := now.Add(-window)
	total, failures := sumWindowBuckets(score.mode(stream).buckets, now, cutoff)
	if total == 0 {
		total, failures = sumWindowBuckets(score.legacy.buckets, now, cutoff)
	}
	if total == 0 {
		return 1
	}
	errorRate := float64(failures) / float64(total)
	if errorRate >= errorBudget {
		return 0
	}
	errorFactor := 1 - errorRate/errorBudget

	minLatency := math.Inf(1)
	haveBaseline := false
	for _, other := range s.providers {
		if lat, ok := other.modeLatency(stream); ok {
			minLatency = math.Min(minLatency, lat)
			haveBaseline = true
		}
	}
	latencyFactor := 1.0
	if provLat, ok := score.modeLatency(stream); ok && haveBaseline && minLatency > 0 && provLat > 0 {
		latencyFactor = minLatency / provLat
		if latencyFactor > 1 {
			latencyFactor = 1
		}
	}
	return errorFactor * latencyFactor
}

// SelectForMode reorders candidate targets using stream or non-stream statistics.
func (s *ScoreStore) SelectForMode(route string, targets []model.Target, policy policy.BalanceConfig, stream bool) []model.Target {
	if len(targets) < 2 {
		return targets
	}
	switch policy.Strategy {
	case "expected-ttft", "expected-completion":
		return s.selectExpectedForMode(targets, policy, true, stream)
	}
	healthy := s.healthyIndexesForMode(targets, policy, stream)
	if len(healthy) == 0 {
		return targets
	}
	switch policy.Strategy {
	case "p2c":
		return s.selectP2CForMode(targets, policy, healthy, stream)
	case "round_robin":
		return s.selectRoundRobin(route, targets, healthy)
	case "adaptive", "weighted":
		weights := make([]float64, len(healthy))
		for i, idx := range healthy {
			w := float64(baseWeight(targets[idx], policy))
			if policy.Strategy == "adaptive" {
				w *= s.healthForMode(targets[idx].Provider, stream, policy.Window, policy.ErrorBudget)
			}
			weights[i] = w
		}
		if pick := s.pickWeighted(healthy, weights); pick >= 0 {
			return promoteFront(targets, targets[healthy[pick]].Provider)
		}
	}
	return targets
}

func (s *ScoreStore) healthyIndexesForMode(targets []model.Target, policy policy.BalanceConfig, stream bool) []int {
	indexes := make([]int, 0, len(targets))
	for i := range targets {
		if s.healthForMode(targets[i].Provider, stream, policy.Window, policy.ErrorBudget) > 0 {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

func (s *ScoreStore) selectP2CForMode(targets []model.Target, policy policy.BalanceConfig, healthy []int, stream bool) []model.Target {
	if len(healthy) == 1 {
		return promoteFront(targets, targets[healthy[0]].Provider)
	}
	weights := make([]float64, len(healthy))
	for i, idx := range healthy {
		weights[i] = float64(baseWeight(targets[idx], policy)) * s.healthForMode(targets[idx].Provider, stream, policy.Window, policy.ErrorBudget)
	}
	first := s.pickWeighted(healthy, weights)
	if first < 0 {
		return targets
	}
	second := int(s.pick01() * float64(len(healthy)-1))
	if second >= first {
		second++
	}
	a, b := healthy[first], healthy[second]
	if s.capacity.Count(targets[a].Provider) <= s.capacity.Count(targets[b].Provider) {
		return promoteFront(targets, targets[a].Provider)
	}
	return promoteFront(targets, targets[b].Provider)
}
