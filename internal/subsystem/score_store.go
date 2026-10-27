package subsystem

import (
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// ScoreStore is the runtime provider-health state behind the balance action.
// It is the analogue of LeaseStore: memory-only, per-provider (globally
// across logical models) and fed from scheduler observations at the same
// points where cooldown and lease state are recorded. Health is a provider
// property, so the store is not scoped by logical model.
//
// For every completed upstream branch it tracks:
//   - an EWMA of the time to the first meaningful event (stream) or to the
//     successful response (non-stream), computed from winner latencies;
//   - a sliding window of results (success / health error), where a health
//     error is one of the balance-confirmed failure classes (429, 5xx,
//     timeout, connection_error).
//
// Health(p) ∈ [0,1] combines the windowed error rate relative to the route's
// error budget with a relative latency factor: a provider whose windowed
// error rate meets or exceeds the budget is unhealthy (health 0), and among
// healthy providers the one with the lowest EWMA latency sets the latency
// factor baseline. Cancellations are neutral (the scheduler never feeds
// them), exactly like lease and cooldown state.
type ScoreStore struct {
	mu          sync.Mutex
	now         func() time.Time
	pick01      func() float64 // injected for deterministic weighted-random tests
	providers   map[string]*providerScore
	rrCursor    map[string]int // per-route round-robin cursor
	capacity    *ProviderCapacity
	retention   time.Duration
	bucketWidth time.Duration
	bucketCount int
}

type providerScore struct {
	latency   float64 // EWMA of first-meaningful success latency, seconds
	latencyOK bool
	buckets   []scoreBucket
}

type scoreBucket struct {
	tick            int64
	latest          time.Time
	total, failures uint64
}

// ScoreStoreOptions configures the common history horizon. MaxWindow must cover
// the largest balance window in the compiled policy; smaller windows only read
// this history and never discard observations needed by other routes.
type ScoreStoreOptions struct {
	MaxWindow time.Duration
}

const (
	DefaultScoreRetention = 5 * time.Minute
	maxScoreBuckets       = 601
)

func NewScoreStore(now func() time.Time, capacity *ProviderCapacity) *ScoreStore {
	return NewScoreStoreWithOptions(now, capacity, ScoreStoreOptions{})
}

func NewScoreStoreWithOptions(now func() time.Time, capacity *ProviderCapacity, opts ScoreStoreOptions) *ScoreStore {
	if now == nil {
		now = time.Now
	}
	retention := max(DefaultScoreRetention, opts.MaxWindow)
	// At most 601 aggregate buckets per provider, independent of throughput
	// and uptime. Boundary windows may include less than one bucket of older
	// evidence; default resolution is one second and scales for long windows.
	width := retention / (maxScoreBuckets - 1)
	if retention%(maxScoreBuckets-1) != 0 {
		width++
	}
	width = max(time.Second, width)
	count := int(retention / width)
	if retention%width != 0 {
		count++
	}
	return &ScoreStore{
		now:         now,
		pick01:      rand.Float64,
		providers:   make(map[string]*providerScore),
		rrCursor:    make(map[string]int),
		capacity:    capacity,
		retention:   retention,
		bucketWidth: width,
		bucketCount: count + 1,
	}
}

func PromoteFront(targets []model.Target, provider string) []model.Target {
	return promoteFront(targets, provider)
}

func (s *ScoreStore) LatencyOf(provider string) float64 {
	return s.latencyOf(provider)
}

func (s *ScoreStore) latencyOf(provider string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if score, ok := s.providers[provider]; ok && score.latencyOK && score.latency > 0 {
		return score.latency
	}
	minLatency := math.Inf(1)
	haveBaseline := false
	for _, other := range s.providers {
		if other.latencyOK && other.latency > 0 {
			minLatency = math.Min(minLatency, other.latency)
			haveBaseline = true
		}
	}
	if haveBaseline {
		return minLatency
	}
	return 1.0
}

// balanceHealthErrors are the failure classes that count against a provider's
// health budget. Configuration errors (model_not_found, 404, invalid_response)
// and client cancellations are health-neutral: they say nothing about whether
// the provider is alive and fast.
func balanceHealthErrors(class model.ErrorClass) bool {
	switch class {
	case model.ErrorRateLimit, model.ErrorUpstream, model.ErrorTimeout, model.ErrorConnection:
		return true
	}
	return false
}

// Observe records one completed upstream branch for a provider. Latency is
// only meaningful for successful branches. Successes and health errors update
// bounded time buckets; neutral failures need no history. No balance reads are
// required to bound memory. Cancelled branches never reach this method.
func (s *ScoreStore) Observe(provider string, callErr *model.CallError, latency time.Duration) {
	if provider == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	score := s.providers[provider]
	if score == nil {
		score = &providerScore{}
		s.providers[provider] = score
	}
	now := s.now()
	success := callErr == nil
	healthErr := callErr != nil && balanceHealthErrors(callErr.Class)
	if success || healthErr {
		if score.buckets == nil {
			score.buckets = make([]scoreBucket, s.bucketCount)
		}
		tick := now.UnixNano() / int64(s.bucketWidth)
		index := tick % int64(len(score.buckets))
		if index < 0 {
			index += int64(len(score.buckets))
		}
		bucket := &score.buckets[index]
		if bucket.tick != tick || bucket.latest.IsZero() {
			*bucket = scoreBucket{tick: tick}
		}
		bucket.latest = now
		bucket.total++
		if healthErr {
			bucket.failures++
		}
	}
	if success {
		// EWMA with α=0.3 (window ≈ 3 samples): smooth but responsive enough
		// that a provider recovering from a bad spell starts winning back
		// influence instead of being cemented out by a long stale history.
		const alpha = 0.3
		seconds := latency.Seconds()
		if !score.latencyOK {
			score.latency = seconds
			score.latencyOK = true
		} else {
			score.latency = alpha*seconds + (1-alpha)*score.latency
		}
	}
}

// health computes [0,1] for one provider using the route's balance policy.
// An empty window keeps every provider nominally healthy (no error rate, no
// latency baseline), so the very first requests fail open to the base order.
// It takes the store lock because concurrent requests may Observe in parallel.
func (s *ScoreStore) health(provider string, window time.Duration, errorBudget float64) float64 {
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
	var total, failures uint64
	for _, bucket := range score.buckets {
		if bucket.latest.After(cutoff) && !bucket.latest.After(now) {
			total += bucket.total
			failures += bucket.failures
		}
	}
	if total == 0 {
		return 1
	}
	errorRate := float64(failures) / float64(total)
	if errorRate >= errorBudget {
		return 0
	}
	errorFactor := 1 - errorRate/errorBudget
	// Relative latency factor: the provider with the best (lowest) EWMA
	// latency across the store sets the baseline. Without a latency baseline
	// for any provider the factor stays 1.
	minLatency := math.Inf(1)
	haveBaseline := false
	for _, other := range s.providers {
		if other.latencyOK {
			minLatency = math.Min(minLatency, other.latency)
			haveBaseline = true
		}
	}
	latencyFactor := 1.0
	if haveBaseline && score.latencyOK && minLatency > 0 && score.latency > 0 {
		latencyFactor = minLatency / score.latency
		if latencyFactor > 1 {
			latencyFactor = 1
		}
	}
	// The latency factor only skews among providers that are already inside
	// their error budget; it can never zero a healthy provider.
	return errorFactor * latencyFactor
}

// Health reports the numeric [0,1] health score of a provider for the given
// balance policy window and budget. It is the public form of the internal
// health computation, used both by the balance selection and by the
// observability snapshot.
func (s *ScoreStore) Health(provider string, window time.Duration, errorBudget float64) float64 {
	return s.health(provider, window, errorBudget)
}

// Healthy reports whether the provider is inside its error budget for the
// route's balance policy.
func (s *ScoreStore) Healthy(provider string, window time.Duration, errorBudget float64) bool {
	return s.health(provider, window, errorBudget) > 0
}

// baseWeight resolves the static per-provider weight of a target for the
// balance policy: an explicit policy weight wins; without one every provider
// carries the same weight (1). Provider priority is deliberately not a base
// for runtime weights — f7-13 showed priority-weighted selection concentrates
// on the highest-priority provider; priority only shapes compile-time pool
// order.
func baseWeight(target model.Target, policy policy.BalanceConfig) int {
	if policyW, ok := policy.Weights[target.Provider]; ok && policyW > 0 {
		return policyW
	}
	return 1
}

// healthyIndexes returns the pool indexes whose provider is inside the health
// floor for the balance policy. p2c, round_robin and adaptive skip providers
// whose windowed error rate meets or exceeds the error budget; weighted is
// static and keeps every provider so manual tuning stays authoritative.
func (s *ScoreStore) healthyIndexes(targets []model.Target, policy policy.BalanceConfig) []int {
	indexes := make([]int, 0, len(targets))
	for i := range targets {
		switch policy.Strategy {
		case "weighted":
			indexes = append(indexes, i)
		default:
			if s.health(targets[i].Provider, policy.Window, policy.ErrorBudget) > 0 {
				indexes = append(indexes, i)
			}
		}
	}
	return indexes
}

// Select chooses and promotes one provider for the balance action: it moves
// the chosen target to the front so a `race count = 1` executes exactly that
// target deterministically (and a `race count > 1` still races the chosen
// provider first). Candidates weaker than the health floor are skipped with
// fail-open: if every candidate is unhealthy the whole pool is used with base
// order only, so the route is never artificially idle.
//
// Strategies:
//
//	p2c          — power of two choices: two random healthy candidates are
//	  drawn and the one with fewer in-flight branches wins. Spreads load under
//	  concurrency without latency feedback (f7-13 showed latency-weighted
//	  selection re-concentrates on the fastest provider).
//	round_robin — a per-route cursor rotates over the healthy candidates,
//	  excluding the unhealthy via the health floor. Maximum distribution.
//	adaptive    — weighted-random by score(p) = base(p) × health(p): spreads
//	  the load and shifts it toward who is currently coping best.
//	weighted    — only the static weights, no health history.
func (s *ScoreStore) Select(route string, targets []model.Target, policy policy.BalanceConfig) []model.Target {
	if len(targets) < 2 {
		return targets
	}
	switch policy.Strategy {
	case "expected-ttft", "expected-completion":
		return s.selectExpectedTTFT(targets, policy)
	}
	healthy := s.healthyIndexes(targets, policy)
	if len(healthy) == 0 {
		// Fail-open: no healthy candidate at all → base order unchanged so
		// the route keeps working instead of refusing the request.
		return targets
	}
	switch policy.Strategy {
	case "p2c":
		return s.selectP2C(targets, policy, healthy)
	case "round_robin":
		return s.selectRoundRobin(route, targets, healthy)
	case "adaptive", "weighted":
		weights := make([]float64, len(healthy))
		for i, idx := range healthy {
			w := float64(baseWeight(targets[idx], policy))
			if policy.Strategy == "adaptive" {
				w *= s.health(targets[idx].Provider, policy.Window, policy.ErrorBudget)
			}
			weights[i] = w
		}
		chosen := s.pickWeighted(healthy, weights)
		return promoteFront(targets, targets[healthy[chosen]].Provider)
	default:
		return targets
	}
}

// selectP2C implements the power-of-two-choices strategy: it draws two
// distinct random candidates from the healthy set and promotes the one with
// fewer in-flight branches. Under concurrency this equalizes the queues
// without any latency feedback, which f7-13 identified as the missing
// distribution signal. Ties in in-flight keep the first draw, so the result
// is deterministic under the injected pick01 while the draws themselves stay
// random: an idle pool distributes uniformly. The first draw is weighted by
// the static base weight (equal by default); the second is uniform over the
// remaining candidates.
func (s *ScoreStore) selectP2C(targets []model.Target, policy policy.BalanceConfig, healthy []int) []model.Target {
	if len(healthy) == 1 {
		return promoteFront(targets, targets[healthy[0]].Provider)
	}
	// The first draw is weighted by the static base weight (equal 1 by
	// default, so the idle-pool draw is uniform; explicit weights bias it).
	weights := make([]float64, len(healthy))
	for i, idx := range healthy {
		weights[i] = float64(baseWeight(targets[idx], policy))
	}
	first := s.pickWeighted(healthy, weights)
	if first < 0 {
		first = 0
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

// selectRoundRobin rotates a per-route cursor over the healthy candidates in
// pool order, so consecutive requests land on different providers. The cursor
// is stored in the score store keyed by route; it is not a lease and grants
// no favour to the selected provider beyond this one request.
func (s *ScoreStore) selectRoundRobin(route string, targets []model.Target, healthy []int) []model.Target {
	s.mu.Lock()
	cursor := s.rrCursor[route]
	s.mu.Unlock()
	cursor %= len(healthy)
	next := healthy[cursor]
	s.mu.Lock()
	s.rrCursor[route] = (cursor + 1) % len(healthy)
	s.mu.Unlock()
	return promoteFront(targets, targets[next].Provider)
}

// pickWeighted selects one index by weighted random over the given weight
// pairs (indexes and weights). It returns -1 when the total weight is not
// positive.
func (s *ScoreStore) pickWeighted(indexes []int, weights []float64) int {
	total := 0.0
	for _, w := range weights {
		total += w
	}
	if total <= 0 {
		return -1
	}
	target := s.pick01() * total
	acc := 0.0
	for i, w := range weights {
		acc += w
		if target < acc {
			return i
		}
	}
	return len(indexes) - 1
}

// promoteFront returns a copy of targets with the first target of the given
// provider moved to the front (the relative order of the rest is preserved).
func promoteFront(targets []model.Target, provider string) []model.Target {
	for i, target := range targets {
		if target.Provider == provider {
			if i == 0 {
				return targets
			}
			reordered := make([]model.Target, 0, len(targets))
			reordered = append(reordered, target)
			reordered = append(reordered, targets[:i]...)
			reordered = append(reordered, targets[i+1:]...)
			return reordered
		}
	}
	return targets
}

func (s *ScoreStore) selectExpectedTTFT(targets []model.Target, policy policy.BalanceConfig) []model.Target {
	if len(targets) < 2 {
		return targets
	}
	type scoredTarget struct {
		target  model.Target
		score   float64
		healthy bool
	}
	scored := make([]scoredTarget, len(targets))
	for i, target := range targets {
		inFlight := s.capacity.Count(target.Provider)
		maxConc := s.capacity.Limit(target.Provider)
		ewma := s.latencyOf(target.Provider)
		health := s.health(target.Provider, policy.Window, policy.ErrorBudget)
		isHealthy := health > 0
		if health <= 0 {
			health = 0.05
		}
		capacityFactor := 1.0 + float64(inFlight)/float64(maxConc)
		if inFlight >= maxConc {
			capacityFactor *= 10.0
		}
		score := (ewma * capacityFactor) / health
		scored[i] = scoredTarget{target: target, score: score, healthy: isHealthy}
	}
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].target.Tier != scored[j].target.Tier {
			return scored[i].target.Tier < scored[j].target.Tier
		}
		if scored[i].healthy != scored[j].healthy {
			return scored[i].healthy
		}
		return scored[i].score < scored[j].score
	})

	// Epsilon-greedy exploration within the top tier among healthy candidates
	if len(scored) > 1 && policy.ExplorationRate > 0 {
		topTier := scored[0].target.Tier
		var exploreCandidates []int
		for i, sc := range scored {
			if sc.target.Tier == topTier && sc.healthy {
				exploreCandidates = append(exploreCandidates, i)
			}
		}
		if len(exploreCandidates) > 1 && s.pick01() < policy.ExplorationRate {
			// Uniformly pick one of the alternative candidates (indexes 1..len-1)
			pickOffset := 1 + int(s.pick01()*float64(len(exploreCandidates)-1))
			if pickOffset >= len(exploreCandidates) {
				pickOffset = len(exploreCandidates) - 1
			}
			chosenIdx := exploreCandidates[pickOffset]
			chosen := scored[chosenIdx]
			copy(scored[1:chosenIdx+1], scored[0:chosenIdx])
			scored[0] = chosen
		}
	}

	result := make([]model.Target, len(targets))
	for i, sc := range scored {
		result[i] = sc.target
	}
	return result
}
