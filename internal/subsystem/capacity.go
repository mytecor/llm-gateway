package subsystem

import (
	"sync"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

const defaultCapacityHalveCooldown = 5 * time.Second

// ProviderCapacity owns global provider limits and their live reservations.
// Balancing and metrics read the same count; only a permit can release a slot.
// Effective limits adapt dynamically via AIMD within [1, configuredLimit] when
// upstream rate limits, timeouts, or 5xx errors occur.
type ProviderCapacity struct {
	mu              sync.Mutex
	now             func() time.Time
	halveCooldown   time.Duration
	used, limits    map[string]int
	effectiveLimits map[string]int
	successStreaks  map[string]int
	lastHalved      map[string]time.Time
	changed         chan struct{}
	metrics         *telemetry.Metrics
}

type Permit struct {
	once     sync.Once
	capacity *ProviderCapacity
	provider string
}

func NewProviderCapacity(metrics *telemetry.Metrics) *ProviderCapacity {
	return NewProviderCapacityWithClock(metrics, nil)
}

func NewProviderCapacityWithClock(metrics *telemetry.Metrics, now func() time.Time) *ProviderCapacity {
	if now == nil {
		now = time.Now
	}
	return &ProviderCapacity{
		now:             now,
		halveCooldown:   defaultCapacityHalveCooldown,
		used:            make(map[string]int),
		limits:          make(map[string]int),
		effectiveLimits: make(map[string]int),
		successStreaks:  make(map[string]int),
		lastHalved:      make(map[string]time.Time),
		changed:         make(chan struct{}),
		metrics:         metrics,
	}
}

func (c *ProviderCapacity) configuredLimit(provider string) int {
	if n := c.limits[provider]; n > 0 {
		return n
	}
	return 16
}

func (c *ProviderCapacity) limit(provider string) int {
	if n := c.effectiveLimits[provider]; n > 0 {
		return n
	}
	return c.configuredLimit(provider)
}

func (c *ProviderCapacity) SetLimit(provider string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n <= 0 {
		n = 16
	}
	c.limits[provider] = n
	c.effectiveLimits[provider] = n
	c.successStreaks[provider] = 0
	delete(c.lastHalved, provider)
	if c.metrics != nil {
		c.metrics.ObserveProviderCapacity(provider, n)
	}
	close(c.changed)
	c.changed = make(chan struct{})
}

// ObserveOutcome adjusts the provider's effective concurrency limit using AIMD:
// consecutive successes gently probe upward toward the configured ceiling, while
// congestion/timeout/5xx failures halve the effective limit (floor 1).
func (c *ProviderCapacity) ObserveOutcome(provider string, callErr *model.CallError) {
	if provider == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ceiling := c.configuredLimit(provider)
	eff := c.limit(provider)
	if eff > ceiling {
		eff = ceiling
	}
	if callErr == nil {
		c.successStreaks[provider]++
		if c.successStreaks[provider] >= 4 && eff < ceiling {
			eff++
			c.effectiveLimits[provider] = eff
			c.successStreaks[provider] = 0
			if c.metrics != nil {
				c.metrics.ObserveProviderCapacity(provider, eff)
			}
			close(c.changed)
			c.changed = make(chan struct{})
		}
		return
	}
	switch callErr.Class {
	case model.ErrorRateLimit, model.ErrorTimeout, model.ErrorConnection, model.ErrorUpstream:
		c.successStreaks[provider] = 0
		now := c.now()
		if last := c.lastHalved[provider]; !last.IsZero() && now.Sub(last) < c.halveCooldown {
			return
		}
		c.lastHalved[provider] = now
		next := max(1, eff/2)
		if next != eff {
			c.effectiveLimits[provider] = next
			if c.metrics != nil {
				c.metrics.ObserveProviderCapacity(provider, next)
			}
			close(c.changed)
			c.changed = make(chan struct{})
		}
	}
}

func (c *ProviderCapacity) Acquire(provider string) *Permit {
	if provider == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.used[provider] >= c.limit(provider) {
		return nil
	}
	c.used[provider]++
	if c.metrics != nil {
		c.metrics.IncrInFlight(provider)
	}
	return &Permit{capacity: c, provider: provider}
}

// Release is safe across cancellation, losing a race and normal completion.
func (p *Permit) Release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		c := p.capacity
		c.mu.Lock()
		defer c.mu.Unlock()
		c.used[p.provider]--
		if c.metrics != nil {
			c.metrics.DecrInFlight(p.provider)
		}
		close(c.changed)
		c.changed = make(chan struct{})
	})
}

func (c *ProviderCapacity) Count(provider string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used[provider]
}

func (c *ProviderCapacity) Limit(provider string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.limit(provider)
}

func (c *ProviderCapacity) Changed() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.changed
}

func (c *ProviderCapacity) Available(targets []model.Target) []model.Target {
	c.mu.Lock()
	defer c.mu.Unlock()
	available := make([]model.Target, 0, len(targets))
	for _, t := range targets {
		if c.used[t.Provider] < c.limit(t.Provider) {
			available = append(available, t)
		}
	}
	return available
}
