package subsystem

import (
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/telemetry"
	"sync"
)

// ProviderCapacity owns global provider limits and their live reservations.
// Balancing and metrics read the same count; only a permit can release a slot.
type ProviderCapacity struct {
	mu           sync.Mutex
	used, limits map[string]int
	changed      chan struct{}
	metrics      *telemetry.Metrics
}

type Permit struct {
	once     sync.Once
	capacity *ProviderCapacity
	provider string
}

func NewProviderCapacity(metrics *telemetry.Metrics) *ProviderCapacity {
	return &ProviderCapacity{used: make(map[string]int), limits: make(map[string]int), changed: make(chan struct{}), metrics: metrics}
}

func (c *ProviderCapacity) limit(provider string) int {
	if n := c.limits[provider]; n > 0 {
		return n
	}
	return 16
}

func (c *ProviderCapacity) SetLimit(provider string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n <= 0 {
		n = 16
	}
	c.limits[provider] = n
	if c.metrics != nil {
		c.metrics.ObserveProviderCapacity(provider, n)
	}
	close(c.changed)
	c.changed = make(chan struct{})
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
