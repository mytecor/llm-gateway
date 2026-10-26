package subsystem

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/executor/external"
	"github.com/mytecor/llm-gateway/internal/model"
)

// CatalogSource is independent of provider transports and credentials.
// Stateful model lists provide metadata; they do not constrain default sessions.
type CatalogSource struct {
	Lister             model.ModelLister
	Required, Validate bool
}

type catalogEntry struct {
	source  CatalogSource
	refresh chan struct{}
	models  []model.ModelInfo
	updated time.Time
}

// Catalog owns all gateway discovery snapshots, including native metadata,
// exact validation and last-known-good retention. Drivers only fetch models.
type Catalog struct {
	mu      sync.RWMutex
	entries map[string]*catalogEntry
}

func NewCatalog(cfg *config.CompiledConfig) *Catalog {
	sources := make(map[string]CatalogSource)
	for id, p := range cfg.Providers {
		if p.Description == nil {
			continue
		}
		discovery := p.Description.Catalog
		if discovery == nil && !p.Capabilities().Stateful {
			continue
		}
		settings := p.DriverSettings(nil)
		sources[id] = CatalogSource{
			Lister:   model.ModelListFunc(func(ctx context.Context) ([]model.ModelInfo, error) { return external.DiscoverModels(ctx, settings) }),
			Required: discovery != nil && discovery.Explicit,
			Validate: !p.Capabilities().Stateful,
		}
	}
	return NewCatalogWithSources(sources)
}

func NewCatalogWithSources(sources map[string]CatalogSource) *Catalog {
	c := &Catalog{entries: make(map[string]*catalogEntry, len(sources))}
	for id, source := range sources {
		c.entries[id] = &catalogEntry{source: source, refresh: make(chan struct{}, 1)}
	}
	return c
}

func (c *Catalog) Validate(providerID, native string) *model.CallError {
	entry := c.entries[providerID]
	if entry == nil || !entry.source.Validate {
		return nil
	}
	c.mu.RLock()
	models := entry.models
	c.mu.RUnlock()
	if len(models) == 0 {
		if entry.source.Required {
			return &model.CallError{Class: model.ErrorInvalid, Status: 503, Cause: fmt.Errorf("provider %q has no last-known-good catalog (explicit models_url)", providerID)}
		}
		return nil
	}
	index := sort.Search(len(models), func(i int) bool { return models[i].ID >= native })
	if index < len(models) && models[index].ID == native {
		return nil
	}
	return &model.CallError{Class: model.ErrorModelNotFound, Status: 404, Cause: fmt.Errorf("native model %q is not in the catalog of provider %q", native, providerID)}
}

func cloneModels(models []model.ModelInfo) []model.ModelInfo {
	result := append([]model.ModelInfo(nil), models...)
	for i := range result {
		result[i].ReasoningEfforts = append([]string(nil), result[i].ReasoningEfforts...)
	}
	return result
}

// Models reads the shared snapshot, lazily discovering an uninitialized source.
// Explicit refresh bypasses it; a failed refresh leaves every existing snapshot intact.
func (c *Catalog) Models(ctx context.Context, provider string) ([]model.ModelInfo, error) {
	entry := c.entries[provider]
	if entry == nil {
		return nil, nil
	}
	// Readers of a last-known-good snapshot never wait for an upstream refresh.
	c.mu.RLock()
	if !entry.updated.IsZero() {
		models := cloneModels(entry.models)
		c.mu.RUnlock()
		return models, nil
	}
	c.mu.RUnlock()
	select {
	case entry.refresh <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-entry.refresh }()
	c.mu.RLock()
	updated := entry.updated
	c.mu.RUnlock()
	if updated.IsZero() {
		if err := c.refreshEntry(ctx, entry); err != nil {
			return nil, err
		}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneModels(entry.models), nil
}

func (c *Catalog) refreshEntry(ctx context.Context, entry *catalogEntry) error {
	if entry.source.Lister == nil {
		return errors.New("catalog source requires a model lister")
	}
	models, err := entry.source.Lister.Models(ctx)
	if err != nil {
		return err
	}
	set := make(map[string]model.ModelInfo, len(models))
	for _, m := range models {
		m.ID = strings.TrimSpace(m.ID)
		if _, exists := set[m.ID]; m.ID != "" && !exists {
			set[m.ID] = m
		}
	}
	if len(set) == 0 {
		if entry.source.Validate {
			return errors.New("catalog contains no models")
		}
		c.mu.Lock()
		if entry.updated.IsZero() {
			entry.updated = time.Now()
		}
		c.mu.Unlock()
		return nil
	}
	normalized := make([]model.ModelInfo, 0, len(set))
	for _, m := range set {
		normalized = append(normalized, m)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].ID < normalized[j].ID })
	c.mu.Lock()
	entry.models = cloneModels(normalized)
	entry.updated = time.Now()
	c.mu.Unlock()
	return nil
}

// Refresh fetches providers concurrently and reports failures independently.
func (c *Catalog) Refresh(ctx context.Context) map[string]error {
	type result struct {
		id  string
		err error
	}
	results := make(chan result, len(c.entries))
	for id, entry := range c.entries {
		go func() {
			select {
			case entry.refresh <- struct{}{}:
			case <-ctx.Done():
				results <- result{id, ctx.Err()}
				return
			}
			defer func() { <-entry.refresh }()
			results <- result{id, c.refreshEntry(ctx, entry)}
		}()
	}
	failures := make(map[string]error)
	for range c.entries {
		r := <-results
		if r.err != nil {
			failures[r.id] = r.err
		}
	}
	return failures
}

func (c *Catalog) Start(ctx context.Context, interval time.Duration, report func(map[string]error)) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				report(c.Refresh(ctx))
			}
		}
	}()
}

func (c *Catalog) Status() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	providers := make(map[string]any, len(c.entries))
	for id, entry := range c.entries {
		providers[id] = map[string]any{"available": len(entry.models) > 0, "models": len(entry.models), "updated_at": entry.updated}
	}
	return providers
}
