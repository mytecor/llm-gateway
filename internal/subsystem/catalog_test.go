package subsystem

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
)

func TestCatalogExactValidationAndMetadataShareSnapshot(t *testing.T) {
	calls := 0
	catalog := NewCatalogWithSources(map[string]CatalogSource{"a": {
		Required: true, Validate: true,
		Lister: model.ModelListFunc(func(context.Context) ([]model.ModelInfo, error) {
			calls++
			return []model.ModelInfo{{ID: "zeta"}, {ID: "alpha", Name: "Alpha", ReasoningEfforts: []string{"low", "high"}}, {ID: "zeta"}}, nil
		}),
	}})
	if err := catalog.Validate("a", "alpha"); err == nil || err.Status != 503 {
		t.Fatal("explicit catalog must fail closed without snapshot", err)
	}
	if failures := catalog.Refresh(context.Background()); len(failures) != 0 {
		t.Fatal(failures)
	}
	for _, id := range []string{"alpha", "zeta"} {
		if err := catalog.Validate("a", id); err != nil {
			t.Fatal(err)
		}
	}
	if err := catalog.Validate("a", "missing"); err == nil || err.Class != model.ErrorModelNotFound {
		t.Fatal("expected exact model rejection", err)
	}
	models, err := catalog.Models(context.Background(), "a")
	if err != nil || len(models) != 2 || models[0].Name != "Alpha" || calls != 1 {
		t.Fatalf("metadata did not reuse snapshot: %v %v calls=%d", models, err, calls)
	}
	models[0].ID = "corrupted"
	models[0].ReasoningEfforts[0] = "corrupted"
	again, _ := catalog.Models(context.Background(), "a")
	if again[0].ID != "alpha" || again[0].ReasoningEfforts[0] != "low" {
		t.Fatal("caller mutated catalog snapshot")
	}
}

func TestCatalogLastKnownGoodAndProviderIsolation(t *testing.T) {
	var mu sync.Mutex
	failA := false
	source := func(id string) CatalogSource {
		return CatalogSource{Required: true, Validate: true,
			Lister: model.ModelListFunc(func(context.Context) ([]model.ModelInfo, error) {
				mu.Lock()
				defer mu.Unlock()
				if id == "a" && failA {
					return nil, errors.New("unavailable")
				}
				return []model.ModelInfo{{ID: id + "-model"}}, nil
			})}
	}
	catalog := NewCatalogWithSources(map[string]CatalogSource{"a": source("a"), "b": source("b")})
	if failures := catalog.Refresh(context.Background()); len(failures) != 0 {
		t.Fatal(failures)
	}
	mu.Lock()
	failA = true
	mu.Unlock()
	if failures := catalog.Refresh(context.Background()); len(failures) != 1 || failures["a"] == nil {
		t.Fatal(failures)
	}
	for _, id := range []string{"a", "b"} {
		if err := catalog.Validate(id, id+"-model"); err != nil {
			t.Fatal("snapshot lost", err)
		}
	}
	if err := catalog.Validate("a", "b-model"); err == nil {
		t.Fatal("provider namespaces were merged")
	}
}

func TestCatalogEmptyRefreshPreservesSnapshot(t *testing.T) {
	models := []model.ModelInfo{{ID: "alpha"}}
	c := NewCatalogWithSources(map[string]CatalogSource{"a": {Validate: true, Lister: model.ModelListFunc(func(context.Context) ([]model.ModelInfo, error) { return models, nil })}})
	if failures := c.Refresh(context.Background()); len(failures) != 0 {
		t.Fatal(failures)
	}
	models = nil
	if failures := c.Refresh(context.Background()); len(failures) != 1 {
		t.Fatal("empty catalog refresh should report a failure")
	}
	if err := c.Validate("a", "alpha"); err != nil {
		t.Fatal("empty refresh destroyed last-known-good", err)
	}
}

func TestCatalogOptionalAndStatefulValidation(t *testing.T) {
	c := NewCatalogWithSources(map[string]CatalogSource{
		"optional": {Validate: true, Lister: model.ModelListFunc(func(context.Context) ([]model.ModelInfo, error) { return nil, errors.New("offline") })},
		"session":  {Lister: model.ModelListFunc(func(context.Context) ([]model.ModelInfo, error) { return []model.ModelInfo{{ID: "agent-model"}}, nil })},
	})
	for _, id := range []string{"missing", "optional", "session"} {
		if err := c.Validate(id, "default"); err != nil {
			t.Fatal("unavailable optional discovery must stay optimistic", err)
		}
	}
	if _, err := c.Models(context.Background(), "session"); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate("session", "default"); err != nil {
		t.Fatal("metadata must not reject native default session", err)
	}
}

func TestCatalogLazyDiscoveryIsSharedByConcurrentReaders(t *testing.T) {
	var calls int
	c := NewCatalogWithSources(map[string]CatalogSource{"a": {Lister: model.ModelListFunc(func(context.Context) ([]model.ModelInfo, error) {
		calls++
		return []model.ModelInfo{{ID: "model"}}, nil
	})}})
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if _, err := c.Models(context.Background(), "a"); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("discovery ran %d times, want 1", calls)
	}
}

func TestCatalogRefreshesProvidersConcurrently(t *testing.T) {
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	source := CatalogSource{Lister: model.ModelListFunc(func(context.Context) ([]model.ModelInfo, error) {
		ready <- struct{}{}
		<-release
		return []model.ModelInfo{{ID: "model"}}, nil
	})}
	c := NewCatalogWithSources(map[string]CatalogSource{"a": source, "b": source})
	done := make(chan struct{})
	go func() { c.Refresh(context.Background()); close(done) }()
	<-ready
	<-ready
	close(release)
	<-done
}

func TestCatalogModelNotFoundIsNotDefaultRetryable(t *testing.T) {
	if policy.AllRetryableClasses()[model.ErrorModelNotFound] {
		t.Fatal("model_not_found must be opt-in")
	}
}
