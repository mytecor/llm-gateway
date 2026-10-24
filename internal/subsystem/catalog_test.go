package subsystem

import (
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func catalogForProvider(t *testing.T, source policy.CatalogSource) *Catalog {
	t.Helper()
	compiled := policy.RaceOnlyConfig(t)
	compiled.CatalogSources = map[string][]policy.CatalogSource{"a": {source}}
	catalog := NewCatalog(compiled)
	return catalog
}

// exact-test helpers mirror the Validate contract: nil (valid or optimistic),
// model.ErrorInvalid 503 (explicit catalog without a snapshot), or
// model.ErrorModelNotFound (snapshot exists without the native ID).

func TestCatalogExactMatchOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(`{"object":"list","data":[{"id":"deepseek-ai/DeepSeek-V4-Flash-0731"},{"id":"gonka/deepseek-ai/DeepSeek-V4-Flash-0731"}]}`))
	}))
	defer server.Close()
	catalog := catalogForProvider(t, policy.CatalogSource{URL: server.URL, Explicit: true})
	if failures := catalog.Refresh(context.Background()); len(failures) != 0 {
		t.Fatalf("initial refresh failed: %v", failures)
	}
	if callErr := catalog.Validate("a", "deepseek-ai/DeepSeek-V4-Flash-0731"); callErr != nil {
		t.Fatalf("exact match must be accepted, got %v", callErr)
	}
	if callErr := catalog.Validate("a", "gonka/deepseek-ai/DeepSeek-V4-Flash-0731"); callErr != nil {
		t.Fatalf("second native alias accepted by the map must validate, got %v", callErr)
	}
	callErr := catalog.Validate("a", "some-arbitrary-id")
	if callErr == nil || callErr.Class != model.ErrorModelNotFound {
		t.Fatalf("a model outside the provider catalog must be rejected with model_not_found, got %v", callErr)
	}
}

func TestCatalogLastKnownGoodSurvivesFailedRefresh(t *testing.T) {
	var mu sync.RWMutex
	status := http.StatusOK
	body := `{"object":"list","data":[{"id":"alpha"},{"id":"alpha"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(body))
	}))
	defer server.Close()

	catalog := catalogForProvider(t, policy.CatalogSource{URL: server.URL, Explicit: true})
	if failures := catalog.Refresh(context.Background()); len(failures) != 0 {
		t.Fatalf("initial refresh failed: %v", failures)
	}
	if callErr := catalog.Validate("a", "alpha"); callErr != nil {
		t.Fatalf("known model rejected: %v", callErr)
	}

	mu.Lock()
	status = http.StatusBadGateway
	body = `{"error":"unavailable"}`
	mu.Unlock()
	if failures := catalog.Refresh(context.Background()); len(failures) != 1 {
		t.Fatalf("failed refresh was not reported: %v", failures)
	}
	if callErr := catalog.Validate("a", "alpha"); callErr != nil {
		t.Fatalf("last-known-good was lost after a failed refresh: %v", callErr)
	}
}

func TestCatalogPartialRefreshIsPerProviderIndependent(t *testing.T) {
	// The first provider's explicit catalog fails; the second's succeeds. A
	// per-provider snapshot must be retained for the successful provider while
	// the failing one keeps its own last-known-good.
	unavailable := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unavailable.Close()
	available := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(`{"data":[{"id":"ok-model"}]}`))
	}))
	defer available.Close()

	compiled := policy.RaceOnlyConfig(t)
	compiled.CatalogSources = map[string][]policy.CatalogSource{
		"a": {{URL: unavailable.URL, Explicit: true}},
		"b": {{URL: available.URL, Explicit: true}},
	}
	catalog := NewCatalog(compiled)
	// Prime both providers with a healthy snapshot.
	compiled.CatalogSources["a"] = []policy.CatalogSource{{URL: available.URL, Explicit: true}}
	catalog.Refresh(context.Background())
	compiled.CatalogSources["a"] = []policy.CatalogSource{{URL: unavailable.URL, Explicit: true}}
	if failures := catalog.Refresh(context.Background()); len(failures) != 1 {
		t.Fatalf("expected exactly one failing provider, got %v", failures)
	}
	if callErr := catalog.Validate("a", "ok-model"); callErr != nil {
		t.Fatalf("last-known-good for provider a was destroyed by its own partial failure: %v", callErr)
	}
	if callErr := catalog.Validate("b", "ok-model"); callErr != nil {
		t.Fatalf("successful provider b snapshot was lost: %v", callErr)
	}
}

func TestCatalogInferredOptimisticWithoutSnapshot(t *testing.T) {
	// An inferred catalog (no explicit modelsUrl) that has not produced a
	// snapshot yet must optimistically accept the configured native ID.
	compiled := policy.RaceOnlyConfig(t)
	compiled.CatalogSources = map[string][]policy.CatalogSource{
		"a": {{URL: "https://catalog.invalid/v1/models"}}, // inferred, not Explicit
	}
	catalog := NewCatalog(compiled)
	if callErr := catalog.Validate("a", "native-model"); callErr != nil {
		t.Fatalf("inferred catalog without a snapshot must stay optimistic: %v", callErr)
	}
}

func TestCatalogExplicitFailClosedWithoutSnapshot(t *testing.T) {
	compiled := policy.RaceOnlyConfig(t)
	compiled.CatalogSources = map[string][]policy.CatalogSource{
		"a": {{URL: "https://catalog.invalid/v1/models", Explicit: true}},
	}
	catalog := NewCatalog(compiled)
	callErr := catalog.Validate("a", "native-model")
	if callErr == nil || callErr.Class != model.ErrorInvalid || callErr.Status != 503 {
		t.Fatalf("explicit catalog without a snapshot must fail closed, got %v", callErr)
	}
}

func TestCatalogNoDiscoveryProviderIsOptimistic(t *testing.T) {
	compiled := policy.RaceOnlyConfig(t)
	compiled.CatalogSources = map[string][]policy.CatalogSource{}
	catalog := NewCatalog(compiled)
	if callErr := catalog.Validate("a", "any-explicit-native"); callErr != nil {
		t.Fatalf("provider without discovery must accept the explicit native: %v", callErr)
	}
}

func TestCatalogMergesSourcesAndDetectsAbsence(t *testing.T) {
	// After a refresh with an explicit catalog, a configured alias that is not
	// present yields model_not_found; errors carry a stable class, never a
	// fallback to an arbitrary first catalog id.
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(`{"data":[{"id":"zeta"},{"id":"alpha"},{"id":"alpha"}]}`))
	}))
	defer server.Close()
	catalog := catalogForProvider(t, policy.CatalogSource{URL: server.URL, Explicit: true})
	if failures := catalog.Refresh(context.Background()); len(failures) != 0 {
		t.Fatalf("refresh failed: %v", failures)
	}
	if callErr := catalog.Validate("a", "alpha"); callErr != nil {
		t.Fatalf("present model rejected: %v", callErr)
	}
	callErr := catalog.Validate("a", "missing-primary")
	if callErr == nil || callErr.Class != model.ErrorModelNotFound {
		t.Fatalf("missing model must be classified model_not_found, got %v", callErr)
	}
	if callErr.Cause == nil {
		t.Fatalf("model_not_found must carry a safe cause: %v", callErr)
	}
}

func TestCatalogModelNotFoundIsNotDefaultRetryable(t *testing.T) {
	if policy.AllRetryableClasses()[model.ErrorModelNotFound] {
		t.Fatalf("model_not_found must not be a default retryable class; it is opt-in via on")
	}
}

func TestCatalogFetchesDerivedProviderPaths(t *testing.T) {
	tests := []struct {
		name     string
		basePath string
		wantPath string
	}{
		{name: "versioned base", basePath: "/v1", wantPath: "/v1/models"},
		{name: "custom prefix", basePath: "/functions/v1/gonka", wantPath: "/functions/v1/gonka/models"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != test.wantPath {
					t.Errorf("catalog path = %q, want %q", request.URL.Path, test.wantPath)
				}
				if request.Header.Get("Authorization") != "Bearer provider-key" {
					t.Errorf("inferred catalog did not use provider credential")
				}
				_, _ = writer.Write([]byte(`{"data":[{"id":"native-model"}]}`))
			}))
			defer server.Close()

			cfg := policy.TestConfig()
			cfg.Providers = []policy.Provider{cfg.Providers[0]}
			cfg.Providers[0].Config = map[string]any{
				"inference_url": server.URL + test.basePath,
				"api_key":       "provider-key",
			}
			cfg.Rules = []policy.Rule{
				policy.FilterModel("standard", "standard"),
				policy.FilterProvider("standard", "a"),
				policy.MapRuleHelper("standard", "native-model"),
				policy.RankRuleHelper("standard"),
				policy.RaceRuleHelper("standard", 1),
			}
			compiled, err := policy.CompileConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			catalog := NewCatalog(compiled)
			if failures := catalog.Refresh(context.Background()); len(failures) != 0 {
				t.Fatalf("derived catalog refresh failed: %v", failures)
			}
			if callErr := catalog.Validate("a", "native-model"); callErr != nil {
				t.Fatalf("derived catalog rejected the configured native: %v", callErr)
			}
		})
	}
}
