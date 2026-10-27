package subsystem

import (
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"

	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mytecor/llm-gateway/internal/config"
)

// Exercise the real HTTP driver subprocess: the core only knows ModelInfo,
// while URL construction, authentication and HTTP decoding stay in the driver.
func TestCatalogDiscoversThroughDriverAndSharesMetadata(t *testing.T) {
	var calls atomic.Int32
	var unavailable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/prefix/v1/models" || r.Header.Get("Authorization") != "Bearer provider-key" {
			t.Error("driver discovery lost path or credentials")
		}
		if unavailable.Load() {
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"native-model","name":"Native Model","reasoning_efforts":["low","high"]}]}`))
	}))
	defer server.Close()
	cfg := configtest.TestConfig()
	cfg.Providers = cfg.Providers[:1]
	cfg.Providers[0].Config = map[string]any{"base_provider": "openai", "inference_url": server.URL + "/prefix/v1", "api_key": "provider-key"}
	cfg.Rules = policytest.EntryRules([]string{"a"}, 1)
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	catalog := NewCatalog(compiled)
	models, err := catalog.Models(context.Background(), "a")
	if err != nil || len(models) != 1 || models[0].Name != "Native Model" || len(models[0].ReasoningEfforts) != 2 {
		t.Fatalf("discovery metadata: %+v %v", models, err)
	}
	if err := catalog.Validate("a", "native-model"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("readers fetched separate snapshots: %d", calls.Load())
	}
	unavailable.Store(true)
	if failures := catalog.Refresh(context.Background()); len(failures) != 1 {
		t.Fatal("failed forced refresh was not reported", failures)
	}
	models, err = catalog.Models(context.Background(), "a")
	if err != nil || len(models) != 1 || calls.Load() != 2 {
		t.Fatalf("last-known-good was lost or fetched again: %+v %v calls=%d", models, err, calls.Load())
	}
}
