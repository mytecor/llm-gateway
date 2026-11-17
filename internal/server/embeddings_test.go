package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/driverproto/embeddings"
	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/executor"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/scheduler"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

func TestEmbeddingsThroughInstalledDriverAndBothDialects(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/models" {
			_, _ = w.Write([]byte(`{"data":[{"id":"native","input_modalities":["text","image","audio","video"],"embedding_dimensions":[128,256,512,768]}]}`))
			return
		}
		calls.Add(1)
		var req embeddings.Request
		if json.NewDecoder(r.Body).Decode(&req) != nil || req.Model != "native" {
			t.Error("driver did not map native model")
			w.WriteHeader(400)
			return
		}
		response := embeddings.Response{}
		for _, i := range req.Items {
			v := make([]float32, i.Dimensions)
			v[0] = 1
			response.Embeddings = append(response.Embeddings, embeddings.Vector{Values: v, Tokens: 4})
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer backend.Close()
	zero := 0
	cfg := config.Config{LogLevel: "silent", Providers: []provider.Provider{{ID: "local", Driver: "embeddings", Config: map[string]any{"inference_url": backend.URL}}}, Models: map[string]policy.ModelSpec{"embedding": {Timeout: config.Duration{Duration: 5 * time.Second}, Budget: &policy.BudgetSpec{MaxCalls: 1, MaxInFlight: 1, MaxCallsPerProvider: 1, MaxHedges: &zero}, Tiers: []policy.TierSpec{{Targets: []policy.TargetSpec{{Provider: "local", Native: "native"}}}}}}}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	compiled.ClientKeys = map[string]string{"fixture": "client-fixture"}
	registry, err := executor.New(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	catalog := subsystem.NewCatalog(compiled)
	runner := scheduler.NewRunner(compiled.RoutingPlan(), catalog, registry)
	defer runner.Close()
	api := httptest.NewServer(NewServer(compiled, catalog, runner))
	defer api.Close()
	for _, input := range []string{`"one"`, `["one","two"]`, `[2,3,1]`, `[[2,3],[2,4]]`, `{"parts":[{"text":"before"},{"inlineData":{"mimeType":"image/png","data":"YQ=="}},{"text":"after"}]}`} {
		status, b, _ := nativePost(t, api.URL, "/v1/embeddings", `{"model":"embedding","input":`+input+`,"dimensions":128}`, nil)
		if status != 200 || !bytes.Contains(b, []byte(`"model":"embedding"`)) || bytes.Contains(b, []byte("native")) {
			t.Fatalf("%d %s", status, b)
		}
	}
	for _, test := range []struct{ path, body string }{
		{"/v1beta/models/embedding:embedContent", `{"content":{"parts":[{"text":"one"}]},"outputDimensionality":128}`},
		{"/v1beta/models/embedding:batchEmbedContents", `{"requests":[{"content":{"parts":[{"text":"one"}]},"outputDimensionality":128},{"content":{"parts":[{"text":"two"}]},"outputDimensionality":256}]}`},
	} {
		status, b, _ := nativePost(t, api.URL, test.path, test.body, map[string]string{"Authorization": "", "x-goog-api-key": "client-fixture"})
		if status != 200 || !bytes.Contains(b, []byte(`"values"`)) {
			t.Fatalf("%d %s", status, b)
		}
	}
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		before := calls.Load()
		status, b, _ := nativePost(t, api.URL, path, `{"model":"embedding","input":"one","messages":[]}`, nil)
		if status != 503 || calls.Load() != before {
			t.Fatalf("chat reached embedding provider: %d %s", status, b)
		}
	}
	for _, headers := range []map[string]string{{"Authorization": ""}, {"x-goog-api-key": "different"}} {
		status, b, _ := nativePost(t, api.URL, "/v1beta/models/embedding:embedContent", `{"content":{"parts":[{"text":"one"}]}}`, headers)
		if (status != 400 && status != 401) || !bytes.Contains(b, []byte(`"status"`)) {
			t.Fatalf("Google auth: %d %s", status, b)
		}
	}
	status, b, _ := nativePost(t, api.URL, "/v1/embeddings", `{"model":"embedding","input":"one","stream":true}`, nil)
	if status != 400 {
		t.Fatalf("stream accepted: %s", b)
	}
	request, _ := http.NewRequest("GET", api.URL+"/v1/models", nil)
	request.Header.Set("Authorization", "Bearer client-fixture")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var models map[string]any
	_ = json.NewDecoder(response.Body).Decode(&models)
	encoded, _ := json.Marshal(models)
	if !strings.Contains(string(encoded), "embedding_dimensions") {
		t.Fatal("embedding capabilities missing from catalog")
	}
}
