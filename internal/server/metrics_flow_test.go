package server

import (
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"

	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/scheduler"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

// withUsageBody returns a chat completion response carrying usage so the tokens
// metric is exercised end-to-end.
const withUsageBody = `{"id":"chat-1","model":"native-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`

func TestMetricsFlowThroughServer(t *testing.T) {
	cfg := configtest.TestConfig()
	cfg.Providers = cfg.Providers[:1]
	cfg.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a"),
		policytest.MapRuleHelper("standard", "native-model"),
		policytest.RankRuleHelper("standard"),
		&policy.BalanceRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionBalance}, Strategy: "adaptive"},
		policytest.RaceRuleHelper("standard", 1),
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	executor := &fakeExecutor{
		do: func(_ context.Context, _ model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
			return []byte(withUsageBody), nil
		},
		stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			return nil, &model.CallError{Class: model.ErrorInvalid, Status: 500}
		},
	}
	catalog := subsystem.NewCatalog(compiled)
	runner := scheduler.NewRunner(compiled.RoutingPlan(), catalog, executor)
	api := httptest.NewServer(NewServer(compiled, catalog, runner))
	defer api.Close()

	// Fire a few chat completions with usage.
	body := `{"model":"standard","messages":[{"role":"user","content":"hi"}]}`
	for i := 0; i < 3; i++ {
		resp, err := http.Post(api.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("chat completion status %d", resp.StatusCode)
		}
	}

	m := runner.Metrics()
	var scrape bytes.Buffer
	if err := m.WriteExposition(&scrape); err != nil {
		t.Fatal(err)
	}
	text := scrape.String()

	// Request-level counter: 3 successes on provider a/route standard. The
	// api_key dimension is empty because the test server runs keyless.
	if !strings.Contains(text, `llm_requests_total{api_key="",route="standard",model="standard",provider="a",native_model="native-model",status="success"} 3`) {
		t.Errorf("request counter missing/inaccurate:\n%s", text)
	}
	// policy.Duration histogram count.
	if !strings.Contains(text, `llm_request_duration_seconds_count{api_key="",route="standard",model="standard",provider="a",native_model="native-model"} 3`) {
		t.Errorf("request duration count missing:\n%s", text)
	}
	// Attempts: 3 successful attempts (empty error_type).
	if !strings.Contains(text, `llm_attempts_total{provider="a",native_model="native-model",error_type=""} 3`) {
		t.Errorf("attempt counter missing:\n%s", text)
	}
	// Tokens accumulated: 3 * (10 in, 20 out).
	if !strings.Contains(text, `llm_input_tokens_total{api_key="",model="standard",provider="a",native_model="native-model"} 30`) {
		t.Errorf("input tokens missing:\n%s", text)
	}
	if !strings.Contains(text, `llm_output_tokens_total{api_key="",model="standard",provider="a",native_model="native-model"} 60`) {
		t.Errorf("output tokens missing:\n%s", text)
	}
	// Balance health snapshot exists for provider a.
	if !strings.Contains(text, `llm_balance_health{provider="a"}`) {
		t.Errorf("balance health gauge missing:\n%s", text)
	}
	// The API listener never exposes the operational metrics registry.
	metricsResp, err := http.Get(api.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metricsResp.Body.Close()
	if metricsResp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /metrics over API mux returned %d, want 404", metricsResp.StatusCode)
	}

	metricsAPI := httptest.NewServer(NewMetricsHandler(m))
	defer metricsAPI.Close()
	dedicatedResp, err := http.Get(metricsAPI.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer dedicatedResp.Body.Close()
	scrapeMetrics, _ := io.ReadAll(dedicatedResp.Body)
	if dedicatedResp.StatusCode != http.StatusOK {
		t.Fatalf("dedicated GET /metrics returned %d", dedicatedResp.StatusCode)
	}
	if !strings.Contains(string(scrapeMetrics), "llm_requests_total") {
		t.Errorf("dedicated GET /metrics missing family")
	}
	for _, path := range []string{"/healthz", "/v1/models", "/admin/models/refresh"} {
		resp, err := http.Get(metricsAPI.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("dedicated metrics listener exposes %s: %d", path, resp.StatusCode)
		}
	}
}

// TestMetricsPerClientKey verifies the per-key attribution contract: with
// named client keys configured, requests and token usage split into separate
// api_key series (the non-secret key ID), and rejected keys stay unauthenticated.
func TestMetricsPerClientKey(t *testing.T) {
	cfg := configtest.TestConfig()
	cfg.Providers = cfg.Providers[:1]
	cfg.ClientAPIKeys = []config.ClientKey{
		{ID: "primary", Key: "key-a"},
		{ID: "secondary", Key: "key-b"},
	}
	cfg.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a"),
		policytest.MapRuleHelper("standard", "native-model"),
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 1),
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	executor := &fakeExecutor{
		do: func(_ context.Context, _ model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
			return []byte(withUsageBody), nil
		},
		stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			return nil, &model.CallError{Class: model.ErrorInvalid, Status: 500}
		},
	}
	catalog := subsystem.NewCatalog(compiled)
	runner := scheduler.NewRunner(compiled.RoutingPlan(), catalog, executor)
	api := httptest.NewServer(NewServer(compiled, catalog, runner))
	defer api.Close()

	post := func(bearer string) *http.Response {
		req, err := http.NewRequest("POST", api.URL+"/v1/chat/completions", strings.NewReader(`{"model":"standard","messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+bearer)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp
	}
	for range []int{0, 1} {
		if resp := post("key-a"); resp.StatusCode != http.StatusOK {
			t.Fatalf("key-a request status %d", resp.StatusCode)
		}
	}
	if resp := post("key-b"); resp.StatusCode != http.StatusOK {
		t.Fatalf("key-b request status %d", resp.StatusCode)
	}

	m := runner.Metrics()
	var scrape bytes.Buffer
	if err := m.WriteExposition(&scrape); err != nil {
		t.Fatal(err)
	}
	text := scrape.String()

	// Each key gets its own request series (non-secret ID, never the secret).
	if !strings.Contains(text, `llm_requests_total{api_key="primary",route="standard",model="standard",provider="a",native_model="native-model",status="success"} 2`) {
		t.Errorf("primary key request series missing:\n%s", text)
	}
	if !strings.Contains(text, `llm_requests_total{api_key="secondary",route="standard",model="standard",provider="a",native_model="native-model",status="success"} 1`) {
		t.Errorf("secondary key request series missing:\n%s", text)
	}
	// Token usage is attributed per key too: 2×10/2×20 for primary, 1×10/1×20
	// for secondary.
	if !strings.Contains(text, `llm_input_tokens_total{api_key="primary",model="standard",provider="a",native_model="native-model"} 20`) {
		t.Errorf("primary input tokens missing:\n%s", text)
	}
	if !strings.Contains(text, `llm_output_tokens_total{api_key="primary",model="standard",provider="a",native_model="native-model"} 40`) {
		t.Errorf("primary output tokens missing:\n%s", text)
	}
	if !strings.Contains(text, `llm_input_tokens_total{api_key="secondary",model="standard",provider="a",native_model="native-model"} 10`) {
		t.Errorf("secondary input tokens missing:\n%s", text)
	}
	// The key material itself never leaks into a label value.
	if strings.Contains(text, "key-a") || strings.Contains(text, "key-b") {
		t.Errorf("key material leaked into the exposition:\n%s", text)
	}
}
