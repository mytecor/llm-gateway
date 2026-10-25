package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/scheduler"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

func TestAdmissionContinuationReleaseHandoverNoLeak(t *testing.T) {
	cfg := config.TestConfig()
	maxPending := 4
	cfg.Rules = []policy.Rule{
		policy.FilterModel("standard", "standard"),
		policy.MapRuleDirect("standard", []string{"a", "b"}, "model-x", 0),
		&policy.AdmissionRule{
			RuleBase:    policy.RuleBase{Route: "standard", Action: policy.ActionAdmission},
			MaxInFlight: 1,
			MaxPending:  &maxPending,
			WaitTimeout: policy.Duration{Duration: time.Second},
		},
		policy.RankRuleHelper("standard"),
		policy.RaceRuleHelper("standard", 1),
		&policy.RetryRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionRetry}, Attempts: 2},
		&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 10 * time.Second}},
		policy.ContinueRuleHelper("standard", 10*time.Second, "full"),
	}
	compiled, err := config.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	executor := &fakeExecutor{
		stream: func(ctx context.Context, target model.Target, req model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			ch := make(chan model.StreamEvent, 3)
			if target.Provider == "a" {
				ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"from-a"}}]}`), Meaningful: true}
				close(ch)
				return ch, nil
			}
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"from-b"}}]}`), Meaningful: true}
			ch <- model.StreamEvent{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`), Meaningful: false}
			close(ch)
			return ch, nil
		},
	}

	api, _ := streamTestServer(t, compiled, executor)
	defer api.Close()

	body1 := postStream(t, api.URL)
	if !strings.Contains(body1, "from-b") {
		t.Fatalf("takeover must relay the continuation from-b, got: %s", body1)
	}

	// Verify that the admission slot was cleanly released:
	// A second request must immediately succeed without queue rejection!
	body2 := postStream(t, api.URL)
	if !strings.Contains(body2, "from-b") {
		t.Fatalf("second request failed (possible admission leak), got: %s", body2)
	}
}

func TestInternalRetryHidesProviderErrorsFromClient(t *testing.T) {
	cfg := config.TestConfig()
	cfg.Rules = []policy.Rule{
		policy.FilterModel("standard", "standard"),
		policy.MapRuleDirect("standard", []string{"a", "b"}, "model-x", 0),
		policy.MapRuleDirect("standard", []string{"c"}, "model-y", 1),
		policy.RankRuleHelper("standard"),
		policy.BalanceRuleCustom("standard", func(r *policy.BalanceRule) { r.Strategy = "expected-ttft" }),
		policy.RaceRuleHelper("standard", 1),
		&policy.RetryRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionRetry}},
		&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 10 * time.Second}},
	}
	compiled, err := config.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	executor := &fakeExecutor{
		do: func(ctx context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
			attempts.Add(1)
			if target.Provider == "a" {
				return nil, &model.CallError{Class: model.ErrorUpstream, Status: 503}
			}
			if target.Provider == "b" {
				return nil, &model.CallError{Class: model.ErrorRateLimit, Status: 429}
			}
			if target.Provider == "c" {
				return []byte(`{"id":"chat-1","choices":[{"message":{"content":"success-c"}}]}`), nil
			}
			return nil, &model.CallError{Class: model.ErrorInvalid, Status: 500}
		},
	}

	runner := scheduler.NewRunner(compiled, subsystem.NewCatalog(compiled), executor)
	srv := NewServer(compiled, subsystem.NewCatalog(compiled), runner)

	reqBody := `{"model":"standard","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected client 200 OK, got status %d body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "success-c") {
		t.Fatalf("expected response from provider c, got %s", rec.Body.String())
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 upstream attempts, got %d", attempts.Load())
	}
}

func TestInternalRetryFallbackToSameProviderDifferentTier(t *testing.T) {
	cfg := config.TestConfig()
	// Provider a is in tier 0 with model-x (fails 404), and in tier 1 with model-y (succeeds)
	cfg.Rules = []policy.Rule{
		policy.FilterModel("standard", "standard"),
		policy.MapRuleDirect("standard", []string{"a"}, "model-x", 0),
		policy.MapRuleDirect("standard", []string{"a"}, "model-y", 1),
		policy.RankRuleHelper("standard"),
		policy.RaceRuleHelper("standard", 1),
		&policy.RetryRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionRetry}, Attempts: 2},
		&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 10 * time.Second}},
	}
	compiled, err := config.CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	executor := &fakeExecutor{
		do: func(ctx context.Context, target model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
			attempts.Add(1)
			if target.Model == "model-x" {
				return nil, &model.CallError{Class: model.ErrorNotFound, Status: 404}
			}
			if target.Model == "model-y" {
				return []byte(`{"id":"chat-1","choices":[{"message":{"content":"success-model-y"}}]}`), nil
			}
			return nil, &model.CallError{Class: model.ErrorInvalid, Status: 500}
		},
	}

	runner := scheduler.NewRunner(compiled, subsystem.NewCatalog(compiled), executor)
	srv := NewServer(compiled, subsystem.NewCatalog(compiled), runner)

	reqBody := `{"model":"standard","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected client 200 OK, got status %d body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "success-model-y") {
		t.Fatalf("expected response from model-y, got %s", rec.Body.String())
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts (model-x then model-y), got %d", attempts.Load())
	}
}
