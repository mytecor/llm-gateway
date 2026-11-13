package server

import (
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"

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
	cfg := configtest.TestConfig()
	maxPending := 4
	cfg.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.MapRuleDirect("standard", []string{"a", "b"}, "model-x", 0),
		&policy.AdmissionRule{
			RuleBase:    policy.RuleBase{Route: "standard", Action: policy.ActionAdmission},
			MaxInFlight: 1,
			MaxPending:  &maxPending,
			WaitTimeout: policy.Duration{Duration: time.Second},
		},
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 1),
		&policy.RetryRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionRetry}, Attempts: 2},
		&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 10 * time.Second}},
		policytest.ContinueRuleHelper("standard", 10*time.Second, "full"),
	}
	compiled, err := config.PrepareAndCompile(cfg)
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
	cfg := configtest.TestConfig()
	cfg.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.MapRuleDirect("standard", []string{"a", "b"}, "model-x", 0),
		policytest.MapRuleDirect("standard", []string{"c"}, "model-y", 1),
		policytest.RankRuleHelper("standard"),
		policytest.BalanceRuleCustom("standard", func(r *policy.BalanceRule) { r.Strategy = "expected-ttft" }),
		policytest.RaceRuleHelper("standard", 1),
		&policy.RetryRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionRetry}},
		&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 10 * time.Second}},
	}
	compiled, err := config.PrepareAndCompile(cfg)
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

	runner := scheduler.NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
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
	cfg := configtest.TestConfig()
	// Provider a is in tier 0 with model-x (fails 404), and in tier 1 with model-y (succeeds)
	cfg.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.MapRuleDirect("standard", []string{"a"}, "model-x", 0),
		policytest.MapRuleDirect("standard", []string{"a"}, "model-y", 1),
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 1),
		&policy.RetryRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionRetry}, Attempts: 2},
		&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 10 * time.Second}},
	}
	compiled, err := config.PrepareAndCompile(cfg)
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

	runner := scheduler.NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
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

func TestServerReturns429WithRetryAfterForQueueRejectionAnd504ForRouteTimeout(t *testing.T) {
	cfg := configtest.TestConfig()
	maxPending := 0
	cfg.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a"),
		policytest.MapRuleHelper("standard", "native-a"),
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 1),
		&policy.AdmissionRule{
			RuleBase:    policy.RuleBase{Route: "standard", Action: policy.ActionAdmission},
			MaxInFlight: 1,
			MaxPending:  &maxPending,
			WaitTimeout: config.Duration{Duration: 50 * time.Millisecond},
		},
	}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	executor := &fakeExecutor{
		do: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
			close(entered)
			select {
			case <-release:
				return []byte(`{"id":"ok","choices":[{"message":{"content":"done"}}]}`), nil
			case <-ctx.Done():
				return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499}
			}
		},
	}
	runner := scheduler.NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), executor)
	defer runner.Close()
	srv := NewServer(compiled, subsystem.NewCatalog(compiled), runner)

	reqBody := `{"model":"standard","messages":[{"role":"user","content":"hi"}]}`
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		srv.ServeHTTP(httptest.NewRecorder(), req)
	}()

	<-entered
	req2 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	srv.ServeHTTP(rec2, req2)

	close(release)
	<-firstDone

	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on full queue, got %d body=%s", rec2.Code, rec2.Body.String())
	}
	if rec2.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on 429 queue rejection")
	}
	if !strings.Contains(rec2.Body.String(), "route_queue_full") {
		t.Fatalf("expected route_queue_full code, got %s", rec2.Body.String())
	}

	// Now verify wait_timeout produces 429 + Retry-After + route_queue_wait_timeout,
	// whereas expired route_timeout during queue wait produces 504 + route_timeout.
	cfg2 := configtest.TestConfig()
	pendingOne := 1
	cfg2.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a"),
		policytest.MapRuleHelper("standard", "native-a"),
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 1),
		&policy.TimeoutRule{
			RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout},
			Duration: policy.Duration{Duration: 2 * time.Second},
		},
		&policy.AdmissionRule{
			RuleBase:    policy.RuleBase{Route: "standard", Action: policy.ActionAdmission},
			MaxInFlight: 1,
			MaxPending:  &pendingOne,
			WaitTimeout: config.Duration{Duration: 25 * time.Millisecond},
		},
	}
	compiled2, err := config.PrepareAndCompile(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	entered2 := make(chan struct{})
	release2 := make(chan struct{})
	executor2 := &fakeExecutor{
		do: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
			close(entered2)
			select {
			case <-release2:
				return []byte(`{"id":"ok","choices":[{"message":{"content":"done"}}]}`), nil
			case <-ctx.Done():
				return nil, &model.CallError{Class: model.ErrorCancelled, Status: 499}
			}
		},
	}
	runner2 := scheduler.NewRunner(compiled2.RoutingPlan(), subsystem.NewCatalog(compiled2), executor2)
	defer runner2.Close()
	srv2 := NewServer(compiled2, subsystem.NewCatalog(compiled2), runner2)

	firstDone2 := make(chan struct{})
	go func() {
		defer close(firstDone2)
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		srv2.ServeHTTP(httptest.NewRecorder(), req)
	}()
	<-entered2

	reqWait := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
	reqWait.Header.Set("Content-Type", "application/json")
	recWait := httptest.NewRecorder()
	srv2.ServeHTTP(recWait, reqWait)

	close(release2)
	<-firstDone2

	if recWait.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on queue wait timeout, got %d body=%s", recWait.Code, recWait.Body.String())
	}
	if recWait.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on 429 queue wait timeout")
	}
	if !strings.Contains(recWait.Body.String(), "route_queue_wait_timeout") {
		t.Fatalf("expected route_queue_wait_timeout code, got %s", recWait.Body.String())
	}

	// Route timeout < admission WaitTimeout -> 504 route_timeout.
	cfg3 := configtest.TestConfig()
	cfg3.Rules = []policy.Rule{
		policytest.FilterModel("standard", "standard"),
		policytest.FilterProvider("standard", "a"),
		policytest.MapRuleHelper("standard", "native-a"),
		policytest.RankRuleHelper("standard"),
		policytest.RaceRuleHelper("standard", 1),
		&policy.TimeoutRule{
			RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout},
			Duration: policy.Duration{Duration: 25 * time.Millisecond},
			Idle:     policy.Duration{Duration: time.Second},
		},
		&policy.AdmissionRule{
			RuleBase:    policy.RuleBase{Route: "standard", Action: policy.ActionAdmission},
			MaxInFlight: 1,
			MaxPending:  &pendingOne,
			WaitTimeout: config.Duration{Duration: time.Second},
		},
	}
	compiled3, err := config.PrepareAndCompile(cfg3)
	if err != nil {
		t.Fatal(err)
	}
	entered3 := make(chan struct{})
	release3 := make(chan struct{})
	executor3 := &fakeExecutor{
		stream: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
			// Keep admission occupied by a selected stream: unlike another
			// non-streaming request, it survives the selection deadline.
			close(entered3)
			events := make(chan model.StreamEvent, 2)
			events <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{"content":"held"}}]}`), Meaningful: true}
			go func() {
				defer close(events)
				select {
				case <-release3:
					events <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{},"finish_reason":"stop"}]}`)}
				case <-ctx.Done():
				}
			}()
			return events, nil
		},
		do: func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError) {
			t.Error("request dispatched after its queue deadline")
			return nil, model.RouteTimeoutError()
		},
	}
	runner3 := scheduler.NewRunner(compiled3.RoutingPlan(), subsystem.NewCatalog(compiled3), executor3)
	defer runner3.Close()
	srv3 := NewServer(compiled3, subsystem.NewCatalog(compiled3), runner3)

	firstDone3 := make(chan struct{})
	go func() {
		defer close(firstDone3)
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(strings.Replace(reqBody, `"model":`, `"stream":true,"model":`, 1)))
		req.Header.Set("Content-Type", "application/json")
		srv3.ServeHTTP(httptest.NewRecorder(), req)
	}()
	<-entered3

	reqRouteTimeout := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
	reqRouteTimeout.Header.Set("Content-Type", "application/json")
	recRouteTimeout := httptest.NewRecorder()
	srv3.ServeHTTP(recRouteTimeout, reqRouteTimeout)

	close(release3)
	<-firstDone3

	if recRouteTimeout.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504 on route timeout, got %d body=%s", recRouteTimeout.Code, recRouteTimeout.Body.String())
	}
	if !strings.Contains(recRouteTimeout.Body.String(), "route_timeout") {
		t.Fatalf("expected route_timeout code, got %s", recRouteTimeout.Body.String())
	}
}
