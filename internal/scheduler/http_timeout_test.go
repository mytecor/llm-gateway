package scheduler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/executor"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"
)

// Exercise selection and cancellation across the actual NDJSON process boundary.
// Both a silent header wait and a role-only stream must release the physical
// primary request; the selected backup must survive its own selection timeout.
func TestHTTPStreamSelectionTimeoutCancelsUpstreamAndRetries(t *testing.T) {
	for _, prelude := range []bool{false, true} {
		t.Run(fmt.Sprintf("role_prelude=%t", prelude), func(t *testing.T) {
			cancelled := make(chan struct{})
			var primaryCalls, backupCalls atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				primaryCalls.Add(1)
				if prelude {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"id\":\"primary\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n")
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
				close(cancelled)
			}))
			defer primary.Close()
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				backupCalls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"id\":\"backup\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"backup\"}}]}\n\n")
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					return
				case <-time.After(300 * time.Millisecond):
				}
				_, _ = io.WriteString(w, "data: {\"id\":\"backup\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
			}))
			defer backup.Close()
			cfg := config.Config{Providers: []provider.Provider{
				{ID: "a", Priority: 20, RequestTimeout: config.Duration{Duration: 100 * time.Millisecond}, Config: map[string]any{"inference_url": primary.URL, "allow_private_network": true}},
				{ID: "b", Priority: 10, RequestTimeout: config.Duration{Duration: 100 * time.Millisecond}, Config: map[string]any{"inference_url": backup.URL, "allow_private_network": true}},
			}}
			cfg.Rules = append(policytest.EntryRules([]string{"a", "b"}, 1),
				&policy.TimeoutRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionTimeout}, Duration: policy.Duration{Duration: 2 * time.Second}},
				&policy.RetryRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionRetry}, Attempts: 1},
				&policy.SemaphoreRule{RuleBase: policy.RuleBase{Route: "standard", Action: policy.ActionSemaphore}, MaxCalls: 2, MaxInFlight: 1, MaxCallsPerProvider: 1},
			)
			compiled, err := config.PrepareAndCompile(cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			drivers, err := executor.New(ctx, compiled)
			if err != nil {
				t.Fatal(err)
			}
			defer drivers.Close()
			runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), drivers)
			defer runner.Close()
			selected, callErr := runner.SelectStream(ctx, "standard", model.ExecuteRequest{Kind: model.RequestChat, Body: []byte(`{"messages":[{"role":"user","content":"hi"}],"stream":true}`)})
			if callErr != nil {
				t.Fatalf("backup selection failed: %+v", callErr)
			}
			defer selected.Cancel()
			if selected.Provider != "b" {
				t.Fatalf("selected %q instead of backup", selected.Provider)
			}
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("primary HTTP request remained active after retry")
			}
			finished := false
			for event := range selected.Remaining {
				if event.Err != nil {
					t.Fatalf("selected backup stream failed: %+v", event.Err)
				}
				finished = finished || strings.Contains(string(event.Data), `"finish_reason":"stop"`)
			}
			if !finished || primaryCalls.Load() != 1 || backupCalls.Load() != 1 {
				t.Fatalf("unexpected stream outcome: finished=%t primary=%d backup=%d", finished, primaryCalls.Load(), backupCalls.Load())
			}
		})
	}
}

func TestHTTPBudgetDisablesInternalRetries(t *testing.T) {
	for _, kind := range []model.RequestKind{model.RequestChat, model.RequestResponses} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("kind=%s/stream=%t", kind, streaming), func(t *testing.T) {
				var calls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					calls.Add(1)
					w.WriteHeader(http.StatusServiceUnavailable)
				}))
				defer upstream.Close()
				zero := 0
				cfg := config.Config{
					Providers: []provider.Provider{{ID: "a", Config: map[string]any{"inference_url": upstream.URL, "allow_private_network": true, "bifrost_max_retries": 1}}},
					Models:    map[string]policy.ModelSpec{"standard": {Budget: &policy.BudgetSpec{MaxCalls: 1, MaxInFlight: 1, MaxCallsPerProvider: 1, MaxHedges: &zero}, Tiers: []policy.TierSpec{{Targets: []policy.TargetSpec{{Provider: "a", Native: "m"}}}}}},
				}
				compiled, err := config.PrepareAndCompile(cfg)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				drivers, err := executor.New(ctx, compiled)
				if err != nil {
					t.Fatal(err)
				}
				defer drivers.Close()
				runner := NewRunner(compiled.RoutingPlan(), subsystem.NewCatalog(compiled), drivers)
				defer runner.Close()
				body := []byte(`{"messages":[{"role":"user","content":"hi"}]}`)
				if kind == model.RequestResponses {
					body = []byte(`{"input":"hi"}`)
				}
				request := model.ExecuteRequest{Kind: kind, Body: body}
				var callErr *model.CallError
				if streaming {
					_, callErr = runner.SelectStream(ctx, "standard", request)
				} else {
					_, callErr = runner.Run(ctx, "standard", request)
				}
				if callErr == nil || callErr.Status != 503 || calls.Load() != 1 {
					t.Fatalf("physical budget bypassed: calls=%d error=%+v", calls.Load(), callErr)
				}
			})
		}
	}
}
