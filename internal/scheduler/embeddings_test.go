package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/driverproto/embeddings"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

func TestEmbeddingCompatibilityAcrossRecoveryAndCooling(t *testing.T) {
	for _, scenario := range []string{"fallback", "retry", "hedge", "cooling", "no-compatible"} {
		t.Run(scenario, func(t *testing.T) {
			entry := &policy.CompiledRoute{Name: "embedding", Pool: []model.Target{{Provider: "text", Model: "native"}}, RaceCount: 1, RouteTimeout: time.Second}
			full := &policy.CompiledRoute{Name: "full", Pool: []model.Target{{Provider: "chat", Model: "native"}, {Provider: "multimodal", Model: "native"}}, RaceCount: 1}
			switch scenario {
			case "fallback":
				entry.FallbackTarget = full
			case "retry":
				entry.Retry.Attempts = 1
				entry.RetryTarget = full
			case "hedge":
				entry.Hedge = policy.HedgeConfig{Enabled: true, Target: "full", Delay: time.Millisecond}
				entry.HedgeTarget = full
			case "cooling":
				entry.Pool = append(entry.Pool, full.Pool...)
			}
			plan := policy.RuntimePlan{Models: map[string]*policy.CompiledRoute{"embedding": entry}, Providers: map[string]policy.RuntimeProvider{
				"text": {Capabilities: &model.Capabilities{Embeddings: true}}, "chat": {Capabilities: &model.Capabilities{Chat: true}}, "multimodal": {Capabilities: &model.Capabilities{Embeddings: true, EmbeddingImages: true, EmbeddingAudio: true, EmbeddingVideo: true}},
			}}
			calls := 0
			exec := &fakeExecutor{do: func(_ context.Context, target model.Target, _ model.ExecuteRequest) ([]byte, *model.CallError) {
				calls++
				if target.Provider != "multimodal" {
					t.Error("incompatible target dispatched")
				}
				return nil, &model.CallError{Status: 503, Class: model.ErrorUpstream}
			}}
			runner := NewRunner(plan, subsystem.NewCatalogWithSources(nil), exec)
			defer runner.Close()
			if scenario == "cooling" {
				runner.cooling[cooldownKey{provider: "multimodal", model: "native"}] = time.Now().Add(time.Minute)
			}
			_, err := runner.Run(context.Background(), "embedding", model.ExecuteRequest{Kind: model.RequestEmbeddings, EmbeddingFeatures: embeddings.Features{Images: true, Audio: true, Video: true}})
			if err == nil {
				t.Fatal("fixture unexpectedly succeeded")
			}
			if scenario == "no-compatible" && calls != 0 {
				t.Fatal("incompatible request was executed")
			}
			if scenario != "no-compatible" && calls != 1 {
				t.Fatalf("compatible recovery not used: %d", calls)
			}
		})
	}
}
