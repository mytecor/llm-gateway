package scheduler

import (
	"context"
	"testing"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/testsupport/configtest"
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"
)

func TestGenerationPreservesFailedAttemptMetadataAndPermits(t *testing.T) {
	cfg := configtest.RulesConfig(t, 1, policytest.EntryRules([]string{"a"}, 1))
	runner := NewRunner(cfg.RoutingPlan(), subsystem.NewCatalog(cfg), &fakeExecutor{do: func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError) {
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 502}
	}})
	t.Cleanup(func() { _ = runner.Close() })
	outcome, err := runner.RunGeneration(context.Background(), "standard", model.ExecuteRequest{Kind: model.RequestChat})
	if err == nil || outcome == nil || outcome.Attempts < 1 || outcome.HasUsage {
		t.Fatalf("failure metadata: %+v %v", outcome, err)
	}
	assertStreamRunReleased(t, runner, cfg)
}
