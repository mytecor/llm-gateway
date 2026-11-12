package scheduler

import (
	"context"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
)

// BeginExchange reserves the native conversation of a compiler-validated fixed
// stateful route. Routing and provider capacity still run for each generation.
func (r *Runner) BeginExchange(ctx context.Context, logical string, request model.ExecuteRequest) (model.Exchange, *model.CallError) {
	entry := r.plan.Models[logical]
	if entry == nil || !entry.Stateful || len(entry.Pool) != 1 {
		return nil, &model.CallError{Class: model.ErrorInvalid, Status: 400, Code: "unsupported_exchange", Scope: model.FailureScopeRequest}
	}
	if executor, ok := r.executor.(interface {
		BeginExchange(context.Context, model.Target, model.ExecuteRequest) (model.Exchange, *model.CallError)
	}); ok {
		return executor.BeginExchange(ctx, entry.Pool[0], request)
	}
	return nil, &model.CallError{Class: model.ErrorInvalid, Status: 400, Code: "unsupported_exchange", Scope: model.FailureScopeRequest}
}

// RunGenerationStream retains the normal routing/stream machinery, but leaves
// client-level observation to the owner of the multi-generation exchange.
// Token and branch observations still belong to the actual provider attempts.
func (r *Runner) RunGenerationStream(ctx context.Context, logical string, request model.ExecuteRequest, started time.Time, sink StreamSink) *model.RunOutcome {
	return r.runStream(ctx, logical, request, started, sink, true)
}
