package scheduler

import (
	"context"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
)

// RequestRun shares one budget across sequential generations of a client
// request. Routing exclusions and generation observations remain independent.
// Its methods must be called sequentially, as in structured.Run's correction loop.
type RequestRun struct {
	runner  *Runner
	logical string
	budget  *RequestBudget
}

func (r *Runner) NewRequestRun(ctx context.Context, logical string) *RequestRun {
	runtime := newRouteRuntime(r.plan.Models[logical])
	if deadline, ok := ctx.Deadline(); ok && (runtime.budget.deadline.IsZero() || deadline.Before(runtime.budget.deadline)) {
		runtime.budget.deadline = deadline
	}
	return &RequestRun{runner: r, logical: logical, budget: runtime.budget}
}

func (r *RequestRun) RunGeneration(ctx context.Context, request model.ExecuteRequest) (*model.RunOutcome, *model.CallError) {
	return r.runner.runGeneration(ctx, r.logical, request, r.budget)
}

func (r *RequestRun) RunGenerationStream(ctx context.Context, request model.ExecuteRequest, started time.Time, sink StreamSink) *model.RunOutcome {
	return r.runner.runStreamWithBudget(ctx, r.logical, request, started, sink, true, r.budget)
}

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
