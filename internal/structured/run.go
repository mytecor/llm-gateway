package structured

import (
	"context"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
)

// Generation is one complete routing/stream run, including reported usage even
// when the run fails. Execute owns admission and provider attempt accounting.
type Generation struct {
	Outcome model.RunOutcome
	Events  []model.StreamEvent
}

type Execute func(context.Context, model.ExecuteRequest) (*Generation, *model.CallError)

type Observation struct {
	Generation int
	Outcome    model.RunOutcome
	Status     string
}

func Deadline(ctx context.Context, started time.Time, routeTimeout time.Duration) (context.Context, context.CancelFunc) {
	deadline, hasClient := ctx.Deadline()
	if routeTimeout > 0 {
		routeDeadline := started.Add(routeTimeout)
		if !hasClient || routeDeadline.Before(deadline) {
			deadline = routeDeadline
		}
	} else if !hasClient {
		deadline = started.Add(30 * time.Minute)
	}
	return context.WithDeadline(ctx, deadline)
}

// Run owns the private history and budget. A stateful request carries an
// Exchange already acquired by its caller; HTTP history is copied on correction.
func Run(ctx context.Context, c *Contract, request model.ExecuteRequest, execute Execute, observe func(Observation)) (generation *Generation, callError *model.CallError) {
	var usage model.UsageSummary
	hasUsage := false
	attempts := 0
	var final *Generation
	defer func() {
		if generation != nil {
			generation.Outcome.Usage = usage
			generation.Outcome.HasUsage = hasUsage
			generation.Outcome.Attempts = attempts
		}
	}()
	if request.Exchange != nil {
		defer request.Exchange.Abort()
		request.ResponseSchema = c.Schema
	}
	for i := 0; i <= MaxCorrections; i++ {
		if err := ctx.Err(); err != nil {
			return final, contextError(ctx)
		}
		result, callErr := execute(ctx, request)
		if result != nil {
			attempts += result.Outcome.Attempts
			usage.Input += result.Outcome.Usage.Input
			usage.Output += result.Outcome.Usage.Output
			usage.CachedTokens += result.Outcome.Usage.CachedTokens
			hasUsage = hasUsage || result.Outcome.HasUsage
			final = result
		}
		if ctx.Err() != nil {
			return final, contextError(ctx)
		}
		if callErr != nil {
			return final, callErr
		}
		if result == nil {
			return nil, Error(502, "invalid_upstream_response")
		}
		if len(result.Outcome.Body) > MaxBufferBytes {
			return final, Error(502, "structured_output_buffer_exceeded")
		}
		content, refusal, callErr := Answer(result.Outcome.Body)
		if callErr != nil {
			return final, callErr
		}
		var validation error
		if !refusal {
			validation = c.Validate(content)
		}
		status := "invalid"
		if refusal {
			status = "refusal"
		} else if validation == nil {
			status = "valid"
		}
		if observe != nil {
			observe(Observation{Generation: i, Outcome: result.Outcome, Status: status})
		}
		if refusal || validation == nil {
			if ctx.Err() != nil {
				return final, contextError(ctx)
			}
			if request.Exchange != nil {
				if err := request.Exchange.Commit(content); err != nil {
					return final, err
				}
			}
			result.Outcome.Usage, result.Outcome.HasUsage, result.Outcome.Attempts = usage, hasUsage, attempts
			result.Outcome.Body = WithUsage(result.Outcome.Body, usage, hasUsage)
			return result, nil
		}
		if i == MaxCorrections {
			return final, Error(502, "structured_output_validation_failed")
		}
		feedback := Feedback(validation)
		if request.Exchange != nil {
			request.Correction = feedback
		} else {
			body, err := appendFeedback(request.Body, content, feedback)
			if err != nil {
				return final, Error(400, "invalid_request")
			}
			request.Body = body
		}
	}
	panic("unreachable correction loop")
}

func contextError(ctx context.Context) *model.CallError {
	if ctx.Err() == context.DeadlineExceeded {
		return &model.CallError{Class: model.ErrorTimeout, Status: 504, Code: "structured_output_timeout", Scope: model.FailureScopeRequest}
	}
	return &model.CallError{Class: model.ErrorCancelled, Status: 499, Code: "request_cancelled", Scope: model.FailureScopeRequest}
}
