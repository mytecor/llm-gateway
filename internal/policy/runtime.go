package policy

import "github.com/mytecor/llm-gateway/internal/model"

// RuntimeRule evaluates an incremental event. Instances belong to one client
// run and are called synchronously by its owner; compiled policies remain
// immutable and may be shared by concurrent runs.
type RuntimeRule interface {
	Evaluate(RuleContext, RuleEvent) RuleDecision
}

// RuleContext contains metadata and a compact snapshot of the current output.
// OriginalRequest provides read-only access to the original request when a
// rule needs it. Message history is neither copied nor replayed per event.
type RuleContext struct {
	Kind            model.RequestKind
	LogicalModel    string
	Provider        string
	Model           string
	Attempts        int
	Output          RuleOutput
	OriginalRequest func() model.ExecuteRequest
}

type RuleOutput struct {
	Finished          bool
	HasText           bool
	HasToolCalls      bool
	CompleteToolCalls bool
}

type RuleEventKind uint8

const (
	// RuleStreamStart starts a provider stream, including a continuation.
	RuleStreamStart RuleEventKind = iota
	RuleDelta
	RuleFailure
	RuleStreamEnd
)

// RuleEvent carries only the new input. Delta is text extracted from one
// upstream event (content and reasoning), never the accumulated response.
// Failures include idle timeouts and streams closed without a finish reason.
type RuleEvent struct {
	Kind  RuleEventKind
	Delta string
	Error *model.CallError
}

type RuleDecisionAction uint8

const (
	RulePass RuleDecisionAction = iota
	RuleStop
	RuleRecover
	RuleFinishToolCalls
)

// RuleDecision requests an effect; only the scheduler performs cancellation,
// dispatch, history resharing, transport writes and resource release. The zero
// value passes the event through unchanged.
type RuleDecision struct {
	Action RuleDecisionAction
	Rule   string
	Error  *model.CallError
}
