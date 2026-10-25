package policy

import (
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
)

// CompiledRoute is the compiled, immutable routing-graph node for one named
// route. External Nix/JSON rules are fully normalized into this structure at
// compile time; the runtime scheduler consumes only these immutable routes and
// never touches the JSON DTOs. Target names are resolved to pointers at the
// compile stage, so a missing target or a routing cycle already fails there.
type CompiledRoute struct {
	// Stateful routes are bound to one native target and cannot be replayed.
	Stateful bool
	Name     string
	// Entry is true when the route carries a request-level model filter: it is
	// a discoverable logical entry route. Subroutes (retry/fallback/hedge
	// targets) are not Entry and never become logical models.
	Entry   bool
	ModelEq string // request-level model filter, non-empty for entry routes
	// ErrorIn gates a transition into this route on the incoming terminal
	// failure class; empty means the route is applicable on any failure.
	ErrorIn map[model.ErrorClass]bool
	// AttemptLT gates entry into this route on the current attempt number
	// (0 >= AttemptLT blocks the transition). Zero means no bound.
	AttemptLT int
	// ProviderUnused restricts the compiled pool at runtime to providers not
	// yet used by the current request graph (explicit routing policy).
	ProviderUnused bool
	// Pool is the ranked candidate target pairs of the route after the
	// filter/map/rank sequence; an immutable snapshot kept at the race action.
	Pool []model.Target
	// RaceCount is the size of the route race batch; 0 means all pool
	// candidates.
	RaceCount int
	Admission AdmissionConfig
	Lease     LeaseConfig
	Balance   BalanceConfig
	Affinity  AffinityConfig
	Retry     RetryConfig
	Fallback  FallbackConfig
	Hedge     HedgeConfig
	Semaphore SemaphoreConfig
	// Continue is the in-gateway stream takeover policy (see ContinueConfig).
	Continue ContinueConfig
	// Repetition is the in-gateway loop-guard policy (see RepetitionConfig).
	// Absent (Disabled) means no loop detection, exactly like Continue.
	Repetition RepetitionConfig
	// RouteTimeout bounds the entire route graph.
	RouteTimeout time.Duration
	// StreamIdleTimeout bounds per-chunk silence on the relayed winner stream.
	StreamIdleTimeout time.Duration

	// Resolved transitions (compile stage).
	RetryTarget    *CompiledRoute
	FallbackTarget *CompiledRoute
	HedgeTarget    *CompiledRoute
}

// Applicable evaluates the destination-owned applicability of the route for an
// incoming terminal failure at the given attempt index. An empty error filter
// applies to any failure; a failure matching no class makes the route not
// applicable, which must never mask the original terminal failure.
func (rt *CompiledRoute) Applicable(callErr *model.CallError, attempt int) bool {
	if callErr == nil {
		return true
	}
	if len(rt.ErrorIn) > 0 && !rt.ErrorIn[callErr.Class] {
		return false
	}
	if rt.AttemptLT > 0 && attempt >= rt.AttemptLT {
		return false
	}
	return true
}

func (rt *CompiledRoute) applicable(callErr *model.CallError, attempt int) bool {
	return rt.Applicable(callErr, attempt)
}

// RetryConfig is a bounded repeated transition to a named route. The retry
// condition lives in the target route's own filter; here only the lifecycle
// (attempts, backoff) plus the unresolved target name at decode time.
type RetryConfig struct {
	Target   string // resolved to retryTarget at compile stage
	Attempts int
	Internal bool // true when Target is empty (in-route scheduler retry)
	Backoff  BackoffConfig
}

// FallbackConfig is a one-shot transition to a named route.
type FallbackConfig struct {
	Target string // resolved to fallbackTarget at compile stage
}

// HedgeConfig is a latency transition: after the delay the target route's
// batch may start while the current route is still executing.
type HedgeConfig struct {
	After    time.Duration
	Target   string // resolved to hedgeTarget at compile stage
	Internal bool   // true when Target is empty (in-route scheduler dynamic hedge)
}

type SemaphoreConfig struct {
	MaxCalls            int
	MaxInFlight         int
	MaxCallsPerProvider int
}

// ContinueConfig is the in-gateway stream takeover policy for a route. When
// the relayed winner stream stalls (idle for longer than Idle, or closes
// without a finish_reason) after having produced meaningful content or
// reasoning, the gateway does not surface an error and stop: instead it
// continues the same client stream by re-dispatching the request to a
// different provider. Reshare controls how the partial output already relayed
// is passed to the next provider ("full" appends all received reasoning and
// content as assistant context so the continuation preserves the work).
// Enabled true means the action was declared on the entry route.
type ContinueConfig struct {
	Enabled bool
	// Idle is the stall threshold below which the takeover triggers. It
	// replaces the route-wide stream idle timeout for the relayed winner.
	Idle time.Duration
	// Reshare is the partial-output handoff mode: "full" (default) reshapes
	// every relayed reasoning/content delta into an assistant message appended
	// to the request history before re-dispatching.
	Reshare string
	// Retries is how many times a fully-exhausted chain (the takeover has no
	// eligible provider left because every provider in the pool already broke
	// during the request) is re-dispatched from the top with the accumulated
	// partial output reshared. Each retry is a fresh pass over the whole pool
	// (cooldown still gates the just-broken providers), so a provider that was
	// unlucky in an earlier round gets another chance instead of the request
	// surfacing a terminal failure once the chain runs out. Bounded by
	// maxContinueChainRetries so a misconfigured rule cannot spin the gateway
	// across dozens of full-pass re-dispatches.
	Retries int
	// Wait is the bounded horizon the gateway holds the relayed stream open
	// while the provider pool recovers from an exhausted takeover, instead of
	// surfacing a terminal error the client would see. During the wait the
	// gateway sends periodic SSE keep-alives (so client-side watchdogs do not
	// kill the silent stream) and re-attempts the continuation until a
	// provider recovers, the client disconnects, or the horizon expires. At
	// expiry the terminal error is surfaced as a last resort. A positive
	// horizon is always derived from the continue rule (see ContinueRule.Wait:
	// omitted/zero becomes continueDefaultWait), so in practice an active
	// continue rule never surfaces the terminal error early — the hold is
	// on. The <= 0 guard in the relay is defensive only.
	Wait time.Duration
}

// RepetitionConfig is the in-gateway loop-guard policy for a route. When the
// relayed winner stream's accumulated output begins self-repeating (the same
// normalized fragment emitted K+ consecutive times with no finish) — the
// "Tool call. Tool call. …" looping observed on the `standard` model — the
// gateway stops the stream and re-dispatches through the existing continue
// path with the accumulated partial output reshared, instead of relaying the
// loop to the client until it exhausts the provider. Enabled true means the
// repetition action was declared on the entry route; absent always means
// detection disabled, exactly like continue.
type RepetitionConfig struct {
	Enabled bool
	// Repeats is K: the number of consecutive identical normalized fragments
	// that trip the guard. Must be >= 2; a lone repeated word or phrase in
	// otherwise live speech is never a loop.
	Repeats int
	// MinLen is the smallest normalized fragment length considered. Shorter
	// fragments ("ha", "the", "}") are noise and never trip the guard, so
	// natural short repeats and barrier-character runs pass through and only
	// a genuine phrase-level loop fires.
	MinLen int
	// MaxLen is the largest normalized fragment length scanned; it bounds the
	// per-event tail window the detector examines.
	MaxLen int
}

// continueDefaultWait is the horizon for ContinueConfig.Wait when a continue
// rule is active but no explicit wait is configured. It bounds how long the
// gateway keeps a held stream open across a total provider-pool outage before
// it finally surfaces the terminal error.
const continueDefaultWait = 10 * time.Minute

// continueRecoveryRecheck is how often the gateway re-attempts a
// continuation while holding a stream open for provider-pool recovery. It is
// deliberately shorter than the provider cooldown so a provider whose window
// expires is re-raced promptly, without hammering the pool between
// re-checks. A var (not a const) so tests can shrink the cadence; only tests
// write to it.
var ContinueRecoveryRecheck = 5 * time.Second
var continueRecoveryRecheck = ContinueRecoveryRecheck

// continueRecoveryKeepAlive is the SSE keep-alive cadence sent while the
// gateway holds a stream open for provider-pool recovery. It is short enough
// to keep client-side stream watchdogs (e.g. pi's stall watchdog and the
// gateway's own idle timeout) from killing the silent stream, and cheap: an
// SSE comment (": ping") carries no payload and no choice semantics. It
// overrides the route-wide idle timeout while a stream is held, exactly like
// the continue Idle threshold does during a normal relay. A var (not a const)
// so tests can shrink the cadence; only tests write to it.
var ContinueRecoveryKeepAlive = 15 * time.Second
var continueRecoveryKeepAlive = ContinueRecoveryKeepAlive

// maxContinueChainRetries caps the whole-chain retry budget of a continue
// rule. A retry is a full re-dispatch of the whole pool with a freshly
// rewritten (partial-appended) request body, so an unbounded value would let
// a single request hammer every provider repeatedly under sustained outages;
// the cap keeps the worst-case per-request work finite while still leaving
// headroom for a real outage to ride out.
const maxContinueChainRetries = 10

type LeaseConfig struct {
	Enabled                bool
	Source                 string // "winner"
	Duration               time.Duration
	RenewOnSuccess         bool
	ReleaseOn              map[model.ErrorClass]bool
	ReleaseAfterSlowStarts int
	SlowStart              time.Duration
}

// BalanceConfig is the compiled balance policy for one route. Unlike lease it
// is applied to every request (there is no winner to reward yet): it selects
// which provider the route's race starts with, so a `race count = 1` executes
// exactly the balanced choice and a larger batch includes it first. The store
// behind it is per-provider and global, so health is shared by every route.
// The strategies:
//
//	p2c          — power of two choices by live in-flight branch count;
//	round_robin — a per-route cursor rotates over the healthy candidates;
//	adaptive    — weighted-random by static weight × health(p) ∈ [0,1];
//	weighted    — only the static weights, no health history.
type BalanceConfig struct {
	Enabled         bool
	Strategy        string // "p2c" | "round_robin" | "adaptive" | "weighted" | "expected-ttft"
	Weights         map[string]int
	Window          time.Duration
	ErrorBudget     float64
	ExplorationRate float64
}

type AffinityConfig struct {
	Enabled           bool
	Sources           []string
	TTL               time.Duration
	OnMissing         string // "ignore"
	OnProviderFailure string // "fail-closed"
}
