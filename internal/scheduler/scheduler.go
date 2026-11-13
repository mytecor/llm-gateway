package scheduler

import (
	"context"
	"errors"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/telemetry"
	"github.com/mytecor/llm-gateway/internal/transform"
	"log/slog"
	"sync"
	"time"
)

// branchResult is the terminal outcome of one upstream call produced by a
// branch goroutine (or a locally catalog-rejected target reported without an
// upstream call, see deliverPreFailed). It is delivered exactly once through
// the schedule results channel.
type branchResult struct {
	id        int
	provider  string
	model     string
	winner    bool
	prefailed bool // catalog-rejected before dispatch: no upstream call, no semaphore slot
	body      []byte
	selected  *SelectedStream
	started   time.Time
	finished  time.Time
	err       *model.CallError
}

// routeOutcome carries the winner and its payload out of the scheduler.
// model names the winner's native provider model (set only for a successful
// selection); failedProvider is recorded separately for fallback
// observability. pinned marks a route narrowed by a known affinity mapping so
// the caller can keep the request fail-closed (no transitions). empty marks a
// transition route whose dynamic pool had no available targets: the transition
// is not applicable and must never mask the original terminal failure.
type routeOutcome struct {
	provider        string
	model           string
	body            []byte
	selected        *SelectedStream
	err             *model.CallError
	pinned          bool
	empty           bool
	fallbackOnly    bool
	failedProvider  string
	failedModel     string
	failedTargets   map[targetKey]bool
	capacityBlocked bool
	// attempts counts route executions dispatched for the request (races plus
	// retry/fallback rounds); ttft is the winner's time to first meaningful
	// event (stream) or the full response duration (non-stream).
	attempts int
	ttft     time.Duration
}

// emptyPoolError is the distinguishable terminal result of a transition route
// whose dynamic pool has no available targets for the current request (for
// example every provider is already used by this request graph).
func emptyPoolError() *model.CallError {
	return &model.CallError{Class: model.ErrorInvalid, Status: 502, Cause: errors.New("route has no available targets")}
}

// targetKey identifies a concrete (provider, model) pair in the request graph.
type targetKey struct {
	provider string
	model    string
}

// routeRuntime owns request-wide state shared by the whole route graph.
// Its deadline and RequestBudget counters plus the used-provider set are
// monotonic for the request and never reset when entering a subroute or tier.
type routeRuntime struct {
	deadline time.Time
	budget   *RequestBudget
	used     map[string]bool
	// exclude is a request-scoped, provider-independent exclusion set seeded by
	// a continuation (the "continue" rule): providers in this set must never
	// be re-dispatched, regardless of the route's unused policy. It is empty for
	// fresh requests and only populated by ContinueStream.
	exclude        map[string]bool
	excludeTargets map[targetKey]bool
	// strictAvailability disables the availability fail-open: when every
	// candidate is cooling, a strict runtime returns no targets instead of
	// re-adding the whole (cooling) pool. Continuations must never re-race a
	// provider that is still cooling — a cooling provider just broke, and
	// fail-open would make the continuation re-hit the upstream it exists to
	// flee, burning the chain-retry budget on known-dead carriers. Fresh
	// requests keep fail-open so a single cooling provider never idles a route.
	strictAvailability bool
	// attempts counts the route executions actually dispatched by this request
	// graph (each raceRoute that launched branches), so request_completed and
	// llm_attempt events can report how many upstream rounds happened.
	attempts int
}

func newRouteRuntime(entry *policy.CompiledRoute) *routeRuntime {
	return newRouteRuntimeWithBudget(entry, nil)
}

func newRouteRuntimeWithBudget(entry *policy.CompiledRoute, budget *RequestBudget) *routeRuntime {
	runtime := &routeRuntime{
		used:           make(map[string]bool),
		exclude:        make(map[string]bool),
		excludeTargets: make(map[targetKey]bool),
	}
	var deadline time.Time
	if entry != nil && entry.RouteTimeout > 0 {
		deadline = time.Now().Add(entry.RouteTimeout)
	}
	if budget == nil {
		fresh := newRequestBudget(entry, deadline)
		budget = &fresh
	}
	runtime.budget = budget
	runtime.deadline = budget.deadline
	return runtime
}

func (r *routeRuntime) expired() bool {
	return r.budget.expired()
}

// Call counters never recover with time. Avoid holding a client stream open
// when no reachable provider can obtain another request-wide call permit.
func (r *routeRuntime) hasCallBudget(entry *policy.CompiledRoute) bool {
	return r.budget.hasCallBudget(entry)
}

func (r *routeRuntime) deadlineTimer() (<-chan time.Time, func()) {
	if r.deadline.IsZero() {
		return nil, func() {}
	}
	duration := time.Until(r.deadline)
	if duration < 0 {
		duration = 0
	}
	timer := time.NewTimer(duration)
	return timer.C, func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func routeTimeoutError() *model.CallError {
	return model.RouteTimeoutError()
}

// waitBackoff sleeps without letting a retry backoff hide the route deadline.
// The injected sleeper keeps scheduler timing tests deterministic.
func (r *routeRuntime) waitBackoff(ctx context.Context, duration time.Duration, sleep func(context.Context, time.Duration) error) *model.CallError {
	if r.expired() {
		return model.RouteTimeoutError()
	}
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sleep(waitCtx, duration) }()
	deadline, stopDeadline := r.deadlineTimer()
	defer stopDeadline()
	select {
	case err := <-done:
		if err == nil {
			return nil
		}
		if errors.Is(err, context.DeadlineExceeded) || r.expired() {
			return model.RouteTimeoutError()
		}
		return &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: err}
	case <-deadline:
		return model.RouteTimeoutError()
	case <-ctx.Done():
		return streamSelectionContextError(ctx)
	}
}

// failureAggregate collects the terminal failures of one route execution and
// selects the externally observed error deterministically, independent of the
// completion order of the branch goroutines. A non-retryable failure dominates
// a retryable one; ties use a stable class priority and finally launch order.
type failureAggregate struct {
	count      int
	selectedID int
	provider   string
	model      string
	selected   *model.CallError
	targets    map[targetKey]bool
}

func (f *failureAggregate) add(id int, provider, nativeModel string, callErr *model.CallError, retryable func(model.ErrorClass) bool) {
	if callErr == nil {
		callErr = &model.CallError{Class: model.ErrorInvalid, Status: 502}
	}
	candidateRetryable := retryable != nil && retryable(callErr.Class)
	if f.selected == nil || preferFailure(callErr, candidateRetryable, id, f.selected, retryable != nil && retryable(f.selected.Class), f.selectedID) {
		f.selected = callErr
		f.selectedID = id
		f.provider = provider
		f.model = nativeModel
	}
	if provider != "" && nativeModel != "" {
		if f.targets == nil {
			f.targets = make(map[targetKey]bool)
		}
		f.targets[targetKey{provider: provider, model: nativeModel}] = true
	}
	f.count++
}

func (f *failureAggregate) err() *model.CallError {
	if f.selected != nil {
		return f.selected
	}
	return &model.CallError{Class: model.ErrorInvalid, Status: 502}
}

// preferFailure makes the externally observed error independent of goroutine
// completion order. A non-retryable failure dominates a retryable one; ties
// use a stable class priority and finally branch launch order.
func preferFailure(candidate *model.CallError, candidateRetryable bool, candidateID int, current *model.CallError, currentRetryable bool, currentID int) bool {
	if candidateRetryable != currentRetryable {
		return !candidateRetryable
	}
	candidatePriority := failurePriority(candidate.Class)
	currentPriority := failurePriority(current.Class)
	if candidatePriority != currentPriority {
		return candidatePriority > currentPriority
	}
	return candidateID < currentID
}

func failurePriority(class model.ErrorClass) int {
	switch class {
	case model.ErrorInvalid:
		return 7
	case model.ErrorNotFound:
		return 6
	case model.ErrorRateLimit:
		return 5
	case model.ErrorUpstream:
		return 4
	case model.ErrorConnection:
		return 3
	case model.ErrorTimeout:
		return 2
	case model.ErrorCancelled:
		return 1
	default:
		return 0
	}
}

// schedule owns the per-request branch launcher of one route execution: the
// route's race batch (group 0) plus, when configured and armed, the hedge
// target batch (group 1). It is owned by the single scheduler goroutine except
// for branch goroutines sending on results.
type activeBranch struct {
	provider    string
	started     time.Time
	group       int
	observation *subsystem.HedgeObservation
	probe       probeLease
	timing      adaptiveAttemptPlan
}

type schedule struct {
	r       *Runner
	ctx     context.Context
	logical string
	route   *policy.CompiledRoute
	groups  [][]model.Target
	// armed[g] says whether group g may launch: the race group is armed from
	// the start, the hedge group only after its delay elapses.
	armed []bool
	// excludeUsed[g] skips members whose provider is already used by the
	// request graph (the hedge target's unused routing policy, applied at
	// launch time because the used set grows while the source races).
	excludeUsed []bool
	// hedgeG limits how many hedge members may actually launch: the unused
	// policy is applied at launch time, so the hedge group carries the full
	// ordered pool and the race count caps the launches.
	limits         []int
	dispatched     []int
	started        [][]bool
	launched       []bool
	request        model.ExecuteRequest
	stream         bool
	pinned         bool
	results        chan *branchResult
	cancels        map[int]context.CancelFunc
	activeBranches map[int]activeBranch
	budget         *RequestBudget
	runtime        *routeRuntime
	active         int
	seq            int
	hedgeG         int
	hedgeC         <-chan time.Time
	hedgeTimer     *time.Timer
	capacityDenied bool
	halfOpenBusy   bool
}

// launch starts the not-yet-started members of group g that still have a
// semaphore permit. Members denied a permit stay pending: the group is marked
// exhausted only when every member has started, so a later slot release (or
// the hedge) can start the denied members instead of dropping them silently.
// Locally catalog-rejected members are reported as pre-failed results without
// an upstream call or a semaphore slot. Returns whether any branch started or
// was reported pre-failed.
func (sc *schedule) hasEligibleTargetAfter(g, afterIdx int) bool {
	if g < 0 || g >= len(sc.groups) {
		return false
	}
	for j := afterIdx + 1; j < len(sc.groups[g]); j++ {
		if sc.started[g][j] {
			continue
		}
		candidate := sc.groups[g][j]
		if sc.excludeUsed[g] && sc.runtime.used[candidate.Provider] {
			continue
		}
		if sc.r.targetEligible(candidate.Provider, candidate.Model) {
			return true
		}
	}
	return false
}

func (sc *schedule) launch(g int) bool {
	if sc.ctx.Err() != nil || sc.runtime.expired() || g < 0 || g >= len(sc.groups) || !sc.armed[g] || sc.launched[g] {
		return false
	}
	started := false
	delivered := false
	allStarted := true
	launchedN := sc.dispatched[g]
	limit := len(sc.groups[g])
	if sc.limits[g] > 0 && sc.limits[g] < limit {
		limit = sc.limits[g]
	}
	for i, target := range sc.groups[g] {
		if sc.started[g][i] {
			continue
		}
		if sc.excludeUsed[g] && sc.runtime.used[target.Provider] {
			// The member's provider is already used by the request graph: the
			// unused routing policy of this group permanently skips it.
			sc.started[g][i] = true
			continue
		}
		if launchedN >= limit {
			// Beyond the race-count limit of this group: consume the member so
			// the group can be exhausted. The amount actually launched is
			// bounded by the race count even when earlier members were skipped
			// by the unused policy.
			sc.started[g][i] = true
			continue
		}
		// Exact catalog validation happens before any semaphore slot is
		// consumed: a locally rejected target (model_not_found in the
		// snapshot, or an explicit catalog without a snapshot) is not an
		// upstream call, so the call budgets stay available for a different
		// native alias of the same provider in a later route.
		if callErr := sc.r.validateTarget(target); callErr != nil {
			sc.started[g][i] = true
			delivered = true
			sc.seq++
			sc.deliverPreFailed(sc.seq, target, callErr)
			launchedN++
			sc.dispatched[g]++
			continue
		}
		allowFailOpen := sc.route.FallbackTarget == nil && !sc.runtime.strictAvailability && !sc.hasEligibleTargetAfter(g, i)
		probe, ok := sc.r.acquireHalfOpenProbe(sc.ctx, target.Provider, target.Model, allowFailOpen)
		if !ok {
			sc.halfOpenBusy = true
			sc.started[g][i] = true
			continue
		}
		permit := sc.r.capacity.Acquire(target.Provider)
		if permit == nil {
			sc.r.releaseHalfOpenProbe(probe)
			sc.capacityDenied = true
			telemetry.LogEvent(sc.ctx, sc.r.logger, slog.LevelDebug, "provider_capacity_denied",
				"route", sc.route.Name,
				"provider", target.Provider,
				"model", target.Model,
			)
			allStarted = false
			continue
		}
		if !sc.budget.acquire(target.Provider, sc.active, g == sc.hedgeG) {
			sc.r.releaseHalfOpenProbe(probe)
			permit.Release()
			telemetry.LogEvent(sc.ctx, sc.r.logger, slog.LevelDebug, "semaphore_denied",
				"route", sc.route.Name,
				"provider", target.Provider,
				"model", target.Model,
			)
			allStarted = false
			continue
		}
		sc.started[g][i] = true
		started = true
		launchedN++
		sc.dispatched[g]++
		sc.active++
		sc.seq++
		id := sc.seq
		sc.runtime.used[target.Provider] = true
		branchCtx, cancel := context.WithCancel(sc.ctx)
		sc.cancels[id] = cancel
		active := activeBranch{provider: target.Provider, started: sc.r.now(), group: g,
			observation: sc.r.hedgeLatency.Begin(target.Provider, target.Model, sc.stream),
			probe:       probe}
		active.timing = sc.planAdaptiveAttempt(target, active.observation.Threshold)
		sc.activeBranches[id] = active
		if g == sc.hedgeG {
			sc.r.metrics.ObserveHedge(sc.route.Name)
			telemetry.LogEvent(sc.ctx, sc.r.logger, slog.LevelDebug, "hedge_launched",
				"route", sc.route.Name, "providers", target.Provider)
		}
		// The permit owns the slot, the balancing load signal and its metric
		// through the branch and any selected stream handed to the caller.
		go sc.runBranch(branchCtx, id, cancel, target, permit, active)
	}
	if allStarted {
		sc.launched[g] = true
	}
	return started || delivered
}

// deliverPreFailed reports a catalog-rejected target as a terminal branch
// result without an upstream call or a semaphore slot. Pre-failed results do
// not mark the provider as used, so a provider with a different native alias
// can still be reached in a later route.
func (sc *schedule) deliverPreFailed(id int, target model.Target, callErr *model.CallError) {
	started := sc.r.now()
	res := &branchResult{
		id: id, provider: target.Provider, model: target.Model, winner: false, prefailed: true,
		err: callErr, started: started, finished: started,
	}
	select {
	case sc.results <- res:
	case <-sc.ctx.Done():
	}
}

// refill attempts to launch the still-pending members of every armed,
// not-yet-exhausted group (a semaphore slot was just released). Returns
// whether any branch started.
func (sc *schedule) refill() bool {
	started := false
	for g := range sc.groups {
		if !sc.armed[g] || sc.launched[g] {
			continue
		}
		if sc.launch(g) {
			started = true
		}
	}
	return started
}

func (sc *schedule) stopHedge() {
	if sc.hedgeTimer != nil {
		sc.hedgeTimer.Stop()
		sc.hedgeTimer = nil
	}
	sc.hedgeC = nil
}

func (sc *schedule) armHedge() {
	sc.stopHedge()
	if sc.hedgeG < 0 || sc.armed[sc.hedgeG] || sc.launched[sc.hedgeG] {
		return
	}
	if sc.runtime != nil && !sc.runtime.budget.canHedge() {
		return
	}
	var deadline time.Time
	for _, active := range sc.activeBranches {
		if active.group != 0 {
			continue
		}
		if active.observation.Threshold <= 0 {
			return
		}
		due := active.started.Add(active.observation.Threshold)
		if due.After(deadline) {
			deadline = due
		}
	}
	if deadline.IsZero() {
		return
	}
	if !sc.canAffordHedge(deadline) {
		return
	}
	delay := max(time.Duration(0), deadline.Sub(sc.r.now()))
	sc.hedgeTimer = time.NewTimer(delay)
	sc.hedgeC = sc.hedgeTimer.C
}

func (sc *schedule) cancelOthers(except int) {
	for id, cancel := range sc.cancels {
		if id != except {
			if active, ok := sc.activeBranches[id]; ok {
				if sc.ctx.Err() == nil {
					active.observation.Censor(sc.r.now().Sub(active.started))
				} else {
					active.observation.Discard()
				}
			}
			cancel()
			if active, ok := sc.activeBranches[id]; ok {
				// Selection exits after choosing a winner. A loser's buffered
				// result may never be consumed, so retire its lease after cancel.
				sc.r.releaseHalfOpenProbe(active.probe)
			}
		}
	}
}

func (r *Runner) observeProviderOutcome(provider string, callErr *model.CallError, latency time.Duration) {
	r.observeProviderOutcomeMode(provider, false, callErr, latency)
}

func (r *Runner) observeProviderOutcomeMode(provider string, stream bool, callErr *model.CallError, latency time.Duration) {
	if provider == "" {
		return
	}
	r.scores.ObserveMode(provider, stream, callErr, latency)
	r.capacity.ObserveOutcome(provider, callErr)
	if r.metrics != nil {
		r.metrics.ObserveProviderEWMATTFT(provider, time.Duration(r.scores.LatencyOf(provider)*float64(time.Second)))
	}
}

func (sc *schedule) cancelAll() {
	for _, active := range sc.activeBranches {
		active.observation.Discard()
		sc.r.releaseHalfOpenProbe(active.probe)
	}
	for _, cancel := range sc.cancels {
		cancel()
	}
}

func (sc *schedule) observeRouteTimeouts() {
	if len(sc.activeBranches) == 0 {
		return
	}
	now := sc.r.now()
	timeoutErr := model.RouteTimeoutError()
	for id, active := range sc.activeBranches {
		var latency time.Duration
		if !active.started.IsZero() {
			latency = now.Sub(active.started)
		}
		active.observation.Censor(latency)
		sc.r.observeProviderOutcomeMode(active.provider, sc.stream, timeoutErr, latency)
		sc.r.releaseHalfOpenProbe(active.probe)
		delete(sc.activeBranches, id)
	}
}

// runBranch performs one upstream call and delivers exactly one terminal
// result. The send never blocks the caller past cancellation.
func (sc *schedule) runBranch(ctx context.Context, id int, cancel context.CancelFunc, target model.Target, permit *subsystem.Permit, active activeBranch) {
	// Failed probes, non-streaming calls and cancelled losers release their
	// provider slot on branch exit. A streaming winner transfers that release to
	// SelectedStream.Cancel because its upstream call remains active after the
	// first meaningful event.
	handedOff := false
	deliveredToLoop := false
	releaseProvider := permit.Release
	defer func() {
		if !handedOff {
			releaseProvider()
		}
		if !deliveredToLoop {
			sc.r.releaseHalfOpenProbe(active.probe)
		}
	}()

	started := active.started
	executor := sc.r.executor
	if entry := sc.r.plan.Models[sc.logical]; entry != nil {
		executor = transform.Wrap(executor, entry.Transforms)
	}
	// Each scheduler permit covers one physical call. A driver must not spend
	// additional calls behind a bounded request's accounting.
	singleAttempt := sc.budget.boundedCalls()
	var res *branchResult
	if sc.stream {
		request := sc.request
		request.Attempt = model.Attempt{ID: id, Target: target}
		request.SingleAttempt = singleAttempt
		provCfg := sc.r.plan.Providers[target.Provider]
		attemptTimeout, enforcedAttemptTimeout, adaptiveSliced := sc.attemptTimeoutFor(provCfg.AttemptTimeout, provCfg.SafeReplay, active.timing)
		if attemptTimeout > 0 {
			request.AttemptTimeout = attemptTimeout
		}
		callCtx := ctx
		attemptCancel := func() {}
		stopSelection := func() {}
		if enforcedAttemptTimeout {
			callCtx, attemptCancel, stopSelection = streamAttemptContext(ctx, attemptTimeout)
		}
		res = sc.r.probeStream(callCtx, target, request, executor)
		stopSelection()
		res.err = sc.normalizeAttemptError(ctx, callCtx, enforcedAttemptTimeout, provCfg.SafeReplay, res.err)
		if adaptiveSliced && res.err != nil && res.err.Code == "attempt_timeout" {
			sc.r.metrics.ObserveAdaptiveTimeout(sc.route.Name, target.Provider)
		}
		if res.err != nil {
			res.winner = false
			res.selected = nil
		}
		if res.selected != nil {
			// A selected result can lose the race or be abandoned before its
			// caller accepts ownership. Parent cancellation must release its
			// slot even when nobody reaches SelectedStream.Cancel.
			context.AfterFunc(ctx, releaseProvider)
			res.selected.Provider = target.Provider
			res.selected.Model = target.Model
			res.selected.Cancel = func() {
				attemptCancel()
				cancel()
				releaseProvider()
			}
			// The selected upstream stream is still physically active after the
			// probe returns. Its provider slot is handed to SelectedStream.Cancel.
			handedOff = true
		} else {
			attemptCancel()
		}
	} else {
		request := sc.request
		request.Attempt = model.Attempt{ID: id, Target: target}
		request.SingleAttempt = singleAttempt
		provCfg := sc.r.plan.Providers[target.Provider]
		attemptTimeout, enforcedAttemptTimeout, adaptiveSliced := sc.attemptTimeoutFor(provCfg.AttemptTimeout, provCfg.SafeReplay, active.timing)
		if attemptTimeout > 0 {
			request.AttemptTimeout = attemptTimeout
		}
		callCtx := ctx
		var attemptCancel context.CancelFunc
		if enforcedAttemptTimeout {
			callCtx, attemptCancel = context.WithTimeout(ctx, attemptTimeout)
		}
		body, callErr := executor.Do(callCtx, target, request)
		if attemptCancel != nil {
			attemptCancel()
		}
		callErr = sc.normalizeAttemptError(ctx, callCtx, enforcedAttemptTimeout, provCfg.SafeReplay, callErr)
		if adaptiveSliced && callErr != nil && callErr.Code == "attempt_timeout" {
			sc.r.metrics.ObserveAdaptiveTimeout(sc.route.Name, target.Provider)
		}
		res = &branchResult{winner: callErr == nil, body: body, err: callErr}
	}
	res.id = id
	res.provider = target.Provider
	res.model = target.Model
	res.started = started
	if res.finished.IsZero() {
		res.finished = sc.r.now()
	}
	elapsed := res.finished.Sub(started)
	switch {
	case res.err == nil:
		active.observation.Success(elapsed)
	case sc.ctx.Err() != nil:
		active.observation.Discard()
	case res.err.Class == model.ErrorTimeout:
		active.observation.Censor(elapsed)
	default:
		// Loser cancellation is reported by cancelOthers before cancelling its context.
		active.observation.Discard()
	}
	select {
	case sc.results <- res:
		deliveredToLoop = true
	case <-ctx.Done():
	}
}

// streamAttemptContext bounds selection without imposing that deadline on the
// selected stream. Stopping selection synchronizes with an already firing timer,
// so its callback cannot cancel a winner after ownership has been transferred.
func streamAttemptContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	var mu sync.Mutex
	stopped := false
	timer := time.AfterFunc(timeout, func() {
		mu.Lock()
		defer mu.Unlock()
		if !stopped {
			cancel(context.DeadlineExceeded)
		}
	})
	stop := func() {
		mu.Lock()
		defer mu.Unlock()
		stopped = true
		timer.Stop()
	}
	return ctx, func() { stop(); cancel(context.Canceled) }, stop
}

func (sc *schedule) normalizeAttemptError(ctx context.Context, callCtx context.Context, enforcedAttemptTimeout, safeReplay bool, callErr *model.CallError) *model.CallError {
	if ctx.Err() != nil {
		return normalizeContextError(ctx, callErr)
	}
	if enforcedAttemptTimeout && errors.Is(context.Cause(callCtx), context.DeadlineExceeded) {
		if sc.runtime != nil && sc.runtime.expired() {
			return &model.CallError{
				Code:   "route_timeout",
				Class:  model.ErrorTimeout,
				Status: 504,
				Replay: model.ReplayUnknown,
				Cause:  context.Cause(callCtx),
			}
		}
		out := &model.CallError{
			Code:   "attempt_timeout",
			Class:  model.ErrorTimeout,
			Status: 504,
			Scope:  model.FailureScopeProvider,
			Cause:  context.Cause(callCtx),
		}
		if !safeReplay {
			out.Replay = model.ReplayUnknown
		}
		return out
	}
	if callErr != nil && callErr.Class == model.ErrorTimeout && callErr.Scope == model.FailureScopeProvider && callErr.Replay == "" {
		if !safeReplay || (sc.runtime != nil && sc.runtime.expired()) {
			cloned := *callErr
			cloned.Replay = model.ReplayUnknown
			return &cloned
		}
	}
	return callErr
}

// drain buffered terminal results into the aggregate. At the terminal point no
// branch is in flight, so everything still buffered was reported as pre-failed
// (catalog-rejected) and never consumed a slot.
func (sc *schedule) drainPrefailed(failures *failureAggregate, retryable func(model.ErrorClass) bool) {
	for {
		select {
		case res := <-sc.results:
			if !res.winner && res.err == nil {
				res.err = &model.CallError{Class: model.ErrorInvalid, Status: 502}
			}
			failures.add(res.id, res.provider, res.model, res.err, retryable)
		default:
			return
		}
	}
}

// raceRoute executes one compiled route: its race batch (group 0) plus an
// optional latency hedge batch (group 1) that starts only while branches are
// still in flight. It returns the winner or the deterministic terminal failure
// of the route execution. No retry/fallback scheduling lives here: those are
// explicit transitions owned by the caller (see executeRoute).
func (r *Runner) raceRoute(ctx context.Context, logical string, route *policy.CompiledRoute, request model.ExecuteRequest, streamMode bool, runtime *routeRuntime) *routeOutcome {
	for {
		capacityChanged := r.capacity.Changed()
		outcome := r.raceRouteOnce(ctx, logical, route, request, streamMode, runtime)
		if !outcome.capacityBlocked {
			return outcome
		}
		deadline, stopDeadline := runtime.deadlineTimer()
		select {
		case <-capacityChanged:
			stopDeadline()
			continue
		case <-deadline:
			stopDeadline()
			return &routeOutcome{err: model.RouteTimeoutError()}
		case <-ctx.Done():
			stopDeadline()
			return &routeOutcome{err: streamSelectionContextError(ctx)}
		}
	}
}

func (r *Runner) raceRouteOnce(ctx context.Context, logical string, route *policy.CompiledRoute, request model.ExecuteRequest, streamMode bool, runtime *routeRuntime) *routeOutcome {
	pool, pinned, callErr := r.buildPool(logical, route, request, runtime)
	if callErr != nil {
		return &routeOutcome{err: callErr, pinned: pinned}
	}
	if len(pool) == 0 {
		if route.FallbackTarget != nil && !runtime.strictAvailability && r.poolHasHalfOpenProbeInFlight(route.Pool) {
			return &routeOutcome{fallbackOnly: true, err: halfOpenBusyError()}
		}
		return &routeOutcome{err: emptyPoolError(), empty: true}
	}
	pool = r.capacity.Available(pool)
	if len(pool) == 0 {
		return &routeOutcome{capacityBlocked: true}
	}
	ordered := r.applyLease(logical, route, pool)
	ordered = r.applyBalance(route.Name, route, ordered, streamMode)
	raceN := route.RaceCount
	if raceN <= 0 || raceN > len(ordered) {
		raceN = len(ordered)
	}
	raceBatch := append([]model.Target(nil), ordered[:raceN]...)
	if len(raceBatch) == 0 {
		return &routeOutcome{err: &model.CallError{Class: model.ErrorInvalid, Status: 502, Cause: errors.New("empty route pool")}}
	}

	groups := [][]model.Target{raceBatch}
	hedgeG := -1
	if route.HedgeTarget != nil && !pinned {
		hedgeBatch := r.buildHedgeBatch(logical, route.HedgeTarget, request, runtime)
		if len(hedgeBatch) > 0 {
			hedgeG = len(groups)
			groups = append(groups, hedgeBatch)
		}
	} else if route.Hedge.Enabled && route.Hedge.Internal && !pinned {
		if len(ordered) > raceN {
			// Keep only a placeholder here. The candidate set is rebuilt when
			// the timer fires so load, health, cooldown and capacity are current.
			hedgeG = len(groups)
			groups = append(groups, nil)
		}
	}

	totalTargets := 0
	for _, group := range groups {
		totalTargets += len(group)
	}
	sc := &schedule{
		r:              r,
		ctx:            ctx,
		logical:        logical,
		route:          route,
		groups:         groups,
		request:        request,
		stream:         streamMode,
		pinned:         pinned,
		results:        make(chan *branchResult, totalTargets),
		cancels:        make(map[int]context.CancelFunc),
		activeBranches: make(map[int]activeBranch, totalTargets),
		budget:         runtime.budget,
		runtime:        runtime,
		hedgeG:         hedgeG,
	}
	defer sc.stopHedge()
	sc.dispatched = make([]int, len(groups))
	sc.armed = make([]bool, len(groups))
	sc.excludeUsed = make([]bool, len(groups))
	sc.limits = make([]int, len(groups))
	sc.started = make([][]bool, len(groups))
	sc.launched = make([]bool, len(groups))
	for g := range groups {
		sc.started[g] = make([]bool, len(groups[g]))
		sc.limits[g] = len(groups[g])
	}
	sc.armed[0] = true
	if hedgeG >= 0 {
		if route.HedgeTarget != nil {
			// The hedge target's unused routing policy is enforced at launch time,
			// because the request's used set grows while the source route races;
			// the race count caps how many hedge members may actually launch.
			sc.excludeUsed[hedgeG] = route.HedgeTarget.ProviderUnused
			if route.HedgeTarget.RaceCount > 0 {
				sc.limits[hedgeG] = route.HedgeTarget.RaceCount
			}
		} else {
			sc.excludeUsed[hedgeG] = true
			sc.limits[hedgeG] = 1
		}
	}

	// The absolute request-wide deadline is observed by every schedule without
	// attaching it to the winning stream's context, so a stream selected before
	// the deadline remains usable after selection.
	deadlineCh, stopDeadline := runtime.deadlineTimer()
	defer stopDeadline()

	if !sc.launch(0) {
		sc.cancelAll()
		if runtime.expired() {
			return &routeOutcome{err: model.RouteTimeoutError()}
		}
		if sc.halfOpenBusy && !runtime.strictAvailability {
			return &routeOutcome{fallbackOnly: route.FallbackTarget != nil, err: halfOpenBusyError()}
		}
		if sc.capacityDenied {
			return &routeOutcome{capacityBlocked: true}
		}
		// Every member is blocked by the request-wide semaphore budget: the
		// route has no available targets for this request.
		return &routeOutcome{err: emptyPoolError(), empty: true}
	}
	runtime.attempts++
	sc.armHedge()

	// A failure admits a retry transition when it matches the retry target's
	// own error filter; the flag only orders the deterministic selection below.
	retryable := func(model.ErrorClass) bool { return false }
	if route.RetryTarget != nil {
		retryable = func(class model.ErrorClass) bool { return route.RetryTarget.ErrorIn[class] }
	} else if route.Retry.Internal {
		retryable = func(class model.ErrorClass) bool {
			return class != model.ErrorCancelled && class != model.ErrorInvalid
		}
	}

	failures := &failureAggregate{}
	for {
		select {
		case res := <-sc.results:
			active := sc.activeBranches[res.id]
			delete(sc.activeBranches, res.id)
			if !res.winner && res.err == nil {
				res.err = &model.CallError{Class: model.ErrorInvalid, Status: 502}
			}
			// Cancellations (route timeout, client cancel, loser cancel) are
			// neutral for health and lease state.
			if res.err == nil || res.err.Class != model.ErrorCancelled {
				r.record(ctx, res.provider, res.model, res.err)
				r.observeLeaseFailure(logical, route, res.provider, res.err)
				r.observeBranch(res)
				// Not a res.err class-wise for cancellation: the latency is only
				// meaningful for successful branches, and health-neutral failures
				// still advance the window. subsystem.Catalog-rejected targets are skipped:
				// they carry no timing and their failure is local, not a health
				// signal of the provider.
				if !res.prefailed {
					var latency time.Duration
					if !res.finished.IsZero() && !res.started.IsZero() {
						latency = res.finished.Sub(res.started)
					}
					r.observeProviderOutcomeMode(res.provider, streamMode, res.err, latency)
				}
			} else {
				r.observeBranchCancelled(res)
			}
			r.releaseHalfOpenProbe(active.probe)
			if res.winner {
				r.observeLeaseWinner(logical, route, res.provider, res)
				if !res.finished.IsZero() && !res.started.IsZero() {
					r.metrics.ObserveTTFT(logical, res.provider, res.model, res.finished.Sub(res.started))
				}
				sc.cancelOthers(res.id)
				outcome := &routeOutcome{provider: res.provider, model: res.model, attempts: runtime.attempts}
				if !res.finished.IsZero() && !res.started.IsZero() {
					outcome.ttft = res.finished.Sub(res.started)
				}
				if streamMode {
					outcome.selected = res.selected
				} else {
					outcome.body = res.body
				}
				return outcome
			}
			if !res.prefailed {
				sc.active--
			}
			failures.add(res.id, res.provider, res.model, res.err, retryable)
			// A slot just freed: try to start members of an armed group that were
			// denied a permit while it was held (refills the race batch and the
			// hedge batch alike).
			sc.refill()
			sc.armHedge()
			if sc.active == 0 {
				// No branch is in flight and the refill started nothing: no
				// future event can free another slot, so the route is terminal.
				// An as-yet-unarmed hedge is abandoned by design (hedge is
				// latency-only and must not delay a fast terminal failure).
				sc.drainPrefailed(failures, retryable)
				sc.cancelAll()
				return &routeOutcome{err: failures.err(), pinned: pinned, failedProvider: failures.provider, failedModel: failures.model, failedTargets: failures.targets, attempts: runtime.attempts}
			}
		case <-sc.hedgeC:
			sc.hedgeC = nil
			if runtime.expired() {
				sc.observeRouteTimeouts()
				sc.cancelAll()
				return &routeOutcome{err: model.RouteTimeoutError()}
			}
			if sc.hedgeG >= 0 && sc.runtime.budget.canHedge() {
				if route.Hedge.Internal && route.HedgeTarget == nil {
					pool, _, hedgeErr := r.buildPool(logical, route, request, runtime)
					if hedgeErr == nil {
						pool = r.capacity.Available(pool)
						pool = r.applyLease(logical, route, pool)
						pool = r.applyBalance(route.Name, route, pool, sc.stream)
						unused := make([]model.Target, 0, len(pool))
						for _, target := range pool {
							if !runtime.used[target.Provider] {
								unused = append(unused, target)
							}
						}
						sc.groups[sc.hedgeG] = unused
						sc.started[sc.hedgeG] = make([]bool, len(unused))
						sc.launched[sc.hedgeG] = false
						sc.limits[sc.hedgeG] = 1
					}
				}
				if route.AdaptiveTiming && sc.runtime != nil && !sc.runtime.deadline.IsZero() {
					filtered := r.filterWorthwhileHedgeTargets(sc.groups[sc.hedgeG], sc.stream, time.Until(sc.runtime.deadline))
					sc.groups[sc.hedgeG] = filtered
					sc.started[sc.hedgeG] = make([]bool, len(filtered))
				}
				if len(sc.groups[sc.hedgeG]) > 0 {
					sc.armed[sc.hedgeG] = true
					sc.launch(sc.hedgeG)
				}
			}
		case <-deadlineCh:
			sc.observeRouteTimeouts()
			sc.cancelAll()
			return &routeOutcome{err: model.RouteTimeoutError()}
		case <-ctx.Done():
			sc.cancelAll()
			return &routeOutcome{err: streamSelectionContextError(ctx)}
		}
	}
}

// collectFailures is unused: prefailed results are accumulated through the
// main loop and drained at the terminal point.
