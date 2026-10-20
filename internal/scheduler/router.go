package scheduler

import (
	"context"
	"errors"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/telemetry"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type Runner struct {
	config   *policy.CompiledConfig
	catalog  *subsystem.Catalog
	executor model.Executor
	logger   *slog.Logger
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
	mu       sync.Mutex
	// cooling maps a (provider, native model) pair to the absolute deadline
	// until which that pair is excluded from candidate pools. The window is
	// lazy: no timer rearms it, entries are checked at request time and an
	// expired entry is dropped on the pair's next success.
	cooling   map[cooldownKey]time.Time
	leases    *subsystem.LeaseStore
	scores    *subsystem.ScoreStore
	affinity  *subsystem.AffinityStore
	metrics   *telemetry.Metrics
	admission *subsystem.AdmissionController
}

// cooldownKey identifies the unit of provider failure isolation: one native
// model served by one provider. A provider that serves several logical models
// keeps serving the healthy ones while a broken mapping cools down alone.
type cooldownKey struct {
	provider string
	model    string
}

func NewRunner(config *policy.CompiledConfig, catalog *subsystem.Catalog, executor model.Executor) *Runner {
	return NewRunnerMetrics(config, catalog, executor, telemetry.NewMetrics())
}

func NewRunnerMetrics(config *policy.CompiledConfig, catalog *subsystem.Catalog, executor model.Executor, metrics *telemetry.Metrics) *Runner {
	if metrics == nil {
		metrics = telemetry.NewMetrics()
	}
	logger := config.Logger
	if logger == nil {
		logger = telemetry.NewGatewayLogger("info")
	}
	runner := &Runner{
		config: config, catalog: catalog, executor: executor, logger: logger,
		now: time.Now, sleep: sleepContext, cooling: make(map[cooldownKey]time.Time),
		metrics: metrics,
	}
	runner.leases = subsystem.NewLeaseStore(runner.now)
	runner.scores = subsystem.NewScoreStore(runner.now)
	runner.affinity = subsystem.NewAffinityStore(runner.now, config.Raw.AffinityFile, func(err error) {
		telemetry.LogEvent(context.Background(), config.Logger, slog.LevelError, "affinity_persistence_failed", "detail", telemetry.SafeLogDetail(err.Error()))
	})
	runner.admission = subsystem.NewAdmissionController(runner.metrics, runner.now)
	if config != nil {
		for id, p := range config.Providers {
			runner.scores.SetMaxConcurrent(id, p.MaxConcurrent)
			runner.metrics.ObserveProviderCapacity(id, p.MaxConcurrent)
			runner.metrics.ObserveProviderEWMATTFT(id, time.Duration(runner.scores.LatencyOf(id)*float64(time.Second)))
		}
	}
	return runner
}

// telemetry.Metrics exposes the runner's metric registry so the server can mount the
// /metrics endpoint and record request-level observations.
func (r *Runner) Metrics() *telemetry.Metrics {
	return r.metrics
}

// Affinity exposes the runner's affinity store.
func (r *Runner) Affinity() *subsystem.AffinityStore {
	return r.affinity
}

// Logger exposes the runner's logger.
func (r *Runner) Logger() *slog.Logger {
	return r.logger
}

// SetLogger sets the runner's logger.
func (r *Runner) SetLogger(logger *slog.Logger) {
	r.logger = logger
}

// Scores exposes the runner's score store.
func (r *Runner) Scores() *subsystem.ScoreStore {
	return r.scores
}

// AvailableTargets exposes the cooling-filtered targets.
func (r *Runner) AvailableTargets(targets []model.Target) []model.Target {
	return r.availableTargets(targets)
}

// observeBranch records the branch attempt counter (the empty error_type labels
// a success). The in-flight gauge slot is released by the branch's own defer in
// runBranch (scheduler.go), so every launched branch returns it exactly once on
// every exit path — including when a racing sibling wins and the main loop
// returns before draining this branch's result.
func (r *Runner) observeBranch(res *branchResult) {
	errorType := ""
	if res.err != nil {
		errorType = string(res.err.Class)
	}
	r.metrics.ObserveAttempt(res.provider, res.model, errorType)
}

// observeBranchCancelled records a cancelled branch (client cancel, loser
// cancel, route deadline) without updating health. The in-flight gauge slot is
// released by the branch's own defer in runBranch, not here.
func (r *Runner) observeBranchCancelled(res *branchResult) {
	r.metrics.ObserveAttempt(res.provider, res.model, "cancelled")
}

// RecordStreamFailure feeds a mid-stream (post-selection) failure of the
// winning stream back into the same health machinery that pre-selection branch
// failures use. The scheduler has already returned by the time the winner's
// stream breaks, so without this call the provider would keep a clean health
// record no matter how often it breaks streams mid-flight. Cancelled streams
// are health-neutral, exactly like cancelled branches. The lease (if enabled)
// is released per the entry route's ReleaseOn policy, and both the branch
// attempt counter and the dedicated stream-break counter are observed.
// RecordStreamFailure feeds a mid-stream (post-selection) failure of the
// winning stream back into the same health machinery that pre-selection branch
// failures use. The scheduler has already returned by the time the winner's
// stream breaks, so without this call the provider would keep a clean health
// record no matter how often it breaks streams mid-flight. Cancelled streams
// are health-neutral, exactly like cancelled branches. The lease (if enabled)
// is released per the entry route's ReleaseOn policy, and both the branch
// attempt counter and the dedicated stream-break counter are observed.
func (r *Runner) RecordStreamFailure(ctx context.Context, logical, provider, nativeModel string, callErr *model.CallError) {
	if callErr == nil || callErr.Class == model.ErrorCancelled || provider == "" {
		return
	}
	r.record(ctx, provider, nativeModel, callErr)
	if entry, ok := r.config.Models[logical]; ok {
		r.observeLeaseFailure(logical, entry, provider, callErr)
	}
	r.scores.Observe(provider, callErr, 0)
	r.metrics.ObserveAttempt(provider, nativeModel, string(callErr.Class))
	r.metrics.ObserveStreamBreak(provider, nativeModel, string(callErr.Class))
	telemetry.LogEvent(ctx, r.logger, slog.LevelWarn, "llm_stream_break",
		"provider", provider,
		"native_model", nativeModel,
		"error_type", string(callErr.Class),
		"status_code", model.CallErrorStatus(callErr),
	)
}

// streamRetryable reports whether a fresh client request carrying the same
// failure would activate an explicit transition of the logical model's entry
// route (retry or fallback, gated by their own error filters). With no
// explicit transition configured it falls back to the shared retryable class
// set. The gateway itself never retries after the winner's first meaningful
// event (the SSE prelude is already flushed); this flag is emitted in the SSE
// error payload so a retry-capable client can decide on typed data instead of
// matching the message text.
func (r *Runner) StreamRetryable(logical string, callErr *model.CallError) bool {
	if callErr != nil && callErr.Replay == model.ReplayUnknown {
		return false
	}
	if route, ok := r.config.Models[logical]; ok && route.Stateful {
		return false
	}
	if callErr == nil {
		return false
	}
	entry, ok := r.config.Models[logical]
	if !ok {
		return policy.AllRetryableClasses()[callErr.Class]
	}
	hasTransition := false
	if entry.Retry.Attempts > 0 && entry.RetryTarget != nil {
		if entry.RetryTarget.Applicable(callErr, 0) {
			return true
		}
		hasTransition = true
	}
	if entry.FallbackTarget != nil {
		if entry.FallbackTarget.Applicable(callErr, 0) {
			return true
		}
		hasTransition = true
	}
	if hasTransition {
		// The entry route owns explicit transitions: their error filters are
		// authoritative, and an excluded class must not be advertised as
		// retryable.
		return false
	}
	return policy.AllRetryableClasses()[callErr.Class]
}

// Close shuts down the runner's persistent state: the affinity store flushes
// any recent mappings so a graceful restart does not lose them.
func (r *Runner) Close() error {
	if r.affinity != nil {
		return r.affinity.Close()
	}
	return nil
}

func (r *Runner) admit(ctx context.Context, logical string, runtime *routeRuntime) (func(), *model.CallError) {
	if r.admission == nil || r.config == nil {
		return func() {}, nil
	}
	entry, ok := r.config.Models[logical]
	if !ok {
		return func() {}, nil
	}
	deadline := time.Time{}
	if runtime != nil {
		deadline = runtime.deadline
	}
	return r.admission.Admit(ctx, entry.Name, entry.Admission, deadline)
}

// Run executes the bounded non-streaming route graph and returns the winner
// body.
func (r *Runner) Run(ctx context.Context, logical string, request model.ExecuteRequest) ([]byte, *model.CallError) {
	outcome, err := r.RunWithResult(ctx, logical, request)
	if err != nil {
		return nil, err
	}
	return outcome.Body, nil
}

// RunWithResult executes the non-streaming route graph and also reports the
// winning provider, needed for lease and affinity bookkeeping.
func (r *Runner) RunWithResult(ctx context.Context, logical string, request model.ExecuteRequest) (*model.RunOutcome, *model.CallError) {
	entry, ok := r.config.Models[logical]
	if !ok {
		return nil, &model.CallError{Class: model.ErrorInvalid, Status: 404, Scope: model.FailureScopeRequest}
	}
	runtime := newRouteRuntime(entry)
	release, admitErr := r.admit(ctx, logical, runtime)
	if admitErr != nil {
		return nil, admitErr
	}
	defer release()
	outcome := r.executeRoute(ctx, logical, entry, request, false, runtime)
	if outcome.err != nil {
		return nil, outcome.err
	}
	ttft := time.Duration(0)
	if outcome.ttft > 0 {
		ttft = outcome.ttft
	}
	return &model.RunOutcome{Body: outcome.body, Provider: outcome.provider, Model: outcome.model, Attempts: outcome.attempts, TTFT: ttft}, nil
}

// runPlan resolves the request-level entry route for the logical model and
// executes the compiled route graph with one request-wide runtime: the
// deadline, the semaphore budget and the used-provider set are shared by every
// transition and never reset when entering a subroute.
func (r *Runner) runPlan(ctx context.Context, logical string, request model.ExecuteRequest, streamMode bool) *routeOutcome {
	entry, ok := r.config.Models[logical]
	if !ok {
		return &routeOutcome{err: &model.CallError{Class: model.ErrorInvalid, Status: 404}}
	}
	runtime := newRouteRuntime(entry)
	return r.executeRoute(ctx, logical, entry, request, streamMode, runtime)
}

// executeRoute runs one compiled route and applies its explicit transitions on
// a terminal failure: retry is a bounded repeated transition into the retry
// target (with backoff), fallback a one-shot alternate transition. The
// destination owns its applicability through its own filter; a non-applicable
// destination returns the current terminal failure unchanged, and a destination
// with no available targets (empty) never masks the original failure. A
// cancellation or a known affinity pin (fail-closed) terminates the graph.
func (r *Runner) executeRoute(ctx context.Context, logical string, route *policy.CompiledRoute, request model.ExecuteRequest, streamMode bool, runtime *routeRuntime) *routeOutcome {
	outcome := r.raceRoute(ctx, logical, route, request, streamMode, runtime)
	if outcome.err == nil || outcome.err.Class == model.ErrorCancelled || outcome.err.Replay == model.ReplayUnknown || outcome.pinned {
		return outcome
	}
	if outcome.empty {
		return outcome
	}
	original := outcome.err

	if route.Retry.Attempts > 0 && route.Retry.Internal {
		for attempt := 0; attempt < route.Retry.Attempts; attempt++ {
			if !isReschedulableError(outcome.err) {
				break
			}
			telemetry.LogEvent(ctx, r.logger, slog.LevelWarn, "llm_retry",
				"route", route.Name,
				"provider", outcome.failedProvider,
				"error_type", string(outcome.err.Class),
				"status_code", outcome.err.Status,
				"attempt", attempt+1,
			)
			if callErr := runtime.waitBackoff(ctx, backoffDuration(route.Retry.Backoff, attempt), r.sleep); callErr != nil {
				return &routeOutcome{err: callErr}
			}
			for target := range outcome.failedTargets {
				runtime.excludeTargets[target] = true
			}
			if len(outcome.failedTargets) == 0 && outcome.failedProvider != "" {
				runtime.exclude[outcome.failedProvider] = true
			}
			next := r.raceRoute(ctx, logical, route, request, streamMode, runtime)
			if next.err == nil || next.err.Class == model.ErrorCancelled {
				if next.err == nil {
					telemetry.LogEvent(ctx, r.logger, slog.LevelInfo, "llm_attempt",
						"route", route.Name,
						"provider", next.provider,
						"native_model", next.model,
						"status", "success",
						"attempt", attempt+1,
					)
				}
				return next
			}
			if next.empty {
				break
			}
			outcome = next
		}
	}

	if route.Retry.Attempts > 0 && route.RetryTarget != nil {
		for attempt := 0; attempt < route.Retry.Attempts; attempt++ {
			if !route.RetryTarget.Applicable(outcome.err, attempt) {
				break
			}
			telemetry.LogEvent(ctx, r.logger, slog.LevelWarn, "llm_retry",
				"route", route.Name,
				"provider", outcome.failedProvider,
				"error_type", string(outcome.err.Class),
				"status_code", outcome.err.Status,
				"attempt", attempt+1,
			)
			if callErr := runtime.waitBackoff(ctx, backoffDuration(route.Retry.Backoff, attempt), r.sleep); callErr != nil {
				return &routeOutcome{err: callErr}
			}
			next := r.executeRoute(ctx, logical, route.RetryTarget, request, streamMode, runtime)
			if next.err == nil || next.err.Class == model.ErrorCancelled {
				if next.err == nil {
					// The retried attempt succeeded: one attempt-level success line
					// so a request_id reconstructs the retry→success path.
					telemetry.LogEvent(ctx, r.logger, slog.LevelInfo, "llm_attempt",
						"route", route.RetryTarget.Name,
						"provider", next.provider,
						"native_model", next.model,
						"status", "success",
						"attempt", attempt+1,
					)
				}
				return next
			}
			if next.empty {
				// The retry route has no available targets for this request:
				// the transition is not usable, so restore the original failure
				// and continue to any sibling fallback transition.
				outcome = &routeOutcome{err: original}
				break
			}
			outcome = next
		}
	}
	if route.FallbackTarget != nil && route.FallbackTarget.Applicable(outcome.err, 0) {
		from := outcome.failedProvider
		to := ""
		if len(route.FallbackTarget.Pool) > 0 {
			to = route.FallbackTarget.Pool[0].Provider
		}
		reason := ""
		if outcome.err != nil {
			reason = string(outcome.err.Class)
		}
		r.metrics.ObserveFallback(from, to, reason)
		telemetry.LogEvent(ctx, r.logger, slog.LevelWarn, "llm_fallback",
			"route", route.Name,
			"from_provider", from,
			"to_provider", to,
			"reason", reason,
		)
		next := r.executeRoute(ctx, logical, route.FallbackTarget, request, streamMode, runtime)
		if !next.empty {
			return next
		}
	}
	return outcome
}

// buildPool produces the runtime candidate pool of a route for the request. A
// known affinity mapping narrows the route to the pinned provider for the
// whole request graph; an unknown state identifier is ignored (on_missing =
// "ignore"). The unused-provider routing policy excludes providers already
// used by this request graph; cooling (provider, model) pairs are skipped with
// fail-open so the route is never artificially idle. Exact catalog validation
// happens at branch execution, so a missing native produces a model_not_found failure
// that can activate the configured fallback.
func (r *Runner) buildPool(logical string, route *policy.CompiledRoute, request model.ExecuteRequest, runtime *routeRuntime) ([]model.Target, bool, *model.CallError) {
	if route.Affinity.Enabled && request.Kind == model.RequestResponses {
		if id := subsystem.RequestAffinityID(request.Body, request.Kind, route.Affinity.Sources); id != "" {
			if provider, ok := r.affinity.Lookup(id); ok {
				if _, exists := r.config.Providers[provider]; !exists {
					// A mapping can outlive a configuration change that removes
					// or renames its provider. Only that structural case is
					// treated as missing affinity; runtime resolution failures
					// stay fail-closed.
					r.affinity.Forget(id)
				} else {
					for _, target := range route.Pool {
						if target.Provider == provider {
							return []model.Target{target}, true, nil
						}
					}
					// The pinned provider is not a member of the compiled pool:
					// treat the mapping as stale and forget it (same structural
					// boundary).
					r.affinity.Forget(id)
				}
			}
		}
	}
	pool, callErr := r.buildDynamicPool(route, request, runtime)
	if callErr != nil {
		return nil, false, callErr
	}
	return pool, false, nil
}

// buildDynamicPool applies the route's own runtime selection: the
// unused-provider policy (explicit routing policy, not a hidden retry
// property) and the cooling fail-open. An all-used pool returns empty with no
// error, so the caller can report the route as not applicable.
func (r *Runner) buildDynamicPool(route *policy.CompiledRoute, _ model.ExecuteRequest, runtime *routeRuntime) ([]model.Target, *model.CallError) {
	pool := route.Pool
	if route.ProviderUnused {
		filtered := make([]model.Target, 0, len(pool))
		for _, target := range pool {
			if !runtime.used[target.Provider] {
				filtered = append(filtered, target)
			}
		}
		if len(filtered) == 0 {
			return nil, nil
		}
		pool = filtered
	}
	// The continuation exclusion set is provider-independent: a takeover
	// re-dispatch must never re-hit an upstream that already failed a previous
	// round of the same request, even when the route does not carry the unused
	// policy. Empty for fresh requests, so the normal path is untouched.
	if len(runtime.exclude) > 0 || len(runtime.excludeTargets) > 0 {
		filtered := make([]model.Target, 0, len(pool))
		for _, target := range pool {
			if runtime.exclude[target.Provider] {
				continue
			}
			if runtime.excludeTargets[targetKey{provider: target.Provider, model: target.Model}] {
				continue
			}
			filtered = append(filtered, target)
		}
		if len(filtered) == 0 {
			return nil, nil
		}
		pool = filtered
	}
	return r.availableFromPool(pool, runtime.strictAvailability), nil
}

// availableFromPool filters the pool against cooling (provider, model) pairs.
// A non-strict pool fails open with the full pool when every target is
// cooling, so a route is never artificially idle because one provider cools.
// A strict pool (a continuation with the cached-exclusion strictAvailability
// policy) does NOT fail open: when everything is cooling, it returns no
// targets, so the continuation never re-races a still-cooling provider that
// just broke. The caller then decides (chain-retry budget, terminal error)
// instead of burning the re-dispatch on known-dead carriers.
func (r *Runner) availableFromPool(pool []model.Target, strict bool) []model.Target {
	available := r.availableTargets(pool)
	if len(available) == 0 && !strict {
		return append([]model.Target(nil), pool...)
	}
	return available
}

// buildHedgeBatch derives the hedge target route's runtime pool (the full
// ordered selection, not yet capped): the hedge target's unused routing policy
// and race count are applied at launch time, because the request's used set
// grows while the source route races. Here only cooling (honoring
// strictAvailability) and the lease promotion are applied.
func (r *Runner) buildHedgeBatch(logical string, target *policy.CompiledRoute, _ model.ExecuteRequest, runtime *routeRuntime) []model.Target {
	pool := r.availableFromPool(target.Pool, runtime.strictAvailability)
	if len(pool) == 0 {
		return nil
	}
	return r.applyLease(logical, target, pool)
}

// applyLease promotes the current lease holder to the top of the ranking.
func (r *Runner) applyLease(logical string, route *policy.CompiledRoute, pool []model.Target) []model.Target {
	if !route.Lease.Enabled {
		return pool
	}
	holder, ok := r.leases.Holder(logical)
	if !ok {
		return pool
	}
	return subsystem.PromoteFront(pool, holder)
}

// applyBalance performs the runtime provider selection of the balance action:
// it promotes the balanced choice to the front of the pool for this request.
// Balance and lease are mutually exclusive per route (checked at compile
// stage), so it never runs on a route that also has a lease. Static weights
// default to equal (1): provider priority does not feed the runtime choice —
// f7-13 showed priority-weighted selection concentrates on one provider —
// it only shapes the compile-time pool order.
func (r *Runner) applyBalance(route string, routeConfig *policy.CompiledRoute, pool []model.Target) []model.Target {
	// The health snapshot is recorded whether or not the pool is large enough
	// for the balance action to reorder it: the pool state is observable even
	// for a single-provider balance route.
	if routeConfig.Balance.Enabled {
		r.observeBalanceHealth(routeConfig, pool)
	}
	if !routeConfig.Balance.Enabled || len(pool) < 2 {
		return pool
	}
	selected := r.scores.Select(route, pool, routeConfig.Balance)
	// The balanced choice is the front of the returned order; record it so the
	// cursor movement is observable.
	if len(selected) > 0 {
		r.metrics.ObserveBalanceSelection(route, selected[0].Provider)
	}
	return selected
}

// observeBalanceHealth snapshots the current balance health of every pool
// member under the route's policy. It must not be confused with the
// request-level outcome: it describes the provider pool state, not the final
// winner.
func (r *Runner) observeBalanceHealth(routeConfig *policy.CompiledRoute, pool []model.Target) {
	for _, target := range pool {
		health := r.scores.Health(target.Provider, routeConfig.Balance.Window, routeConfig.Balance.ErrorBudget)
		r.metrics.ObserveBalanceHealth(target.Provider, health)
	}
}

// validateTarget checks the explicit native model of a target against the
// provider's last-known-good catalog snapshot before dispatch:
//   - snapshot available, native present: valid;
//   - snapshot available, native absent: model_not_found (may activate fallback);
//   - implicit catalog not yet available: optimistic call of the configured ID;
//   - explicit catalog unavailable without a snapshot: fail closed.
func (r *Runner) validateTarget(target model.Target) *model.CallError {
	return r.catalog.Validate(target.Provider, target.Model)
}

// availableTargets filters the pool against cooling (provider, native model)
// pairs. The cooling map is consulted lazily at request time — no timer rearms
// it — so an expired window is simply no longer filtered out. An empty Model
// (callers without a native mapping) matches any cooling entry for the
// provider, the conservative direction for an unkeyed lookup.
func (r *Runner) availableTargets(targets []model.Target) []model.Target {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	available := make([]model.Target, 0, len(targets))
	for _, target := range targets {
		if !r.coolingLocked(target.Provider, target.Model, now) {
			available = append(available, target)
		}
	}
	return available
}

// coolingLocked reports whether the (provider, model) pair is cooling at now.
// The caller must hold r.mu. An empty model matches any cooling entry for the
// provider (conservative for callers without a native mapping).
func (r *Runner) coolingLocked(providerID, model string, now time.Time) bool {
	for key, until := range r.cooling {
		if key.provider != providerID {
			continue
		}
		if key.model == "" || model == "" || key.model == model {
			if now.Before(until) {
				return true
			}
		}
	}
	return false
}

func (r *Runner) record(ctx context.Context, providerID, nativeModel string, callErr *model.CallError) {
	key := cooldownKey{provider: providerID, model: nativeModel}
	before := r.cooldownUntil(providerID, nativeModel)
	r.mu.Lock()
	newUntil := time.Time{}
	if callErr == nil {
		delete(r.cooling, key)
	} else if policy.AllRetryableClasses()[callErr.Class] {
		newUntil = r.now().Add(r.config.Providers[providerID].Cooldown.Duration)
		r.cooling[key] = newUntil
	}
	r.mu.Unlock()
	// Snapshot the cooldown gauge on every cooldown-state transition the
	// request produced: enter, extend and clear. The gauge carries the absolute
	// deadline; a panel computes the remaining window at scrape time with
	// deadline − time(), so the value decays truthfully between snapshots and
	// a stale deadline never outlives its window. A cleared window drops the
	// series.
	if newUntil.IsZero() {
		if before.IsZero() {
			return
		}
		r.metrics.ObserveCooldownUntil(providerID, nativeModel, time.Time{})
		return
	}
	r.metrics.ObserveCooldownUntil(providerID, nativeModel, newUntil)
	// Emit cooldown_put only when the provider actually enters the cooling
	// window (was not cooling, now is); re-extending an active window is not a
	// new event worth a line.
	if !before.IsZero() || newUntil.IsZero() {
		return
	}
	telemetry.LogEvent(ctx, r.logger, slog.LevelWarn, "cooldown_put",
		"provider", providerID,
		"native_model", nativeModel,
		"error_type", string(callErr.Class),
		"cooldown_ms", int64(r.config.Providers[providerID].Cooldown.Duration.Milliseconds()),
	)
}

// cooldownUntil reports the current cooling deadline of a (provider, model)
// pair (zero when not cooling), read under the runner lock.
func (r *Runner) cooldownUntil(providerID, nativeModel string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cooling[cooldownKey{provider: providerID, model: nativeModel}]
}

// observeLeaseFailure releases the model lease when its holder fails with a
// configured hard-failure class. Cancelled losers never reach this method.
func (r *Runner) observeLeaseFailure(logical string, route *policy.CompiledRoute, provider string, callErr *model.CallError) {
	if !route.Lease.Enabled || callErr == nil || callErr.Class == model.ErrorCancelled {
		return
	}
	if route.Lease.ReleaseOn[callErr.Class] {
		r.leases.ReleaseIfHolder(logical, provider)
	}
}

// observeLeaseWinner renews or acquires the lease for the winner and accounts
// its start-to-meaningful time for the consecutive slow-start counter.
func (r *Runner) observeLeaseWinner(logical string, route *policy.CompiledRoute, provider string, res *branchResult) {
	if !route.Lease.Enabled {
		return
	}
	if route.Lease.RenewOnSuccess || !r.leases.Exists(logical) {
		r.leases.Renew(logical, provider, route.Lease.Duration)
	}
	if route.Lease.ReleaseAfterSlowStarts > 0 && route.Lease.SlowStart > 0 {
		if res.finished.Sub(res.started) > route.Lease.SlowStart {
			r.leases.ObserveSlowStart(logical, provider, route.Lease.ReleaseAfterSlowStarts)
		} else {
			r.leases.ResetSlowStarts(logical, provider)
		}
	}
}

// affinityBindTTL returns the affinity TTL for a logical model when affinity is
// enabled, so the server can persist response id mappings.
func (r *Runner) AffinityBindTTL(logical string) (time.Duration, bool) {
	entry, ok := r.config.Models[logical]
	if !ok || !entry.Affinity.Enabled {
		return 0, false
	}
	return entry.Affinity.TTL, true
}

func backoffDuration(config policy.BackoffConfig, retryIndex int) time.Duration {
	initial := config.Initial.Duration
	if initial <= 0 {
		initial = 100 * time.Millisecond
	}
	maximum := config.Max.Duration
	if maximum <= 0 {
		maximum = time.Second
	}
	if strings.ToLower(config.Type) != "exponential" {
		return min(initial, maximum)
	}
	delay := initial
	for range retryIndex {
		if delay >= maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return min(delay, maximum)
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func normalizeContextError(ctx context.Context, callErr *model.CallError) *model.CallError {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &model.CallError{Class: model.ErrorTimeout, Status: 504, Cause: ctx.Err()}
	}
	if errors.Is(ctx.Err(), context.Canceled) && callErr != nil {
		return &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
	}
	return callErr
}

func streamSelectionContextError(ctx context.Context) *model.CallError {
	if ctx.Err() == context.DeadlineExceeded {
		return &model.CallError{Class: model.ErrorTimeout, Status: 504, Cause: context.DeadlineExceeded}
	}
	return &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
}

func isReschedulableError(callErr *model.CallError) bool {
	if callErr != nil && callErr.Replay == model.ReplayUnknown {
		return false
	}
	if callErr == nil {
		return false
	}
	if callErr.Class == model.ErrorCancelled || callErr.Scope == model.FailureScopeRequest {
		return false
	}
	// Preserve the historical meaning of an unscoped invalid_response as a
	// terminal request/configuration failure. model.Executor-originated failures set
	// an explicit provider/target scope.
	return callErr.Class != model.ErrorInvalid || callErr.Scope != ""
}

// probeStream reads one streaming branch until the first meaningful content,
// reasoning or tool-call event, buffering the prelude. Cancellations of losers
// are neutral for health and lease state.
func (r *Runner) probeStream(ctx context.Context, target model.Target, request model.ExecuteRequest) *branchResult {
	stream, callErr := r.executor.Stream(ctx, target, request)
	res := &branchResult{}
	if callErr != nil {
		res.err = callErr
		return res
	}
	buffered := make([]model.StreamEvent, 0, 4)
	for {
		select {
		case <-ctx.Done():
			class := model.ErrorCancelled
			status := 499
			if ctx.Err() == context.DeadlineExceeded {
				class = model.ErrorTimeout
				status = 504
			}
			res.err = &model.CallError{Class: class, Status: status, Cause: ctx.Err()}
			return res
		case event, ok := <-stream:
			if !ok {
				res.err = &model.CallError{Class: model.ErrorInvalid, Status: 502}
				return res
			}
			if event.Err != nil {
				res.err = normalizeContextError(ctx, event.Err)
				return res
			}
			buffered = append(buffered, event)
			if event.Meaningful {
				res.winner = true
				res.finished = r.now()
				res.selected = &SelectedStream{Buffered: buffered, Remaining: stream}
				return res
			}
		}
	}
}

// ModelDetails returns optional native metadata for a single-target logical model.
func (r *Runner) ModelDetails(ctx context.Context, logical string) (model.ModelInfo, error) {
	route, ok := r.config.Models[logical]
	if !ok || len(route.Pool) != 1 {
		return model.ModelInfo{}, nil
	}
	source, ok := r.executor.(interface {
		ModelDetails(context.Context, model.Target) (model.ModelInfo, error)
	})
	if !ok {
		return model.ModelInfo{}, nil
	}
	return source.ModelDetails(ctx, route.Pool[0])
}
