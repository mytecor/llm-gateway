package policy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/transform"
)

// ModelSpec declares a logical model in the simplified models/tiers/targets
// configuration format. It expresses routing intent (allowed models, tiers,
// label selectors and hard safety boundaries) while the compiler fills in
// execution scheduling defaults from the optional profile.
type ModelSpec struct {
	Tiers             []TierSpec       `json:"tiers"`
	Profile           string           `json:"profile,omitempty"`
	Timeout           Duration         `json:"timeout,omitempty"`
	StreamIdleTimeout Duration         `json:"stream_idle_timeout,omitempty"`
	SafeContinue      *bool            `json:"safe_continue,omitempty"`
	Continue          *ContinueSpec    `json:"continue,omitempty"`
	Budget            *BudgetSpec      `json:"budget,omitempty"`
	LoopGuard         *RepetitionSpec  `json:"loop_guard,omitempty"`
	Affinity          *AffinitySpec    `json:"affinity,omitempty"`
	Transforms        []transform.Kind `json:"transforms,omitempty"`
}

func (m *ModelSpec) UnmarshalJSON(data []byte) error {
	type plain ModelSpec
	var decoded plain
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&decoded); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON in model spec")
	}
	*m = ModelSpec(decoded)
	return nil
}

type TierSpec struct {
	Targets []TargetSpec `json:"targets"`
}

type TargetSpec struct {
	Provider string            `json:"provider,omitempty"`
	Select   map[string]string `json:"select,omitempty"`
	Native   string            `json:"native"`
}

type BudgetSpec struct {
	MaxCalls            int  `json:"max_calls,omitempty"`
	MaxInFlight         int  `json:"max_in_flight,omitempty"`
	MaxHedges           *int `json:"max_hedges,omitempty"`
	MaxCallsPerProvider int  `json:"max_calls_per_provider,omitempty"`
}

type ContinueSpec struct {
	Idle           Duration `json:"idle,omitempty"`
	Reshare        string   `json:"reshare,omitempty"`
	Retries        int      `json:"retries,omitempty"`
	Wait           Duration `json:"wait,omitempty"`
	MaxBufferBytes int      `json:"max_buffer_bytes,omitempty"`
}

type RepetitionSpec struct {
	Repeats int `json:"repeats,omitempty"`
	MinLen  int `json:"min_len,omitempty"`
	MaxLen  int `json:"max_len,omitempty"`
}

type AffinitySpec struct {
	Sources           []string `json:"sources"`
	TTL               Duration `json:"ttl"`
	OnMissing         string   `json:"on_missing,omitempty"`
	OnProviderFailure string   `json:"on_provider_failure,omitempty"`
}

type profileDefaults struct {
	routeTimeout        time.Duration
	hedgeEnabled        bool
	maxCallsCap         int
	maxCallsFloor       int
	maxInFlight         int
	maxHedges           int
	maxCallsPerProvider int
}

func resolveProfile(profile string) (profileDefaults, error) {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "", "balanced":
		return profileDefaults{
			routeTimeout:        3 * time.Minute,
			hedgeEnabled:        true,
			maxCallsCap:         6,
			maxCallsFloor:       4,
			maxInFlight:         2,
			maxHedges:           1,
			maxCallsPerProvider: 2,
		}, nil
	case "low-latency", "low_latency":
		return profileDefaults{
			routeTimeout:        2 * time.Minute,
			hedgeEnabled:        true,
			maxCallsCap:         4,
			maxCallsFloor:       3,
			maxInFlight:         2,
			maxHedges:           1,
			maxCallsPerProvider: 1,
		}, nil
	case "high-reliability", "high_reliability":
		return profileDefaults{
			routeTimeout:        5 * time.Minute,
			hedgeEnabled:        true,
			maxCallsCap:         8,
			maxCallsFloor:       6,
			maxInFlight:         2,
			maxHedges:           1,
			maxCallsPerProvider: 2,
		}, nil
	default:
		return profileDefaults{}, fmt.Errorf("unsupported model profile %q (expected \"balanced\", \"low-latency\", or \"high-reliability\")", profile)
	}
}

func compileModelSpecs(models map[string]ModelSpec, providers map[string]provider.Provider, result *routeCompileResult) error {
	if len(models) == 0 {
		return nil
	}
	names := sortedKeys(models)
	for _, rawName := range names {
		logicalID := strings.TrimSpace(rawName)
		if logicalID == "" {
			return errors.New("every declarative model requires a non-empty name")
		}
		spec := models[rawName]
		if _, dup := result.entries[logicalID]; dup {
			return fmt.Errorf("duplicate model %q: declared in both rules and models", logicalID)
		}
		if _, collision := result.routes[logicalID]; collision {
			return fmt.Errorf("model %q collides with existing route name", logicalID)
		}
		if err := compileSingleModelSpec(logicalID, spec, providers, result); err != nil {
			return err
		}
	}
	sort.Strings(result.models)
	return nil
}

func compileSingleModelSpec(logicalID string, spec ModelSpec, providers map[string]provider.Provider, result *routeCompileResult) error {
	prof, err := resolveProfile(spec.Profile)
	if err != nil {
		return fmt.Errorf("model %q: %w", logicalID, err)
	}
	if spec.Timeout.Duration < 0 || spec.StreamIdleTimeout.Duration < 0 {
		return fmt.Errorf("model %q: timeouts must not be negative", logicalID)
	}
	if len(spec.Tiers) == 0 {
		return fmt.Errorf("model %q: at least one tier is required", logicalID)
	}

	tierPools := make([][]model.Target, len(spec.Tiers))
	totalTargets := 0
	hasStateful := false
	for tierIdx, tier := range spec.Tiers {
		pool, stateful, err := resolveTierTargets(logicalID, tierIdx, tier, providers)
		if err != nil {
			return err
		}
		if stateful {
			hasStateful = true
		}
		tierPools[tierIdx] = pool
		totalTargets += len(pool)
	}

	routeTimeout := prof.routeTimeout
	if spec.Timeout.Duration > 0 {
		routeTimeout = spec.Timeout.Duration
	}

	continueCfg, err := compileModelContinue(logicalID, spec)
	if err != nil {
		return err
	}
	repCfg, err := compileModelRepetition(logicalID, spec.LoopGuard)
	if err != nil {
		return err
	}
	affinityCfg, err := compileModelAffinity(logicalID, spec.Affinity)
	if err != nil {
		return err
	}
	transforms, err := compileModelTransforms(logicalID, spec.Transforms)
	if err != nil {
		return err
	}
	semCfg, err := compileModelBudget(logicalID, spec.Budget, prof, totalTargets, hasStateful)
	if err != nil {
		return err
	}

	tierRoutes := make([]*CompiledRoute, len(tierPools))
	for tierIdx := len(tierPools) - 1; tierIdx >= 0; tierIdx-- {
		pool := tierPools[tierIdx]
		routeName := logicalID
		if tierIdx > 0 {
			routeName = fmt.Sprintf("%s#tier-%d", logicalID, tierIdx)
			if _, exists := result.routes[routeName]; exists {
				return fmt.Errorf("model %q tier %d route name %q collides with an existing route", logicalID, tierIdx, routeName)
			}
		}
		rt := &CompiledRoute{
			Name:           routeName,
			Pool:           pool,
			RaceCount:      1,
			AdaptiveTiming: !hasStateful,
		}
		if tierIdx > 0 {
			rt.ReschedulableOnly = true
		}
		if !hasStateful && len(pool) > 1 {
			rt.Retry = RetryConfig{
				Internal: true,
				Attempts: len(pool) - 1,
			}
			rt.Balance = BalanceConfig{
				Enabled:         true,
				Strategy:        "expected-ttft",
				Window:          5 * time.Minute,
				ErrorBudget:     0.2,
				ExplorationRate: 0.10,
			}
			if prof.hedgeEnabled && semCfg.MaxHedges > 0 {
				rt.Hedge = HedgeConfig{Enabled: true, Internal: true}
			}
		}
		if tierIdx+1 < len(tierRoutes) {
			rt.FallbackTarget = tierRoutes[tierIdx+1]
		}
		if tierIdx == 0 {
			rt.Entry = true
			rt.ModelEq = logicalID
			rt.Semaphore = semCfg
			rt.RouteTimeout = routeTimeout
			rt.StreamIdleTimeout = spec.StreamIdleTimeout.Duration
			rt.Admission = AdmissionConfig{
				Dynamic:     true,
				MaxPending:  32,
				WaitTimeout: 15 * time.Second,
			}
			rt.Continue = continueCfg
			rt.Repetition = repCfg
			rt.Transforms = transforms
			rt.Affinity = affinityCfg
		}
		tierRoutes[tierIdx] = rt
		result.routes[routeName] = rt
	}

	result.entries[logicalID] = tierRoutes[0]
	result.models = append(result.models, logicalID)
	return nil
}

func resolveTierTargets(logicalID string, tierIdx int, tier TierSpec, providers map[string]provider.Provider) ([]model.Target, bool, error) {
	if len(tier.Targets) == 0 {
		return nil, false, fmt.Errorf("model %q tier %d requires at least one target", logicalID, tierIdx)
	}
	type pair struct {
		provider string
		native   string
	}
	seen := make(map[pair]bool)
	var pool []model.Target
	hasStateful := false
	for targetIdx, spec := range tier.Targets {
		native := strings.TrimSpace(spec.Native)
		if native == "" {
			return nil, false, fmt.Errorf("model %q tier %d target %d requires a non-empty native model", logicalID, tierIdx, targetIdx)
		}
		provID := strings.TrimSpace(spec.Provider)
		hasProvider := provID != ""
		hasSelect := len(spec.Select) > 0
		if hasProvider == hasSelect {
			return nil, false, fmt.Errorf("model %q tier %d target %d must specify exactly one of provider or select", logicalID, tierIdx, targetIdx)
		}
		var matched []string
		if hasProvider {
			if _, ok := providers[provID]; !ok {
				return nil, false, fmt.Errorf("model %q tier %d target %d references unknown provider %q", logicalID, tierIdx, targetIdx, provID)
			}
			matched = []string{provID}
		} else {
			normSelect := make(map[string]string, len(spec.Select))
			for k, v := range spec.Select {
				key := strings.TrimSpace(k)
				val := strings.TrimSpace(v)
				if key == "" || val == "" {
					return nil, false, fmt.Errorf("model %q tier %d target %d selector requires non-empty label keys and values", logicalID, tierIdx, targetIdx)
				}
				normSelect[key] = val
			}
			for id, p := range providers {
				if providerMatchesLabels(p.Labels, normSelect) {
					matched = append(matched, id)
				}
			}
			if len(matched) == 0 {
				return nil, false, fmt.Errorf("model %q tier %d target %d select %v matched no providers", logicalID, tierIdx, targetIdx, normSelect)
			}
			sort.Slice(matched, func(i, j int) bool {
				left, right := providers[matched[i]], providers[matched[j]]
				if left.Priority != right.Priority {
					return left.Priority > right.Priority
				}
				return matched[i] < matched[j]
			})
		}
		for _, id := range matched {
			key := pair{provider: id, native: native}
			if seen[key] {
				continue
			}
			seen[key] = true
			if providers[id].Capabilities().Stateful {
				hasStateful = true
			}
			pool = append(pool, model.Target{
				Provider: id,
				Model:    native,
				Tier:     tierIdx,
			})
		}
	}
	return pool, hasStateful, nil
}

func providerMatchesLabels(labels, selector map[string]string) bool {
	if len(selector) == 0 || len(labels) < len(selector) {
		return false
	}
	for k, want := range selector {
		if labels[k] != want {
			return false
		}
	}
	return true
}

func compileModelBudget(logicalID string, spec *BudgetSpec, prof profileDefaults, totalTargets int, hasStateful bool) (SemaphoreConfig, error) {
	maxCalls := min(max(totalTargets, prof.maxCallsFloor), prof.maxCallsCap)
	if totalTargets == 1 && hasStateful {
		maxCalls = 1
	}
	maxInFlight := prof.maxInFlight
	if hasStateful {
		maxInFlight = 1
	}
	maxHedges := prof.maxHedges
	if hasStateful {
		maxHedges = 0
	}
	maxPerProvider := prof.maxCallsPerProvider
	if hasStateful {
		maxPerProvider = 1
	}
	if spec == nil {
		return SemaphoreConfig{
			MaxCalls:            maxCalls,
			MaxInFlight:         maxInFlight,
			MaxCallsPerProvider: maxPerProvider,
			MaxHedges:           maxHedges,
		}, nil
	}
	if spec.MaxCalls < 0 || spec.MaxInFlight < 0 || (spec.MaxHedges != nil && *spec.MaxHedges < 0) || spec.MaxCallsPerProvider < 0 {
		return SemaphoreConfig{}, fmt.Errorf("model %q: budget limits must not be negative", logicalID)
	}
	if spec.MaxCalls > 0 {
		maxCalls = spec.MaxCalls
	}
	if spec.MaxInFlight > 0 {
		maxInFlight = spec.MaxInFlight
	}
	if spec.MaxCallsPerProvider > 0 {
		maxPerProvider = spec.MaxCallsPerProvider
	}
	if spec.MaxHedges != nil {
		maxHedges = *spec.MaxHedges
	}
	return SemaphoreConfig{
		MaxCalls:            maxCalls,
		MaxInFlight:         maxInFlight,
		MaxCallsPerProvider: maxPerProvider,
		MaxHedges:           maxHedges,
	}, nil
}

func compileModelContinue(logicalID string, spec ModelSpec) (ContinueConfig, error) {
	if spec.SafeContinue == nil {
		if spec.Continue != nil {
			return ContinueConfig{}, fmt.Errorf("model %q: continue configuration requires safe_continue=true", logicalID)
		}
		return ContinueConfig{}, nil
	}
	if !*spec.SafeContinue {
		if spec.Continue != nil {
			return ContinueConfig{}, fmt.Errorf("model %q: safe_continue=false conflicts with continue configuration", logicalID)
		}
		return ContinueConfig{}, nil
	}
	idle := spec.StreamIdleTimeout.Duration
	if spec.Continue != nil && spec.Continue.Idle.Duration > 0 {
		idle = spec.Continue.Idle.Duration
	}
	if idle == 0 {
		idle = 5 * time.Minute
	}
	if idle < 5*time.Second {
		return ContinueConfig{}, fmt.Errorf("model %q: continue idle must be at least 5s", logicalID)
	}
	reshare := "full"
	retries := 2
	wait := continueDefaultWait
	maxBufferBytes := 0
	if spec.Continue != nil {
		if spec.Continue.Reshare != "" {
			reshare = spec.Continue.Reshare
		}
		if reshare != "full" {
			return ContinueConfig{}, fmt.Errorf("model %q: continue reshare supports only \"full\", got %q", logicalID, reshare)
		}
		if spec.Continue.Retries < 0 || spec.Continue.Retries > maxContinueChainRetries {
			return ContinueConfig{}, fmt.Errorf("model %q: continue retries must be between 0 and %d", logicalID, maxContinueChainRetries)
		}
		retries = spec.Continue.Retries
		if spec.Continue.Wait.Duration < 0 {
			return ContinueConfig{}, fmt.Errorf("model %q: continue wait must not be negative", logicalID)
		}
		if spec.Continue.Wait.Duration > 0 {
			wait = spec.Continue.Wait.Duration
		}
		if spec.Continue.MaxBufferBytes < 0 {
			return ContinueConfig{}, fmt.Errorf("model %q: continue max_buffer_bytes must not be negative", logicalID)
		}
		maxBufferBytes = spec.Continue.MaxBufferBytes
	}
	return ContinueConfig{
		Enabled:        true,
		Idle:           idle,
		Reshare:        reshare,
		Retries:        retries,
		Wait:           wait,
		MaxBufferBytes: maxBufferBytes,
	}, nil
}

func compileModelRepetition(logicalID string, spec *RepetitionSpec) (RepetitionConfig, error) {
	if spec == nil {
		return RepetitionConfig{}, nil
	}
	repeats := spec.Repeats
	if repeats == 0 {
		repeats = DefaultRepetitionRepeats
	}
	if repeats < MinRepetitionRepeats || repeats > MaxRepetitionRepeats {
		return RepetitionConfig{}, fmt.Errorf("model %q: loop_guard repeats must be between %d and %d", logicalID, MinRepetitionRepeats, MaxRepetitionRepeats)
	}
	minLen := spec.MinLen
	if minLen == 0 {
		minLen = DefaultRepetitionMinLen
	}
	if minLen < MinRepetitionMinLen {
		return RepetitionConfig{}, fmt.Errorf("model %q: loop_guard min_len must be at least %d", logicalID, MinRepetitionMinLen)
	}
	maxLen := spec.MaxLen
	if maxLen == 0 {
		maxLen = DefaultRepetitionMaxLen
	}
	if maxLen > MaxRepetitionMaxLen || maxLen < minLen {
		return RepetitionConfig{}, fmt.Errorf("model %q: loop_guard max_len (%d) must be >= min_len (%d) and <= %d", logicalID, maxLen, minLen, MaxRepetitionMaxLen)
	}
	return RepetitionConfig{
		Enabled: true,
		Repeats: repeats,
		MinLen:  minLen,
		MaxLen:  maxLen,
	}, nil
}

func compileModelAffinity(logicalID string, spec *AffinitySpec) (AffinityConfig, error) {
	if spec == nil {
		return AffinityConfig{}, nil
	}
	if len(spec.Sources) == 0 {
		return AffinityConfig{}, fmt.Errorf("model %q: affinity requires at least one source", logicalID)
	}
	for _, source := range spec.Sources {
		switch source {
		case "responses.conversation", "responses.previous_response_id":
		default:
			return AffinityConfig{}, fmt.Errorf("model %q: unsupported affinity source %q", logicalID, source)
		}
	}
	if spec.TTL.Duration <= 0 {
		return AffinityConfig{}, fmt.Errorf("model %q: affinity ttl must be positive", logicalID)
	}
	onMissing := spec.OnMissing
	if onMissing == "" {
		onMissing = "ignore"
	}
	if onMissing != "ignore" {
		return AffinityConfig{}, fmt.Errorf("model %q: unsupported affinity on_missing %q", logicalID, onMissing)
	}
	onFailure := spec.OnProviderFailure
	if onFailure == "" {
		onFailure = "fail-closed"
	}
	if onFailure != "fail-closed" {
		return AffinityConfig{}, fmt.Errorf("model %q: unsupported affinity on_provider_failure %q", logicalID, onFailure)
	}
	return AffinityConfig{
		Enabled:           true,
		Sources:           append([]string(nil), spec.Sources...),
		TTL:               spec.TTL.Duration,
		OnMissing:         onMissing,
		OnProviderFailure: onFailure,
	}, nil
}

func compileModelTransforms(logicalID string, list []transform.Kind) ([]transform.Kind, error) {
	if len(list) == 0 {
		return nil, nil
	}
	seen := make(map[transform.Kind]bool, len(list))
	out := make([]transform.Kind, 0, len(list))
	for _, kind := range list {
		if kind != transform.SplitThinking {
			return nil, fmt.Errorf("model %q: unsupported transform %q", logicalID, kind)
		}
		if seen[kind] {
			return nil, fmt.Errorf("model %q: duplicate transform %q", logicalID, kind)
		}
		seen[kind] = true
		out = append(out, kind)
	}
	return out, nil
}
