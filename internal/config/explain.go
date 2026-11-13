package config

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/transform"
)

type ConfigExplanation struct {
	Providers []ProviderExplanation `json:"providers"`
	Models    []ModelExplanation    `json:"models"`
}

type ProviderExplanation struct {
	ID         string            `json:"id"`
	Driver     string            `json:"driver"`
	Priority   int               `json:"priority"`
	Labels     map[string]string `json:"labels,omitempty"`
	SafeReplay bool              `json:"safe_replay"`
	Stateful   bool              `json:"stateful"`
}

type ModelExplanation struct {
	Model             string                  `json:"model"`
	Profile           string                  `json:"profile,omitempty"`
	RouteTimeout      string                  `json:"route_timeout,omitempty"`
	StreamIdleTimeout string                  `json:"stream_idle_timeout,omitempty"`
	AdaptiveTiming    bool                    `json:"adaptive_timing"`
	SafeContinue      bool                    `json:"safe_continue"`
	HedgeEnabled      bool                    `json:"hedge_enabled"`
	Budget            BudgetExplanation       `json:"budget"`
	Admission         AdmissionExplain        `json:"admission"`
	Balance           BalanceExplain          `json:"balance"`
	Tiers             []TierExplanation       `json:"tiers"`
	Routes            []RouteExplanation      `json:"routes"`
	Transitions       []TransitionExplanation `json:"transitions,omitempty"`
	Transforms        []transform.Kind        `json:"transforms,omitempty"`
}

type BudgetExplanation struct {
	MaxCalls            int `json:"max_calls"`
	MaxInFlight         int `json:"max_in_flight"`
	MaxHedges           int `json:"max_hedges"`
	MaxCallsPerProvider int `json:"max_calls_per_provider"`
}

type AdmissionExplain struct {
	Dynamic     bool   `json:"dynamic"`
	MaxInFlight int    `json:"max_in_flight"`
	MaxPending  int    `json:"max_pending"`
	WaitTimeout string `json:"wait_timeout,omitempty"`
}

type BalanceExplain struct {
	Enabled         bool    `json:"enabled"`
	Strategy        string  `json:"strategy,omitempty"`
	Window          string  `json:"window,omitempty"`
	ErrorBudget     float64 `json:"error_budget,omitempty"`
	ExplorationRate float64 `json:"exploration_rate,omitempty"`
}

type RouteExplanation struct {
	Route             string              `json:"route"`
	ReschedulableOnly bool                `json:"reschedulable_only,omitempty"`
	RetryAttempts     int                 `json:"retry_attempts,omitempty"`
	RaceCount         int                 `json:"race_count"`
	ProviderUnused    bool                `json:"provider_unused,omitempty"`
	ErrorIn           []string            `json:"error_in,omitempty"`
	AttemptLT         int                 `json:"attempt_lt,omitempty"`
	Targets           []TargetExplanation `json:"targets"`
}

// Tiers remains the ordered fallback chain; recovery routes are represented
// separately so retries and hedges are not misreported as quality tiers.
type TierExplanation = RouteExplanation

type TransitionExplanation struct {
	From           string `json:"from"`
	To             string `json:"to"`
	Kind           string `json:"kind"`
	Internal       bool   `json:"internal,omitempty"`
	Attempts       int    `json:"attempts,omitempty"`
	BackoffType    string `json:"backoff_type,omitempty"`
	BackoffInitial string `json:"backoff_initial,omitempty"`
	BackoffMax     string `json:"backoff_max,omitempty"`
}

type TargetExplanation struct {
	Provider string `json:"provider"`
	Native   string `json:"native"`
	Priority int    `json:"priority"`
}

// ExplainConfig validates, compiles, and builds a human/CI-inspectable summary
// of every provider and resolved logical model routing graph.
func ExplainConfig(path string, requireEnvSecrets bool, allowedEnv string) (ConfigExplanation, error) {
	cfg, err := DecodeConfig(path)
	if err != nil {
		return ConfigExplanation{}, err
	}
	if requireEnvSecrets {
		if err := ValidateEnvSecretReferences(cfg, ParseAllowedEnv(allowedEnv)); err != nil {
			return ConfigExplanation{}, fmt.Errorf("validate config: %w", err)
		}
	}
	compiled, err := PrepareAndCompile(cfg)
	if err != nil {
		return ConfigExplanation{}, fmt.Errorf("validate config: %w", err)
	}
	return ExplainCompiled(compiled), nil
}

// ExplainCompiled summarizes an already compiled configuration.
func ExplainCompiled(compiled *CompiledConfig) ConfigExplanation {
	if compiled == nil {
		return ConfigExplanation{}
	}
	provIDs := make([]string, 0, len(compiled.Providers))
	for id := range compiled.Providers {
		provIDs = append(provIDs, id)
	}
	sort.Strings(provIDs)

	providers := make([]ProviderExplanation, 0, len(provIDs))
	for _, id := range provIDs {
		p := compiled.Providers[id]
		safeReplay := p.Description == nil || (!p.Capabilities().Stateful && p.Capabilities().SafeReplay)
		providers = append(providers, ProviderExplanation{
			ID:         p.ID,
			Driver:     p.Driver,
			Priority:   p.Priority,
			Labels:     p.Labels,
			SafeReplay: safeReplay,
			Stateful:   p.Capabilities().Stateful,
		})
	}

	models := make([]ModelExplanation, 0, len(compiled.LogicalIDs))
	for _, id := range compiled.LogicalIDs {
		entry := compiled.Models[id]
		if entry == nil {
			continue
		}
		profile := ""
		if spec, ok := compiled.Raw.Models[id]; ok {
			profile = spec.Profile
			if profile == "" {
				profile = "balanced"
			}
		} else {
			profile = "rules"
		}
		me := ModelExplanation{
			Model:          id,
			Profile:        profile,
			AdaptiveTiming: entry.AdaptiveTiming,
			SafeContinue:   entry.Continue.Enabled,
			HedgeEnabled:   entry.Hedge.Enabled || entry.HedgeTarget != nil,
			Transforms:     append([]transform.Kind(nil), entry.Transforms...),
			Budget: BudgetExplanation{
				MaxCalls:            entry.Semaphore.MaxCalls,
				MaxInFlight:         entry.Semaphore.MaxInFlight,
				MaxHedges:           entry.Semaphore.MaxHedges,
				MaxCallsPerProvider: entry.Semaphore.MaxCallsPerProvider,
			},
			Admission: AdmissionExplain{
				Dynamic:     entry.Admission.Dynamic,
				MaxInFlight: entry.Admission.MaxInFlight,
				MaxPending:  entry.Admission.MaxPending,
			},
			Balance: BalanceExplain{
				Enabled:         entry.Balance.Enabled,
				Strategy:        entry.Balance.Strategy,
				ErrorBudget:     entry.Balance.ErrorBudget,
				ExplorationRate: entry.Balance.ExplorationRate,
			},
		}
		if entry.RouteTimeout > 0 {
			me.RouteTimeout = entry.RouteTimeout.String()
		}
		if entry.StreamIdleTimeout > 0 {
			me.StreamIdleTimeout = entry.StreamIdleTimeout.String()
		}
		if entry.Admission.WaitTimeout > 0 {
			me.Admission.WaitTimeout = entry.Admission.WaitTimeout.String()
		}
		if entry.Balance.Window > 0 {
			me.Balance.Window = entry.Balance.Window.String()
		}

		seen := make(map[*policy.CompiledRoute]bool)
		for cur := entry; cur != nil && !seen[cur]; cur = cur.FallbackTarget {
			seen[cur] = true
			me.Tiers = append(me.Tiers, explainRoute(cur, compiled.Providers))
		}
		me.Routes, me.Transitions = explainRouteGraph(entry, compiled.Providers)
		models = append(models, me)
	}

	return ConfigExplanation{
		Providers: providers,
		Models:    models,
	}
}

func explainRoute(route *policy.CompiledRoute, providers map[string]provider.Provider) RouteExplanation {
	exp := RouteExplanation{Route: route.Name, ReschedulableOnly: route.ReschedulableOnly,
		RetryAttempts: route.Retry.Attempts, RaceCount: route.RaceCount,
		ProviderUnused: route.ProviderUnused, AttemptLT: route.AttemptLT,
		Targets: make([]TargetExplanation, 0, len(route.Pool))}
	for class, enabled := range route.ErrorIn {
		if enabled {
			exp.ErrorIn = append(exp.ErrorIn, string(class))
		}
	}
	sort.Strings(exp.ErrorIn)
	for _, target := range route.Pool {
		exp.Targets = append(exp.Targets, TargetExplanation{Provider: target.Provider, Native: target.Model, Priority: providers[target.Provider].Priority})
	}
	return exp
}

func explainRouteGraph(entry *policy.CompiledRoute, providers map[string]provider.Provider) ([]RouteExplanation, []TransitionExplanation) {
	routes := make([]RouteExplanation, 0)
	var edges []TransitionExplanation
	seen := make(map[*policy.CompiledRoute]bool)
	var visit func(*policy.CompiledRoute)
	visit = func(route *policy.CompiledRoute) {
		if route == nil || seen[route] {
			return
		}
		seen[route] = true
		routes = append(routes, explainRoute(route, providers))
		var next []*policy.CompiledRoute
		add := func(kind string, target *policy.CompiledRoute, internal bool, attempts int) {
			if target == nil {
				return
			}
			edge := TransitionExplanation{From: route.Name, To: target.Name, Kind: kind, Internal: internal, Attempts: attempts}
			if kind == "retry" {
				edge.BackoffType = route.Retry.Backoff.Type
				if route.Retry.Backoff.Initial.Duration > 0 {
					edge.BackoffInitial = route.Retry.Backoff.Initial.Duration.String()
				}
				if route.Retry.Backoff.Max.Duration > 0 {
					edge.BackoffMax = route.Retry.Backoff.Max.Duration.String()
				}
			}
			edges = append(edges, edge)
			next = append(next, target)
		}
		if route.Retry.Attempts > 0 {
			target := route.RetryTarget
			if route.Retry.Internal {
				target = route
			}
			add("retry", target, route.Retry.Internal, route.Retry.Attempts)
		}
		add("fallback", route.FallbackTarget, false, 1)
		if route.HedgeTarget != nil {
			add("hedge", route.HedgeTarget, false, 1)
		} else if route.Hedge.Enabled && route.Hedge.Internal {
			add("hedge", route, true, 1)
		}
		for _, target := range next {
			visit(target)
		}
	}
	visit(entry)
	return routes, edges
}

// MarshalExplanation formats an explanation as indented JSON.
func MarshalExplanation(exp ConfigExplanation) ([]byte, error) {
	return json.MarshalIndent(exp, "", "  ")
}
