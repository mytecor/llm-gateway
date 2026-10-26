package policy

import (
	"fmt"
	"strings"
)

// Rule is the external routing-rule contract. Every concrete action owns only
// its own fields, validates itself and applies itself to the compiler state
// through apply. The unexported methods keep the implementation set closed
// inside this package: no single public struct carries the union of all
// action fields, and adding an action touches only ruleRegistry and one new
// implementation file.
type Rule interface {
	route() string
	action() string
	apply(ctx *stageContext) error
	isRule()
}

// ruleBase carries the genuinely common routing-rule data: the named route
// the rule belongs to and the action identity. Action-specific fields never
// live here; they belong to the concrete types that embed ruleBase. The route
// is a plain string naming a routing scope ("standard", "standard.retry",
// "standard.fallback"); dots are a naming convention and carry no runtime
// meaning.
type RuleBase struct {
	Route  string `json:"route"`
	Action string `json:"action"`
}

type ruleBase = RuleBase

func (b *RuleBase) route() string  { return strings.TrimSpace(b.Route) }
func (b *RuleBase) action() string { return strings.ToLower(strings.TrimSpace(b.Action)) }
func (b *RuleBase) isRule()        {}

func (b *RuleBase) SetIdentity(route, action string) {
	b.Route = route
	b.Action = action
}

func (b *RuleBase) setIdentity(route, action string) {
	b.SetIdentity(route, action)
}

// Routing action names. A route is a flat, ordered sequence of these actions:
// filter (selection/applicability), map (native mapping), rank, lease,
// balance, affinity, race (execution), retry/fallback/hedge (explicit
// transitions to other named routes), semaphore and timeout (request-wide
// safety).
const (
	ActionFilter     = "filter"
	ActionMap        = "map"
	ActionRank       = "rank"
	ActionAdmission  = "admission"
	ActionLease      = "lease"
	ActionBalance    = "balance"
	ActionAffinity   = "affinity"
	ActionRace       = "race"
	ActionRetry      = "retry"
	ActionHedge      = "hedge"
	ActionSemaphore  = "semaphore"
	ActionTimeout    = "timeout"
	ActionFallback   = "fallback"
	ActionContinue   = "continue"
	ActionRepetition = "repetition"
)

// ruleDescriptor declares one routing action: its canonical pipeline position
// and a factory used by the strict decoder. Adding a new action changes only
// this registry plus the new implementation file; no central switch is
// extended.
type ruleDescriptor struct {
	rank    int
	new     func() Rule
	aliases map[string][]string
}

// ruleRegistry is the action discriminator shared by the decoder and the
// compiler: an unknown action fails at the registry boundary before any
// concrete type is constructed.
var ruleRegistry = map[string]ruleDescriptor{
	ActionFilter:     {rank: 1, new: func() Rule { return &FilterRule{} }},
	ActionMap:        {rank: 2, new: func() Rule { return &MapRule{} }},
	ActionRank:       {rank: 3, new: func() Rule { return &RankRule{} }},
	ActionAdmission:  {rank: 4, new: func() Rule { return &AdmissionRule{} }, aliases: map[string][]string{"max_in_flight": {"maxInFlight"}, "max_pending": {"maxPending"}, "wait_timeout": {"waitTimeout"}}},
	ActionLease:      {rank: 5, new: func() Rule { return &LeaseRule{} }},
	ActionBalance:    {rank: 5, new: func() Rule { return &BalanceRule{} }},
	ActionAffinity:   {rank: 6, new: func() Rule { return &AffinityRule{} }},
	ActionRace:       {rank: 7, new: func() Rule { return &RaceRule{} }},
	ActionRetry:      {rank: 8, new: func() Rule { return &RetryRule{} }, aliases: map[string][]string{"attempts": {"max_attempts", "maxAttempts"}}},
	ActionHedge:      {rank: 9, new: func() Rule { return &HedgeRule{} }},
	ActionSemaphore:  {rank: 10, new: func() Rule { return &SemaphoreRule{} }},
	ActionTimeout:    {rank: 11, new: func() Rule { return &TimeoutRule{} }},
	ActionFallback:   {rank: 12, new: func() Rule { return &FallbackRule{} }},
	ActionContinue:   {rank: 13, new: func() Rule { return &ContinueRule{} }},
	ActionRepetition: {rank: 14, new: func() Rule { return &RepetitionRule{} }},
}

// ruleErrf formats a rule error that always carries the rule index,
// the named route and the action followed by the concrete cause (never
// credentials or internal URLs).
func ruleErrf(index int, route, action, format string, args ...any) error {
	return fmt.Errorf("rule %d (route %q, action %q): %s",
		index, route, action, fmt.Sprintf(format, args...))
}
