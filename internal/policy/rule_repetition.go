package policy

import (

)

// Repetition rule validation bounds and defaults. They keep the detector from
// tripping on natural speech while leaving the operator an explicit budget for
// tightening or loosening the guard on a specific logical model.
const (
	MinRepetitionRepeats = 2
	MaxRepetitionRepeats = 100
	MinRepetitionMinLen = 2
	DefaultRepetitionRepeats = 4
	DefaultRepetitionMinLen = 6
	DefaultRepetitionMaxLen = 256
	MaxRepetitionMaxLen = 1024

	minRepetitionRepeats     = MinRepetitionRepeats
	maxRepetitionRepeats     = MaxRepetitionRepeats
	minRepetitionMinLen      = MinRepetitionMinLen
	defaultRepetitionRepeats = DefaultRepetitionRepeats
	defaultRepetitionMinLen  = DefaultRepetitionMinLen
	defaultRepetitionMaxLen  = DefaultRepetitionMaxLen
	maxRepetitionMaxLen      = MaxRepetitionMaxLen
)

// RepetitionRule declares the in-gateway loop-guard policy for a route. It is
// an optional, opt-in typed routing action: its presence on an entry route
// arms the repetition detector on that logical model's relayed winner stream.
// Unlike continue there is no built-in loop detection in the gateway core —
// an absent rule means the relay behaves exactly as before. When the
// accumulated output of a relayed stream trips the guard (the same normalized
// fragment repeated K+ consecutive times), the gateway stops the stream and
// re-dispatches through the existing continue path with the accumulated
// partial output reshared, so the successor provider continues instead of the
// client watching an endless "Tool call. Tool call. …" loop (observed
// 2026-09-24 on the `standard` model via llm-gateway).
type RepetitionRule struct {
	RuleBase
	// Repeats is K: the number of consecutive identical normalized fragments
	// that trip the guard. Omitted/zero defaults to defaultRepetitionRepeats.
	Repeats int `json:"repeats,omitempty"`
	// MinLen is the smallest normalized fragment length considered. Omitted/
	// zero defaults to defaultRepetitionMinLen.
	MinLen int `json:"min_len,omitempty"`
	// MaxLen is the largest normalized fragment length scanned. Omitted/zero
	// defaults to defaultRepetitionMaxLen.
	MaxLen int `json:"max_len,omitempty"`
}

// apply validates the loop-guard policy and stores it in the compiled entry
// route. Like continue it must sit on an entry route after a race; it never
// enables anything by default.
func (r *RepetitionRule) apply(ctx *stageContext) error {
	if !ctx.st.sawRace {
		return ctx.errf("repetition requires a preceding race action")
	}
	if !ctx.st.entrySet {
		return ctx.errf("repetition must be declared on an entry route (the route needs a filter where.model) so the logical model and its relay policy are known")
	}
	repeats := r.Repeats
	if repeats == 0 {
		repeats = defaultRepetitionRepeats
	}
	if repeats < minRepetitionRepeats {
		return ctx.errf("repetition repeats must be at least %d, got %d", minRepetitionRepeats, repeats)
	}
	if repeats > maxRepetitionRepeats {
		return ctx.errf("repetition repeats exceeds the cap of %d, got %d", maxRepetitionRepeats, repeats)
	}
	minLen := r.MinLen
	if minLen == 0 {
		minLen = defaultRepetitionMinLen
	}
	if minLen < minRepetitionMinLen {
		return ctx.errf("repetition min_len must be at least %d, got %d", minRepetitionMinLen, minLen)
	}
	maxLen := r.MaxLen
	if maxLen == 0 {
		maxLen = defaultRepetitionMaxLen
	}
	if maxLen > maxRepetitionMaxLen {
		return ctx.errf("repetition max_len exceeds the cap of %d, got %d", maxRepetitionMaxLen, maxLen)
	}
	if minLen >= maxLen {
		return ctx.errf("repetition min_len (%d) must be strictly less than max_len (%d)", minLen, maxLen)
	}
	ctx.plan.Repetition = RepetitionConfig{
		Enabled: true,
		Repeats: repeats,
		MinLen:  minLen,
		MaxLen:  maxLen,
	}
	return nil
}
