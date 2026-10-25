package policy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mytecor/llm-gateway/internal/executor/external"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

const defaultCatalogRefresh = 10 * time.Minute

// defaultStreamIdleTimeout bounds a silently stalled winner stream: any event
// (including provider keep-alives) re-arms the timer. The default is
// deliberately conservative because reasoning models may pause mid-stream
// without emitting events for a long time.
const defaultStreamIdleTimeout = 5 * time.Minute

// CheckConfig validates a configuration file as requested by the "check" CLI command.
func CheckConfig(path string, requireEnvSecrets bool, allowedEnv string) error {
	cfg, err := DecodeConfig(path)
	if err != nil {
		return err
	}
	if requireEnvSecrets {
		if err := ValidateEnvSecretReferences(cfg, ParseAllowedEnv(allowedEnv)); err != nil {
			return fmt.Errorf("validate config: %w", err)
		}
	}
	if _, err := CompileConfig(cfg); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}
	return nil
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		d.Duration = 0
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return errors.New("duration must be a string such as 500ms or 10m")
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value, err)
	}
	d.Duration = parsed
	return nil
}

type Config struct {
	Host        string `json:"host"`
	Port        int    `json:"port"`
	MetricsHost string `json:"metrics_host,omitempty"`
	MetricsPort int    `json:"metrics_port,omitempty"`
	LogLevel    string `json:"log_level"`
	// ClientAPIKeys is the set of named client credentials the gateway
	// accepts. Each request is attributed to the matching key's ID (a
	// non-secret label) in the request/usage metrics; an empty list disables
	// client authentication entirely (keyless loopback mode). Key values are
	// literal secrets or "env.VARNAME" references resolved at load time.
	ClientAPIKeys          []ClientKey `json:"client_api_keys"`
	CatalogRefreshInterval Duration    `json:"catalog_refresh_interval"`
	StreamIdleTimeout      Duration    `json:"stream_idle_timeout"`
	AffinityFile           string      `json:"affinity_file,omitempty"`
	SessionFile            string      `json:"session_file,omitempty"`
	Providers              []Provider  `json:"providers"`
	Rules                  Rules       `json:"rules"`
}

// ClientKey is one named client credential. ID is the non-secret identity
// used as the metrics api_key dimension and to attribute requests (never the
// key material itself); Key is the bearer secret, either literal or an
// "env.VARNAME" reference resolved at load time.
type ClientKey struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

type SessionSettings struct {
	DefaultCWD   string   `json:"default_cwd"`
	AllowedRoots []string `json:"allowed_roots,omitempty"`
	TTL          Duration `json:"ttl,omitempty"`
	TurnTimeout  Duration `json:"turn_timeout,omitempty"`
	Heartbeat    Duration `json:"heartbeat,omitempty"`
}

type Provider struct {
	ID                 string            `json:"id"`
	Driver             string            `json:"driver,omitempty"`
	Command            []string          `json:"command,omitempty"`
	Env                map[string]string `json:"env,omitempty"`
	Priority           int               `json:"priority,omitempty"`
	Weight             int               `json:"weight,omitempty"`
	MaxConcurrent      int               `json:"max_concurrent,omitempty"`
	MaxConcurrentCamel int               `json:"maxConcurrent,omitempty"`
	Cooldown           Duration          `json:"cooldown,omitempty"`
	RequestTimeout     Duration          `json:"request_timeout,omitempty"`
	ModelsURL          string            `json:"models_url,omitempty"`
	ToolStrategy       string            `json:"tool_strategy,omitempty"`
	Session            *SessionSettings  `json:"session,omitempty"`
	Config             map[string]any    `json:"config,omitempty"`

	capabilities model.Capabilities
}

var providerCoreKeys = map[string]bool{
	"id":              true,
	"driver":          true,
	"command":         true,
	"env":             true,
	"priority":        true,
	"weight":          true,
	"max_concurrent":  true,
	"maxConcurrent":   true,
	"cooldown":        true,
	"request_timeout": true,
	"models_url":      true,
	"tool_strategy":   true,
	"session":         true,
	"config":          true,
}

type providerCoreAlias struct {
	ID                 string            `json:"id"`
	Driver             string            `json:"driver,omitempty"`
	Command            []string          `json:"command,omitempty"`
	Env                map[string]string `json:"env,omitempty"`
	Priority           int               `json:"priority,omitempty"`
	Weight             int               `json:"weight,omitempty"`
	MaxConcurrent      int               `json:"max_concurrent,omitempty"`
	MaxConcurrentCamel int               `json:"maxConcurrent,omitempty"`
	Cooldown           Duration          `json:"cooldown,omitempty"`
	RequestTimeout     Duration          `json:"request_timeout,omitempty"`
	ModelsURL          string            `json:"models_url,omitempty"`
	ToolStrategy       string            `json:"tool_strategy,omitempty"`
	Session            *SessionSettings  `json:"session,omitempty"`
}

func (p *Provider) UnmarshalJSON(data []byte) error {
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &rawMap); err != nil {
		return err
	}
	coreMap := make(map[string]json.RawMessage, len(rawMap))
	for k, v := range rawMap {
		if providerCoreKeys[k] && k != "config" {
			coreMap[k] = v
		}
	}
	coreBytes, err := json.Marshal(coreMap)
	if err != nil {
		return err
	}
	var core providerCoreAlias
	dec := json.NewDecoder(bytes.NewReader(coreBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&core); err != nil {
		return err
	}

	cfgMap := make(map[string]any)
	if rawCfg, ok := rawMap["config"]; ok {
		trimmed := bytes.TrimSpace(rawCfg)
		if len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null")) {
			if err := json.Unmarshal(trimmed, &cfgMap); err != nil {
				return fmt.Errorf("provider config must be a JSON object: %w", err)
			}
		}
	}
	for k, v := range rawMap {
		if providerCoreKeys[k] {
			continue
		}
		var val any
		if err := json.Unmarshal(v, &val); err != nil {
			return err
		}
		cfgMap[k] = val
	}
	if len(cfgMap) == 0 {
		cfgMap = nil
	}

	*p = Provider{
		ID:                 core.ID,
		Driver:             core.Driver,
		Command:            core.Command,
		Env:                core.Env,
		Priority:           core.Priority,
		Weight:             core.Weight,
		MaxConcurrent:      core.MaxConcurrent,
		MaxConcurrentCamel: core.MaxConcurrentCamel,
		Cooldown:           core.Cooldown,
		RequestTimeout:     core.RequestTimeout,
		ModelsURL:          core.ModelsURL,
		ToolStrategy:       core.ToolStrategy,
		Session:            core.Session,
		Config:             cfgMap,
	}
	return nil
}

type BackoffConfig struct {
	Type    string   `json:"type"`
	Initial Duration `json:"initial"`
	Max     Duration `json:"max"`
}

// CompiledConfig separates external rules from the compiled immutable routing
// graph: named routes, their candidate pools, dispatch batches, explicit
// transition references, semaphore limits and lease and affinity policy.
// Catalog sources are indexed by provider ID: each provider owns its
// last-known-good snapshot and exact native validation.
type CompiledConfig struct {
	Raw            Config
	Logger         *slog.Logger
	Providers      map[string]Provider
	Routes         map[string]*CompiledRoute
	Models         map[string]*CompiledRoute // logical model → entry route
	LogicalIDs     []string
	CatalogSources map[string][]CatalogSource
	// ClientKeys maps a client key ID (non-secret metrics label) to its
	// resolved bearer secret. Empty means client authentication is disabled.
	ClientKeys map[string]string
}

type CatalogSource struct {
	URL      string
	APIKey   string
	Explicit bool
}

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

func ParseAllowedEnv(value string) map[string]bool {
	allowed := make(map[string]bool)
	if value == "" {
		return allowed
	}
	for _, name := range strings.Split(value, ",") {
		allowed[name] = true
	}
	return allowed
}

func parseAllowedEnv(value string) map[string]bool {
	return ParseAllowedEnv(value)
}

func loadConfig(path string) (Config, error) {
	return LoadConfig(path)
}

func LoadConfig(path string) (Config, error) {
	cfg, err := decodeConfig(path)
	if err != nil {
		return Config{}, err
	}
	if err := resolveConfigSecrets(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// decodeConfig reads and strictly decodes the public configuration without
// resolving env.NAME secret references. Keeping this boundary separate lets
// `llm-gateway check` validate a generated config in a Nix build, where the
// runtime credentials deliberately do not exist.
func decodeConfig(path string) (Config, error) {
	return DecodeConfig(path)
}

func DecodeConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("decode config: trailing JSON content")
	}
	return cfg, nil
}

func resolveConfigSecrets(cfg *Config) error {
	for i := range cfg.ClientAPIKeys {
		key, err := resolveSecret(cfg.ClientAPIKeys[i].Key)
		if err != nil {
			return fmt.Errorf("client_api_keys[%d] (%s): %w", i, cfg.ClientAPIKeys[i].ID, err)
		}
		cfg.ClientAPIKeys[i].Key = key
	}
	for i := range cfg.Providers {
		for k, v := range cfg.Providers[i].Env {
			resolved, err := resolveSecret(v)
			if err != nil {
				return fmt.Errorf("provider %q env[%s]: %w", cfg.Providers[i].ID, k, err)
			}
			cfg.Providers[i].Env[k] = resolved
		}
		if len(cfg.Providers[i].Config) > 0 {
			resolved, err := resolveValueSecrets(cfg.Providers[i].ID, "", cfg.Providers[i].Config)
			if err != nil {
				return err
			}
			cfg.Providers[i].Config = resolved.(map[string]any)
		}
	}
	return nil
}

func resolveValueSecrets(providerID, key string, val any) (any, error) {
	switch v := val.(type) {
	case string:
		res, err := resolveSecret(v)
		if err != nil {
			return nil, fmt.Errorf("provider %q %s: %w", providerID, key, err)
		}
		return res, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			res, err := resolveValueSecrets(providerID, k, item)
			if err != nil {
				return nil, err
			}
			out[k] = res
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			res, err := resolveValueSecrets(providerID, key, item)
			if err != nil {
				return nil, err
			}
			out[i] = res
		}
		return out, nil
	case []string:
		out := make([]string, len(v))
		for i, item := range v {
			res, err := resolveSecret(item)
			if err != nil {
				return nil, fmt.Errorf("provider %q %s: %w", providerID, key, err)
			}
			out[i] = res
		}
		return out, nil
	default:
		return val, nil
	}
}

func isSecretKey(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	return k == "key" ||
		strings.HasSuffix(k, "_key") ||
		strings.HasSuffix(k, "_secret") ||
		strings.HasSuffix(k, "_token") ||
		strings.HasSuffix(k, "_password") ||
		strings.HasSuffix(k, "_credentials")
}

// validateEnvSecretReferences enforces the NixOS public-config boundary:
// credentials may be absent, but every configured value must be an env.NAME
// reference with a portable environment-variable name. Runtime loading still
// resolves and requires the referenced value separately.
func validateEnvSecretReferences(cfg Config, allowed map[string]bool) error {
	return ValidateEnvSecretReferences(cfg, allowed)
}

func ValidateEnvSecretReferences(cfg Config, allowed map[string]bool) error {
	referenced := make(map[string]bool)
	for i, clientKey := range cfg.ClientAPIKeys {
		name, err := validateEnvSecretReference(clientKey.Key)
		if err != nil {
			return fmt.Errorf("client_api_keys[%d] (%s): %w", i, clientKey.ID, err)
		}
		if name != "" {
			referenced[name] = true
		}
	}
	for _, provider := range cfg.Providers {
		for k, v := range provider.Env {
			if isSecretKey(k) || strings.HasPrefix(v, "env.") {
				name, err := validateEnvSecretReference(v)
				if err != nil {
					return fmt.Errorf("provider %q env[%s]: %w", provider.ID, k, err)
				}
				if name != "" {
					referenced[name] = true
				}
			}
		}
		if err := validateValueEnvSecrets(provider.ID, "", provider.Config, referenced); err != nil {
			return err
		}
	}
	for name := range referenced {
		if allowed != nil && !allowed[name] {
			return fmt.Errorf("environment variable %s is referenced but not declared", name)
		}
	}
	for name := range allowed {
		if !referenced[name] {
			return fmt.Errorf("environment variable %s is declared but not referenced", name)
		}
	}
	return nil
}

func validateValueEnvSecrets(providerID, key string, val any, referenced map[string]bool) error {
	switch v := val.(type) {
	case string:
		if isSecretKey(key) || strings.HasPrefix(v, "env.") {
			name, err := validateEnvSecretReference(v)
			if err != nil {
				return fmt.Errorf("provider %q %s: %w", providerID, key, err)
			}
			if name != "" {
				referenced[name] = true
			}
		}
		return nil
	case map[string]any:
		for k, item := range v {
			if err := validateValueEnvSecrets(providerID, k, item, referenced); err != nil {
				return err
			}
		}
		return nil
	case []any:
		for _, item := range v {
			if err := validateValueEnvSecrets(providerID, key, item, referenced); err != nil {
				return err
			}
		}
		return nil
	case []string:
		for _, item := range v {
			if isSecretKey(key) || strings.HasPrefix(item, "env.") {
				name, err := validateEnvSecretReference(item)
				if err != nil {
					return fmt.Errorf("provider %q %s: %w", providerID, key, err)
				}
				if name != "" {
					referenced[name] = true
				}
			}
		}
		return nil
	default:
		return nil
	}
}

func validateEnvSecretReference(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if !strings.HasPrefix(value, "env.") {
		return "", errors.New("literal secret is forbidden; use env.NAME")
	}
	name := strings.TrimPrefix(value, "env.")
	for index, char := range name {
		if (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || char == '_' || (index > 0 && char >= '0' && char <= '9') {
			continue
		}
		return "", fmt.Errorf("invalid environment variable name %q", name)
	}
	if name == "" {
		return "", errors.New("empty environment variable name")
	}
	return name, nil
}

func resolveSecret(value string) (string, error) {
	if !strings.HasPrefix(value, "env.") {
		return value, nil
	}
	name := strings.TrimPrefix(value, "env.")
	if name == "" {
		return "", errors.New("empty environment variable name")
	}
	secret, ok := os.LookupEnv(name)
	if !ok || secret == "" {
		return "", fmt.Errorf("environment variable %s is not set", name)
	}
	return secret, nil
}

func compileConfig(cfg Config) (*CompiledConfig, error) {
	return CompileConfig(cfg)
}

func CompileConfig(cfg Config) (*CompiledConfig, error) {
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == 0 {
		cfg.Port = 9208
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return nil, fmt.Errorf("port must be between 1 and 65535")
	}
	if err := validateMetricsBind(cfg.MetricsHost, cfg.MetricsPort); err != nil {
		return nil, err
	}
	if cfg.MetricsHost == "" {
		cfg.MetricsHost = "127.0.0.1"
	}
	if cfg.MetricsPort == 0 {
		cfg.MetricsPort = 9209
	}
	if cfg.MetricsHost == cfg.Host && cfg.MetricsPort == cfg.Port {
		return nil, errors.New("metrics listener must not share the API host:port")
	}
	if cfg.CatalogRefreshInterval.Duration == 0 {
		cfg.CatalogRefreshInterval.Duration = defaultCatalogRefresh
	}
	if cfg.CatalogRefreshInterval.Duration < time.Second {
		return nil, fmt.Errorf("catalog_refresh_interval must be at least 1s")
	}
	if cfg.StreamIdleTimeout.Duration == 0 {
		cfg.StreamIdleTimeout.Duration = defaultStreamIdleTimeout
	}
	if cfg.StreamIdleTimeout.Duration < time.Second {
		return nil, fmt.Errorf("stream_idle_timeout must be at least 1s")
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "silent"
	}
	switch cfg.LogLevel {
	case "silent", "error", "warn", "info", "debug", "trace":
	default:
		return nil, fmt.Errorf("unsupported log_level %q", cfg.LogLevel)
	}
	if len(cfg.Providers) == 0 {
		return nil, errors.New("at least one provider is required")
	}
	cfg.SessionFile = strings.TrimSpace(cfg.SessionFile)
	if cfg.SessionFile != "" {
		path, err := filepath.Abs(cfg.SessionFile)
		if err != nil {
			return nil, fmt.Errorf("resolve session_file: %w", err)
		}
		cfg.SessionFile = path
	}

	compiled := &CompiledConfig{
		Raw:            cfg,
		Logger:         telemetry.NewGatewayLogger(cfg.LogLevel),
		Providers:      make(map[string]Provider, len(cfg.Providers)),
		CatalogSources: make(map[string][]CatalogSource),
		ClientKeys:     make(map[string]string, len(cfg.ClientAPIKeys)),
	}
	for _, clientKey := range cfg.ClientAPIKeys {
		id := strings.TrimSpace(clientKey.ID)
		if id == "" {
			return nil, errors.New("every client key requires a non-empty id")
		}
		if _, duplicate := compiled.ClientKeys[id]; duplicate {
			return nil, fmt.Errorf("duplicate client key id %q", id)
		}
		if clientKey.Key == "" {
			return nil, fmt.Errorf("client key %q must not be empty", id)
		}
		compiled.ClientKeys[id] = clientKey.Key
	}
	for _, provider := range cfg.Providers {
		provider.ID = strings.TrimSpace(provider.ID)
		provider.ModelsURL = strings.TrimSpace(provider.ModelsURL)
		provider.Driver = strings.ToLower(strings.TrimSpace(provider.Driver))
		if provider.Driver == "" && len(provider.Command) == 0 {
			provider.Driver = "http"
		}
		if provider.ID == "" {
			return nil, errors.New("every provider requires id")
		}
		if _, exists := compiled.Providers[provider.ID]; exists {
			return nil, fmt.Errorf("duplicate provider id %q", provider.ID)
		}
		if provider.RequestTimeout.Duration < 0 || provider.Cooldown.Duration < 0 {
			return nil, fmt.Errorf("provider %q: durations must not be negative", provider.ID)
		}
		if provider.ModelsURL != "" {
			if err := validateEndpoint(provider.ModelsURL); err != nil {
				return nil, fmt.Errorf("provider %q models_url: %w", provider.ID, err)
			}
		}

		desc, err := external.Describe(context.Background(), external.Config{
			ID:             provider.ID,
			Driver:         provider.Driver,
			Command:        provider.Command,
			Env:            provider.Env,
			ToolStrategy:   provider.ToolStrategy,
			ModelsURL:      provider.ModelsURL,
			HasSession:     provider.Session != nil,
			RequestTimeout: provider.RequestTimeout.Duration,
			DriverConfig:   provider.Config,
		})
		if err != nil {
			return nil, fmt.Errorf("provider %q: %w", provider.ID, err)
		}
		provider.capabilities = desc.Capabilities
		provider.ToolStrategy = desc.ToolStrategy
		provider.Config = desc.Config

		if provider.Capabilities().Stateful {
			if provider.Session == nil {
				provider.Session = &SessionSettings{}
			}
			settings := *provider.Session
			if settings.DefaultCWD == "" {
				var cwdErr error
				settings.DefaultCWD, cwdErr = os.Getwd()
				if cwdErr != nil {
					return nil, cwdErr
				}
			}
			var pathErr error
			settings.DefaultCWD, pathErr = filepath.Abs(settings.DefaultCWD)
			if pathErr != nil {
				return nil, fmt.Errorf("provider %q: resolve session.default_cwd: %w", provider.ID, pathErr)
			}
			settings.AllowedRoots = append([]string(nil), settings.AllowedRoots...)
			for i, root := range settings.AllowedRoots {
				if strings.TrimSpace(root) == "" {
					return nil, fmt.Errorf("provider %q: allowed roots must not be empty", provider.ID)
				}
				settings.AllowedRoots[i], pathErr = filepath.Abs(root)
				if pathErr != nil {
					return nil, fmt.Errorf("provider %q: resolve allowed root: %w", provider.ID, pathErr)
				}
			}
			if settings.TTL.Duration < 0 || settings.TurnTimeout.Duration < 0 {
				return nil, fmt.Errorf("provider %q: session durations must not be negative", provider.ID)
			}
			if settings.TTL.Duration == 0 {
				settings.TTL.Duration = 10 * time.Minute
			}
			if settings.TurnTimeout.Duration == 0 {
				settings.TurnTimeout.Duration = 30 * time.Minute
			}
			provider.Session = &settings
			if provider.RequestTimeout.Duration == 0 {
				provider.RequestTimeout.Duration = settings.TurnTimeout.Duration
			}
		} else {
			if provider.Session != nil {
				return nil, fmt.Errorf("provider %q: session requires a stateful driver", provider.ID)
			}
			if provider.RequestTimeout.Duration == 0 {
				provider.RequestTimeout.Duration = 60 * time.Second
			}
		}

		if provider.Cooldown.Duration == 0 {
			provider.Cooldown.Duration = 15 * time.Second
		}
		if provider.MaxConcurrent == 0 && provider.MaxConcurrentCamel != 0 {
			provider.MaxConcurrent = provider.MaxConcurrentCamel
		}
		if provider.MaxConcurrent <= 0 {
			provider.MaxConcurrent = 16
		}
		compiled.Providers[provider.ID] = provider
		if desc.Catalog != nil && desc.Catalog.URL != "" {
			compiled.CatalogSources[provider.ID] = appendCatalogSource(
				compiled.CatalogSources[provider.ID],
				CatalogSource{URL: desc.Catalog.URL, APIKey: desc.Catalog.APIKey, Explicit: desc.Catalog.Explicit},
			)
		}
	}

	result, err := compileRoutes(cfg.Rules, compiled.Providers)
	if err != nil {
		return nil, err
	}
	if len(result.models) == 0 {
		return nil, errors.New("at least one logical model with an entry route is required")
	}
	for _, route := range result.routes {
		if route.RouteTimeout <= 0 {
			continue
		}
		for _, target := range route.Pool {
			if p, ok := compiled.Providers[target.Provider]; ok && p.RequestTimeout.Duration < route.RouteTimeout {
				p.RequestTimeout.Duration = route.RouteTimeout
				compiled.Providers[target.Provider] = p
			}
		}
	}
	if err := validateSessionRoutes(result.routes, compiled.Providers); err != nil {
		return nil, err
	}
	compiled.Routes = result.routes
	compiled.Models = result.entries
	compiled.LogicalIDs = result.models
	return compiled, nil
}

func appendCatalogSource(sources []CatalogSource, candidate CatalogSource) []CatalogSource {
	for index, source := range sources {
		if source.URL != candidate.URL || source.APIKey != candidate.APIKey {
			continue
		}
		if candidate.Explicit && !source.Explicit {
			sources[index].Explicit = true
		}
		return sources
	}
	return append(sources, candidate)
}

func validateEndpoint(value string) error {
	parsed, err := url.Parse(value)
	if err != nil {
		return errors.New("invalid URL")
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("URL must use http or https and include a host")
	}
	if parsed.User != nil {
		return errors.New("URL must not contain credentials")
	}
	return nil
}

// validateMetricsBind validates and normalizes the metrics listener bind.
// The metrics endpoint is deliberately non-public: it defaults to a dedicated
// loopback interface (127.0.0.1:9209) distinct from the API port and must not
// share a port with the API listener.
func validateMetricsBind(_ string, port int) error {
	if port == 0 {
		port = 9209
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("metrics_port must be between 1 and 65535")
	}
	return nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// trimNonEmpty returns the input with each element TrimSpace'd and blanks
// removed. Used for lists that are config-optional in meaning (empty == not
// set) without confusing the executor with empty/whitespace entries.
func trimNonEmpty(values []string) []string {
	var out []string
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func parseErrorClasses(values []string) map[model.ErrorClass]bool {
	if len(values) == 0 {
		return allRetryableClasses()
	}
	classes := make(map[model.ErrorClass]bool, len(values))
	for _, value := range values {
		classes[model.ErrorClass(strings.ToLower(strings.TrimSpace(value)))] = true
	}
	return classes
}

func allRetryableClasses() map[model.ErrorClass]bool {
	return AllRetryableClasses()
}

func AllRetryableClasses() map[model.ErrorClass]bool {
	return map[model.ErrorClass]bool{
		model.ErrorTimeout: true, model.ErrorConnection: true, model.ErrorRateLimit: true,
		model.ErrorNotFound: true, model.ErrorUpstream: true, model.ErrorInvalid: true,
	}
}
