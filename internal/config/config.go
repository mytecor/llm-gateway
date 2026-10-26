package config

import (
	"log/slog"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
)

type Duration = model.Duration

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
	ClientAPIKeys          []ClientKey         `json:"client_api_keys"`
	CatalogRefreshInterval Duration            `json:"catalog_refresh_interval"`
	StreamIdleTimeout      Duration            `json:"stream_idle_timeout"`
	AffinityFile           string              `json:"affinity_file,omitempty"`
	SessionFile            string              `json:"session_file,omitempty"`
	Providers              []provider.Provider `json:"providers"`
	Rules                  policy.Rules        `json:"rules"`
}

// ClientKey is one named client credential. ID is the non-secret identity
// used as the metrics api_key dimension and to attribute requests (never the
// key material itself); Key is the bearer secret, either literal or an
// "env.VARNAME" reference resolved at load time.
type ClientKey struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

// CompiledConfig is the application configuration prepared for startup. It
// combines normalized settings and credentials with prepared providers and the
// routing graph compiled by policy.
type CompiledConfig struct {
	Raw        Config
	Logger     *slog.Logger
	Providers  map[string]provider.Provider
	Routes     map[string]*policy.CompiledRoute
	Models     map[string]*policy.CompiledRoute // logical model → entry route
	LogicalIDs []string
	// ClientKeys maps a client key ID (non-secret metrics label) to its
	// resolved bearer secret. Empty means client authentication is disabled.
	ClientKeys map[string]string
}
