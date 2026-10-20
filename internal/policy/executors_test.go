package policy

import (
	"encoding/json"
	"strings"
	"testing"
)

func sessionConfig(t *testing.T, kind string) Config {
	t.Helper()
	raw := `{"providers":[{"id":"p","executor":"` + kind + `","command":["driver"],"session":{"default_cwd":"` + t.TempDir() + `"}}],"routing_rules":[{"route":"main","action":"filter","where":{"model":{"eq":"agent"}}},{"route":"main","action":"map","providers":["p"],"native":"default"},{"route":"main","action":"race","count":1}]}`
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}
func TestSessionExecutorsCompileWithoutHTTPSettings(t *testing.T) {
	for _, kind := range []string{"acp", "web"} {
		cfg := sessionConfig(t, kind)
		compiled, err := CompileConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !compiled.Models["agent"].Stateful || len(compiled.CatalogSources) != 0 || compiled.Providers["p"].Session.TurnTimeout.Duration <= 0 {
			t.Fatalf("bad compiled config: %+v", compiled)
		}
	}
}
func TestSessionExecutorRejectsInvalidSettings(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Provider)
	}{
		{"unknown", func(p *Provider) { p.Executor = "missing" }},
		{"command", func(p *Provider) { p.Command = nil }},
		{"url", func(p *Provider) { p.InferenceURL = "https://example.com" }},
		{"key", func(p *Provider) { p.APIKey = "secret" }},
		{"cwd", func(p *Provider) { p.Session.DefaultCWD = "relative" }},
		{"root", func(p *Provider) { p.Session.AllowedRoots = []string{"relative"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := sessionConfig(t, "acp")
			tc.change(&cfg.Providers[0])
			if _, err := CompileConfig(cfg); err == nil {
				t.Fatal("accepted invalid provider")
			}
		})
	}
}
func TestSessionRoutesRejectSpeculationAndReplay(t *testing.T) {
	for _, action := range []string{
		`{"route":"main","action":"retry","attempts":2}`,
		`{"route":"main","action":"hedge","after":"1s"}`,
		`{"route":"main","action":"continue"}`,
		`{"route":"main","action":"repetition"}`,
	} {
		cfg := sessionConfig(t, "acp")
		var rules RoutingRules
		if err := json.Unmarshal([]byte("["+action+"]"), &rules); err != nil {
			t.Fatal(err)
		}
		cfg.RoutingRules = append(cfg.RoutingRules, rules...)
		if _, err := CompileConfig(cfg); err == nil {
			t.Fatalf("accepted %s", action)
		}
	}
	cfg := sessionConfig(t, "acp")
	cfg.RoutingRules[len(cfg.RoutingRules)-1].(*RaceRule).Count = 0
	if _, err := CompileConfig(cfg); err == nil || !strings.Contains(err.Error(), "stateful") {
		t.Fatal("accepted implicit stateful race", err)
	}
}

func TestProvidersCannotShareCheckpointFile(t *testing.T) {
	cfg := sessionConfig(t, "acp")
	cfg.Providers[0].Session.StorePath = t.TempDir() + "/sessions.json"
	other := cfg.Providers[0]
	other.ID = "another-provider"
	settings := *other.Session
	other.Session = &settings
	cfg.Providers = append(cfg.Providers, other)
	if _, err := CompileConfig(cfg); err == nil || !strings.Contains(err.Error(), "store_path") {
		t.Fatal("accepted shared checkpoint file", err)
	}
}
