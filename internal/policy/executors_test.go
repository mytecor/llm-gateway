package policy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sessionConfig(t *testing.T, driver string) Config {
	t.Helper()
	raw := `{"providers":[{"id":"p","driver":"` + driver + `","command":["driver"],"session":{"default_cwd":"` + t.TempDir() + `"}}],"rules":[{"route":"main","action":"filter","where":{"model":{"eq":"agent"}}},{"route":"main","action":"map","providers":["p"],"native":"default"},{"route":"main","action":"race","count":1}]}`
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestSessionDriversCompileWithoutHTTPSettings(t *testing.T) {
	for _, driver := range []string{"acp", "web", "custom"} {
		cfg := sessionConfig(t, driver)
		compiled, err := CompileConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !compiled.Models["agent"].Stateful || len(compiled.CatalogSources) != 0 || compiled.Providers["p"].Session.TurnTimeout.Duration <= 0 {
			t.Fatalf("bad compiled config: %+v", compiled)
		}
		if !compiled.Providers["p"].Capabilities().Resume {
			t.Fatalf("expected driver %q to support resume", driver)
		}
	}
}

func TestWebDriverAcceptsDeclarativeProfileWithoutCommand(t *testing.T) {
	cfg := sessionConfig(t, "web")
	cfg.Providers[0].Command = nil
	cfg.Providers[0].Profile = "./drivers/web/sites/muse.yaml"
	cfg.Providers[0].CDPURL = "http://127.0.0.1:19222"
	cfg.Providers[0].Account = "main"
	compiled, err := CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(compiled.Providers["p"].Profile) {
		t.Fatalf("expected profile path to be resolved to absolute, got %q", compiled.Providers["p"].Profile)
	}
}

func TestDriverToolStrategies(t *testing.T) {
	// ACP driver with mcp tool strategy
	cfg := sessionConfig(t, "acp")
	cfg.Providers[0].ToolStrategy = "mcp"
	if _, err := CompileConfig(cfg); err != nil {
		t.Fatal(err)
	}

	// Custom driver with prompt tool strategy
	cfg = sessionConfig(t, "custom")
	cfg.Providers[0].ToolStrategy = "prompt"
	if _, err := CompileConfig(cfg); err != nil {
		t.Fatal(err)
	}

	// Web driver with wire tool strategy
	cfg = sessionConfig(t, "web")
	cfg.Providers[0].ToolStrategy = "wire"
	if _, err := CompileConfig(cfg); err != nil {
		t.Fatal(err)
	}

	// Reject unknown driver
	cfg = sessionConfig(t, "unknown")
	if _, err := CompileConfig(cfg); err == nil {
		t.Fatal("expected error on unknown driver")
	}

	// Reject unknown tool strategy
	cfg = sessionConfig(t, "custom")
	cfg.Providers[0].ToolStrategy = "magic"
	if _, err := CompileConfig(cfg); err == nil {
		t.Fatal("expected error on unknown tool strategy")
	}

	// Reject tool_strategy on http driver
	httpCfg := sessionConfig(t, "http")
	httpCfg.Providers[0].Command = nil
	httpCfg.Providers[0].Session = nil
	httpCfg.Providers[0].InferenceURL = "https://api.openai.com/v1"
	httpCfg.Providers[0].ToolStrategy = "mcp"
	if _, err := CompileConfig(httpCfg); err == nil {
		t.Fatal("expected error on tool_strategy with http driver")
	}
}

func TestACPToolStrategies(t *testing.T) {
	for _, strategy := range []string{"", "auto", "mcp", " AUTO ", " MCP ", "prompt", "wire", "both", " Prompt "} {
		t.Run(strategy, func(t *testing.T) {
			cfg := sessionConfig(t, "acp")
			cfg.Providers[0].ToolStrategy = strategy
			_, err := CompileConfig(cfg)
			switch strings.ToLower(strings.TrimSpace(strategy)) {
			case "", "auto", "mcp":
				if err != nil {
					t.Fatal(err)
				}
			default:
				if err == nil || !strings.Contains(err.Error(), "ACP driver supports only auto or mcp tool_strategy") {
					t.Fatalf("expected unsupported ACP tool strategy error, got %v", err)
				}
			}
		})
	}
}

func TestSessionDriverRejectsInvalidSettings(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Provider)
	}{
		{"unknown", func(p *Provider) { p.Driver = "missing" }},
		{"command", func(p *Provider) { p.Command = nil }},
		{"url", func(p *Provider) { p.InferenceURL = "https://example.com" }},
		{"key", func(p *Provider) { p.APIKey = "secret" }},
		{"root", func(p *Provider) { p.Session.AllowedRoots = []string{""} }},
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
		var rules Rules
		if err := json.Unmarshal([]byte("["+action+"]"), &rules); err != nil {
			t.Fatal(err)
		}
		cfg.Rules = append(cfg.Rules, rules...)
		if _, err := CompileConfig(cfg); err == nil {
			t.Fatalf("accepted %s", action)
		}
	}
	cfg := sessionConfig(t, "acp")
	cfg.Rules[len(cfg.Rules)-1].(*RaceRule).Count = 0
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

func TestSessionPathsResolveFromWorkingDirectory(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	cfg := sessionConfig(t, "custom")
	cfg.Providers[0].Session.DefaultCWD = "."
	cfg.Providers[0].Session.AllowedRoots = []string{".", "../other"}
	cfg.Providers[0].Session.StorePath = "state/sessions.json"
	compiled, err := CompileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	settings := compiled.Providers["p"].Session
	if settings.DefaultCWD != cwd || settings.AllowedRoots[0] != cwd || settings.AllowedRoots[1] != filepath.Join(cwd, "../other") || settings.StorePath != filepath.Join(cwd, "state/sessions.json") {
		t.Fatalf("unexpected resolved paths: %+v", settings)
	}
	if cfg.Providers[0].Session.AllowedRoots[0] != "." {
		t.Fatal("compilation mutated config roots")
	}
}
