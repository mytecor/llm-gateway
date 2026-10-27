package external

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/mytecor/llm-gateway/driverproto"
	"github.com/mytecor/llm-gateway/internal/model"
)

// Describe validates provider configuration in a short-lived driver process.
func Describe(ctx context.Context, cfg Config) (Description, error) {
	command, err := ResolveDriverCommand(cfg.Driver, cfg.Command)
	if err != nil {
		return Description{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	process, err := startProcess(command, cfg.Env, cfg.DefaultCWD)
	if err != nil {
		return Description{}, err
	}
	defer process.Close()
	id := strings.TrimSpace(cfg.ID)
	if id == "" {
		id = "default"
	}
	driverCfg := effectiveDriverConfig(cfg)
	event, err := process.exchange(ctx, wireInbound{
		Type: "init", ProtocolVersion: driverproto.Version, ID: id,
		ToolStrategy: cfg.ToolStrategy, ModelsURL: cfg.ModelsURL, HasSession: cfg.HasSession,
		RequestTimeoutMs: cfg.RequestTimeout.Milliseconds(), Config: driverCfg,
	}, true)
	if err != nil {
		return Description{}, err
	}
	if event.Type == "error" {
		return Description{}, errors.New(event.Error)
	}
	if err := driverproto.ValidateReady(event); err != nil {
		return Description{}, err
	}
	strategy := strings.ToLower(strings.TrimSpace(event.ToolStrategy))
	if event.Capabilities.Stateful {
		if strategy == "" {
			strategy = strings.ToLower(strings.TrimSpace(cfg.ToolStrategy))
			if strategy == "" || strategy == ToolStrategyAuto {
				strategy = ToolStrategyBoth
			}
		}
		if strategy != ToolStrategyMCP && strategy != ToolStrategyPrompt && strategy != ToolStrategyWire && strategy != ToolStrategyBoth {
			return Description{}, fmt.Errorf("unknown tool strategy %q", cfg.ToolStrategy)
		}
	}
	normalized := event.Config
	if normalized == nil {
		normalized = driverCfg
	}
	return Description{Capabilities: *event.Capabilities, ToolStrategy: strategy, Catalog: event.Catalog, Config: normalized}, nil
}

// Models performs fresh discovery; the gateway Catalog owns cached snapshots.
func (d *Driver) Models(ctx context.Context) ([]model.ModelInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	process, err := startProcess(d.command, d.env, d.defaultCWD)
	if err != nil {
		return nil, err
	}
	defer process.Close()
	event, err := process.exchange(ctx, wireInbound{
		Type: "models", ProtocolVersion: driverproto.Version, ID: d.id,
		ModelsURL: d.modelsURL, RequestTimeoutMs: d.requestTimeout.Milliseconds(),
		CWD: d.defaultCWD, Config: d.driverConfig,
	}, true)
	if err != nil {
		return nil, err
	}
	if event.Type == "error" {
		return nil, errors.New(event.Error)
	}
	if event.Type != "models" {
		return nil, fmt.Errorf("unexpected driver response %q to models request", event.Type)
	}
	return event.Models, nil
}

// SetModelLister installs the shared catalog before serving requests.
func (d *Driver) SetModelLister(lister model.ModelLister) { d.modelLister = lister }

func DiscoverModels(ctx context.Context, cfg Config) ([]model.ModelInfo, error) {
	command, err := ResolveDriverCommand(cfg.Driver, cfg.Command)
	if err != nil {
		return nil, err
	}
	d := &Driver{id: cfg.ID, command: command, env: cfg.Env, defaultCWD: cfg.DefaultCWD,
		modelsURL: cfg.ModelsURL, driverConfig: cfg.DriverConfig, requestTimeout: cfg.RequestTimeout}
	return d.Models(ctx)
}

// Resolve validates effort against the common catalog. Native model/effort
// encoding is driver-specific and must never leak into this transport.
func (d *Driver) Resolve(ctx context.Context, name, effort string) (string, error) {
	if name == "default" {
		name = ""
	}
	if effort == "" {
		return name, nil
	}
	if name == "" {
		return "", errors.New("select a concrete model to choose effort")
	}
	lister := d.modelLister
	if lister == nil {
		lister = d
	}
	models, err := lister.Models(ctx)
	if err != nil {
		return "", fmt.Errorf("reasoning effort discovery: %w", err)
	}
	for _, item := range models {
		if item.ID == name && slices.Contains(item.ReasoningEfforts, effort) {
			return name, nil
		}
	}
	return "", fmt.Errorf("reasoning effort %q is not available for model", effort)
}
