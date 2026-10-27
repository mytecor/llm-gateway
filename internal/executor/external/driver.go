package external

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
)

func effectiveDriverConfig(cfg Config) map[string]any {
	out := make(map[string]any, len(cfg.DriverConfig)+1)
	for k, v := range cfg.DriverConfig {
		out[k] = v
	}
	if strings.TrimSpace(cfg.Driver) != "" && len(cfg.Command) > 0 {
		if _, exists := out["command"]; !exists {
			out["command"] = append([]string(nil), cfg.Command...)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneDescription(d Description) Description {
	out := d
	if d.Catalog != nil {
		cat := *d.Catalog
		out.Catalog = &cat
	}
	if d.Config != nil {
		cfgCopy := make(map[string]any, len(d.Config))
		for k, v := range d.Config {
			cfgCopy[k] = v
		}
		out.Config = cfgCopy
	}
	return out
}

// Driver keeps prepared provider settings and adapts either a stateless worker
// or independent native sessions to the gateway executor contracts.
type Driver struct {
	id             string
	command        []string
	env            map[string]string
	defaultCWD     string
	toolStrategy   string
	modelsURL      string
	requestTimeout time.Duration
	driverConfig   map[string]any
	capabilities   model.Capabilities
	description    Description
	logger         *slog.Logger
	modelLister    model.ModelLister

	workerMu       sync.Mutex
	worker         *statelessWorker
	changed        chan struct{}
	closed         chan struct{}
	supervisorDone chan struct{}
	stopSupervisor context.CancelFunc
	once           sync.Once
	closeErr       error
	nextID         atomic.Uint64
}

type DriverEngine = Driver

func newDriver(ctx context.Context, cfg Config) (*Driver, error) {
	if strings.TrimSpace(cfg.Driver) == "" && len(cfg.Command) == 0 {
		return nil, errors.New("driver or command is required")
	}
	execCmd, err := ResolveDriverCommand(cfg.Driver, cfg.Command)
	if err != nil {
		return nil, err
	}
	var desc Description
	if cfg.Description != nil {
		desc = cloneDescription(*cfg.Description)
	} else {
		desc, err = Describe(ctx, cfg)
		if err != nil {
			return nil, err
		}
	}
	d := &Driver{
		id:             strings.TrimSpace(cfg.ID),
		command:        execCmd,
		env:            cfg.Env,
		defaultCWD:     cfg.DefaultCWD,
		toolStrategy:   desc.ToolStrategy,
		modelsURL:      cfg.ModelsURL,
		requestTimeout: cfg.RequestTimeout,
		driverConfig:   desc.Config,
		capabilities:   desc.Capabilities,
		description:    desc,
		logger:         cfg.Logger,
	}
	if !desc.Capabilities.Stateful {
		if err := d.startStateless(ctx); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (d *Driver) Capabilities() model.Capabilities {
	return d.capabilities
}

func (d *Driver) Description() Description {
	return cloneDescription(d.description)
}

func (d *Driver) ID() string { return d.id }
