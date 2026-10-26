package external

import (
	"context"
	"github.com/mytecor/llm-gateway/driverproto"
	"log/slog"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
)

const (
	ToolStrategyAuto   = "auto"
	ToolStrategyMCP    = "mcp"
	ToolStrategyPrompt = "prompt"
	ToolStrategyWire   = "wire"
	ToolStrategyBoth   = "both"
)

type CatalogSource = driverproto.CatalogSource

type Description struct {
	Capabilities model.Capabilities `json:"capabilities"`
	ToolStrategy string             `json:"tool_strategy,omitempty"`
	Catalog      *CatalogSource     `json:"catalog,omitempty"`
	Config       map[string]any     `json:"config,omitempty"`
}

type Config struct {
	Description    *Description
	ID             string
	Driver         string
	Command        []string
	Env            map[string]string
	DefaultCWD     string
	ToolStrategy   string
	ModelsURL      string
	HasSession     bool
	RequestTimeout time.Duration
	DriverConfig   map[string]any
	Logger         *slog.Logger
}

func New(ctx context.Context, cfg Config) (*Driver, error) {
	return newDriver(ctx, cfg)
}

func NewDriver(cfg Config) (*Driver, error) {
	return newDriver(context.Background(), cfg)
}
