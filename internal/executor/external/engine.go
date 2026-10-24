package external

import (
	"github.com/mytecor/llm-gateway/internal/session"
)

const (
	DriverHTTP   = "http"
	DriverACP    = "acp"
	DriverWeb    = "web"
	DriverCustom = "custom"

	ToolStrategyAuto   = "auto"
	ToolStrategyMCP    = "mcp"
	ToolStrategyPrompt = "prompt"
	ToolStrategyWire   = "wire"
	ToolStrategyBoth   = "both"
)

type Config struct {
	Driver       string
	Command      []string
	DefaultCWD   string
	ToolStrategy string
	Profile      string
	CDPURL       string
	Account      string
	StateDir     string
	Heal         bool
}

func New(cfg Config) (session.Engine, error) {
	return NewDriver(cfg)
}
