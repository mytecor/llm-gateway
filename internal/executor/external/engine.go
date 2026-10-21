package external

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/mytecor/llm-gateway/internal/executor/acp"
	"github.com/mytecor/llm-gateway/internal/session"
)

const (
	ProtocolACP    = "acp"
	ProtocolDriver = "driver"
	ProtocolWeb    = "web"
	ProtocolNDJSON = "ndjson"

	ToolStrategyAuto   = "auto"
	ToolStrategyMCP    = "mcp"
	ToolStrategyPrompt = "prompt"
	ToolStrategyWire   = "wire"
	ToolStrategyBoth   = "both"
)

type Config struct {
	Protocol     string
	Command      []string
	DefaultCWD   string
	ToolStrategy string
}

func New(cfg Config) (session.Engine, error) {
	if len(cfg.Command) == 0 {
		return nil, errors.New("external executor command is required")
	}
	if _, err := exec.LookPath(cfg.Command[0]); err != nil {
		return nil, err
	}
	proto := strings.ToLower(strings.TrimSpace(cfg.Protocol))
	switch proto {
	case ProtocolACP:
		strategy := strings.ToLower(strings.TrimSpace(cfg.ToolStrategy))
		if strategy != "" && strategy != ToolStrategyAuto && strategy != ToolStrategyMCP {
			return nil, fmt.Errorf("ACP protocol supports only auto or mcp tool_strategy, got %q", cfg.ToolStrategy)
		}
		return acp.New(cfg.Command, cfg.DefaultCWD)
	case ProtocolDriver, ProtocolWeb, ProtocolNDJSON, "":
		return NewDriver(cfg)
	default:
		return nil, fmt.Errorf("unknown external executor protocol %q", cfg.Protocol)
	}
}
