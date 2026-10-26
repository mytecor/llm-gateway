// Package driverproto defines the versioned NDJSON contract shared by drivers.
// It depends only on the Go standard library and contains no runtime integration.
package driverproto

import (
	"encoding/json"
	"fmt"
)

const Version = 1

// CheckVersion accepts unversioned v0 peers for wire compatibility. Every peer
// must still advertise explicit capabilities in its initialization response.
func CheckVersion(version int) error {
	if version != 0 && version != Version {
		return fmt.Errorf("unsupported driver protocol version %d (want %d)", version, Version)
	}
	return nil
}

type CatalogSource struct {
	URL      string `json:"url"`
	APIKey   string `json:"api_key,omitempty"`
	Explicit bool   `json:"explicit,omitempty"`
}

type FunctionTool struct {
	Type     string             `json:"type"`
	Function FunctionDefinition `json:"function"`
}
type FunctionDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
	Strict      *bool           `json:"strict,omitempty"`
}

type Capabilities struct {
	Chat       bool `json:"chat,omitempty"`
	Responses  bool `json:"responses,omitempty"`
	Streaming  bool `json:"streaming,omitempty"`
	Tools      bool `json:"tools,omitempty"`
	Stateful   bool `json:"stateful,omitempty"`
	Fork       bool `json:"fork,omitempty"`
	Resume     bool `json:"resume,omitempty"`
	SafeReplay bool `json:"safe_replay,omitempty"`
}

type CallError struct {
	Code   string `json:"code,omitempty"`
	Class  string `json:"class"`
	Status int    `json:"status"`
	Scope  string `json:"scope,omitempty"`
	Replay string `json:"replay,omitempty"`
	Detail string `json:"detail,omitempty"`
	Log    string `json:"log,omitempty"`
}

type ModelInfo struct {
	ID               string   `json:"id"`
	Name             string   `json:"name,omitempty"`
	ReasoningEfforts []string `json:"reasoning_efforts,omitempty"`
}

type Command[C any] struct {
	ProtocolVersion     int               `json:"protocol_version,omitempty"`
	Command             []string          `json:"command,omitempty"`
	Providers           []json.RawMessage `json:"providers,omitempty"`
	Type                string            `json:"type"`
	ID                  string            `json:"id,omitempty"`
	ToolStrategy        string            `json:"tool_strategy,omitempty"`
	ModelsURL           string            `json:"models_url,omitempty"`
	HasSession          bool              `json:"has_session,omitempty"`
	RequestTimeoutMs    int64             `json:"request_timeout_ms,omitempty"`
	Config              C                 `json:"config,omitempty"`
	CWD                 string            `json:"cwd,omitempty"`
	Provider            string            `json:"provider,omitempty"`
	Model               string            `json:"model,omitempty"`
	ReasoningEffort     string            `json:"reasoning_effort,omitempty"`
	System              string            `json:"system,omitempty"`
	ResumeID            string            `json:"resume_id,omitempty"`
	Tools               []FunctionTool    `json:"tools,omitempty"`
	Text                string            `json:"text,omitempty"`
	Content             string            `json:"content,omitempty"`
	Error               string            `json:"error,omitempty"`
	Kind                string            `json:"kind,omitempty"`
	Stream              bool              `json:"stream,omitempty"`
	StreamIdleTimeoutMs int64             `json:"stream_idle_timeout_ms,omitempty"`
	Body                json.RawMessage   `json:"body,omitempty"`
}

type Event struct {
	ProtocolVersion int             `json:"protocol_version,omitempty"`
	Type            string          `json:"type"`
	ID              string          `json:"id,omitempty"`
	Text            string          `json:"text,omitempty"`
	Name            string          `json:"name,omitempty"`
	Arguments       json.RawMessage `json:"arguments,omitempty"`
	ResumeID        string          `json:"resume_id,omitempty"`
	Input           int64           `json:"input_tokens,omitempty"`
	Output          int64           `json:"output_tokens,omitempty"`
	Error           string          `json:"error,omitempty"`
	Models          []ModelInfo     `json:"models,omitempty"`
	ToolStrategy    string          `json:"tool_strategy,omitempty"`
	Capabilities    *Capabilities   `json:"capabilities,omitempty"`
	Catalog         *CatalogSource  `json:"catalog,omitempty"`
	Config          map[string]any  `json:"config,omitempty"`
	Data            json.RawMessage `json:"data,omitempty"`
	Event           string          `json:"event,omitempty"`
	Meaningful      bool            `json:"meaningful,omitempty"`
	CallError       *CallError      `json:"call_error,omitempty"`
}

// ValidateReady is shared by initial discovery and stateless runtime startup.
func ValidateReady(event Event) error {
	if event.Type != "ready" {
		return fmt.Errorf("unexpected driver initialization event %q", event.Type)
	}
	if err := CheckVersion(event.ProtocolVersion); err != nil {
		return err
	}
	if event.Capabilities == nil {
		return fmt.Errorf("driver initialization requires explicit capabilities")
	}
	return nil
}
