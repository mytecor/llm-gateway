package provider

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/mytecor/llm-gateway/internal/model"
)

type Duration = model.Duration

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

func (p Provider) Capabilities() model.Capabilities {
	if p.capabilities != (model.Capabilities{}) {
		return p.capabilities
	}
	return model.Capabilities{Chat: true, Responses: true, Streaming: true, Tools: true, SafeReplay: true}
}
