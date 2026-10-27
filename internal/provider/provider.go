package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/mytecor/llm-gateway/internal/configcompat"
	"github.com/mytecor/llm-gateway/internal/executor/external"
	"github.com/mytecor/llm-gateway/internal/model"
	"log/slog"
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
	ID             string            `json:"id"`
	Driver         string            `json:"driver,omitempty"`
	Command        []string          `json:"command,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	Weight         int               `json:"weight,omitempty"`
	MaxConcurrent  int               `json:"max_concurrent,omitempty"`
	Cooldown       Duration          `json:"cooldown,omitempty"`
	RequestTimeout Duration          `json:"request_timeout,omitempty"`
	ModelsURL      string            `json:"models_url,omitempty"`
	ToolStrategy   string            `json:"tool_strategy,omitempty"`
	Session        *SessionSettings  `json:"session,omitempty"`
	Config         map[string]any    `json:"config,omitempty"`

	Description *external.Description `json:"-"`
}

var providerCoreKeys = map[string]bool{
	"id":              true,
	"driver":          true,
	"command":         true,
	"env":             true,
	"priority":        true,
	"weight":          true,
	"max_concurrent":  true,
	"cooldown":        true,
	"request_timeout": true,
	"models_url":      true,
	"tool_strategy":   true,
	"session":         true,
	"config":          true,
}

// Only these historical driver options are accepted outside config. New/custom
// options must live inside config, so misspelled gateway settings fail decoding.
var legacyDriverKeys = map[string]bool{
	"base_provider": true, "inference_url": true, "api_key": true,
	"vertex_project_id": true, "vertex_project_number": true,
	"vertex_region": true, "vertex_auth_credentials": true,
	"request_timeout_seconds": true, "bifrost_max_retries": true,
	"allow_private_network": true, "headers": true, "strip_params": true,
	"set_params": true, "profile": true, "cdp_url": true, "account": true,
	"state_dir": true, "cdp_header_env": true, "heal": true,
}

func (p *Provider) UnmarshalJSON(data []byte) error {
	data, err := configcompat.Aliases(data, map[string][]string{"max_concurrent": {"maxConcurrent"}})
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	var extras map[string]any
	if raw, ok := fields["config"]; ok {
		if err := json.Unmarshal(raw, &extras); err != nil {
			return fmt.Errorf("provider config must be a JSON object: %w", err)
		}
	}
	if extras == nil {
		extras = make(map[string]any)
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		raw := fields[key]
		if providerCoreKeys[key] {
			continue
		}
		if !legacyDriverKeys[key] {
			return fmt.Errorf("unknown provider field %q; driver options belong in config", key)
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		if existing, ok := extras[key]; ok && !reflect.DeepEqual(existing, value) {
			return fmt.Errorf("conflicting settings %q and %q", "config."+key, key)
		}
		extras[key] = value
		delete(fields, key)
	}
	// The alias avoids recursive UnmarshalJSON while retaining strict decoding
	// for gateway fields and nested session settings.
	type plain Provider
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	var result plain
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&result); err != nil {
		return err
	}
	if len(extras) > 0 {
		result.Config = extras
	} else {
		result.Config = nil
	}
	*p = Provider(result)
	return nil
}

func (p Provider) Capabilities() model.Capabilities {
	if p.Description != nil {
		return p.Description.Capabilities
	}
	return model.Capabilities{}
}

// DriverSettings is the single mapping from provider settings to driver startup.
func (p Provider) DriverSettings(logger *slog.Logger) external.Config {
	var cwd string
	if p.Session != nil {
		cwd = p.Session.DefaultCWD
	}
	return external.Config{ID: p.ID, Driver: p.Driver, Command: p.Command, Env: p.Env,
		DefaultCWD: cwd, ToolStrategy: p.ToolStrategy, ModelsURL: p.ModelsURL,
		HasSession: p.Session != nil, RequestTimeout: p.RequestTimeout.Duration,
		DriverConfig: p.Config, Logger: logger, Description: p.Description}
}
