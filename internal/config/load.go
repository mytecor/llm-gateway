package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

func LoadConfig(path string) (Config, error) {
	cfg, err := DecodeConfig(path)
	if err != nil {
		return Config{}, err
	}
	if err := resolveConfigSecrets(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// DecodeConfig reads and strictly decodes the public configuration without
// resolving env.NAME secret references. Keeping this boundary separate lets
// `llm-gateway check` validate a generated config in a Nix build, where the
// runtime credentials deliberately do not exist.
func DecodeConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("decode config: trailing JSON content")
	}
	return cfg, nil
}
