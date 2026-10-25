package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

func ParseAllowedEnv(value string) map[string]bool {
	allowed := make(map[string]bool)
	if value == "" {
		return allowed
	}
	for _, name := range strings.Split(value, ",") {
		allowed[name] = true
	}
	return allowed
}

func resolveConfigSecrets(cfg *Config) error {
	for i := range cfg.ClientAPIKeys {
		key, err := resolveSecret(cfg.ClientAPIKeys[i].Key)
		if err != nil {
			return fmt.Errorf("client_api_keys[%d] (%s): %w", i, cfg.ClientAPIKeys[i].ID, err)
		}
		cfg.ClientAPIKeys[i].Key = key
	}
	for i := range cfg.Providers {
		for k, v := range cfg.Providers[i].Env {
			resolved, err := resolveSecret(v)
			if err != nil {
				return fmt.Errorf("provider %q env[%s]: %w", cfg.Providers[i].ID, k, err)
			}
			cfg.Providers[i].Env[k] = resolved
		}
		if len(cfg.Providers[i].Config) > 0 {
			resolved, err := resolveValueSecrets(cfg.Providers[i].ID, "", cfg.Providers[i].Config)
			if err != nil {
				return err
			}
			cfg.Providers[i].Config = resolved.(map[string]any)
		}
	}
	return nil
}

func resolveValueSecrets(providerID, key string, val any) (any, error) {
	switch v := val.(type) {
	case string:
		res, err := resolveSecret(v)
		if err != nil {
			return nil, fmt.Errorf("provider %q %s: %w", providerID, key, err)
		}
		return res, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			res, err := resolveValueSecrets(providerID, k, item)
			if err != nil {
				return nil, err
			}
			out[k] = res
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			res, err := resolveValueSecrets(providerID, key, item)
			if err != nil {
				return nil, err
			}
			out[i] = res
		}
		return out, nil
	case []string:
		out := make([]string, len(v))
		for i, item := range v {
			res, err := resolveSecret(item)
			if err != nil {
				return nil, fmt.Errorf("provider %q %s: %w", providerID, key, err)
			}
			out[i] = res
		}
		return out, nil
	default:
		return val, nil
	}
}

func isSecretKey(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	return k == "key" ||
		strings.HasSuffix(k, "_key") ||
		strings.HasSuffix(k, "_secret") ||
		strings.HasSuffix(k, "_token") ||
		strings.HasSuffix(k, "_password") ||
		strings.HasSuffix(k, "_credentials")
}

// ValidateEnvSecretReferences enforces the NixOS public-config boundary:
// credentials may be absent, but every configured value must be an env.NAME
// reference with a portable environment-variable name. Runtime loading still
// resolves and requires the referenced value separately.
func ValidateEnvSecretReferences(cfg Config, allowed map[string]bool) error {
	referenced := make(map[string]bool)
	for i, clientKey := range cfg.ClientAPIKeys {
		name, err := validateEnvSecretReference(clientKey.Key)
		if err != nil {
			return fmt.Errorf("client_api_keys[%d] (%s): %w", i, clientKey.ID, err)
		}
		if name != "" {
			referenced[name] = true
		}
	}
	for _, provider := range cfg.Providers {
		for k, v := range provider.Env {
			if isSecretKey(k) || strings.HasPrefix(v, "env.") {
				name, err := validateEnvSecretReference(v)
				if err != nil {
					return fmt.Errorf("provider %q env[%s]: %w", provider.ID, k, err)
				}
				if name != "" {
					referenced[name] = true
				}
			}
		}
		if err := validateValueEnvSecrets(provider.ID, "", provider.Config, referenced); err != nil {
			return err
		}
	}
	for name := range referenced {
		if allowed != nil && !allowed[name] {
			return fmt.Errorf("environment variable %s is referenced but not declared", name)
		}
	}
	for name := range allowed {
		if !referenced[name] {
			return fmt.Errorf("environment variable %s is declared but not referenced", name)
		}
	}
	return nil
}

func validateValueEnvSecrets(providerID, key string, val any, referenced map[string]bool) error {
	switch v := val.(type) {
	case string:
		if isSecretKey(key) || strings.HasPrefix(v, "env.") {
			name, err := validateEnvSecretReference(v)
			if err != nil {
				return fmt.Errorf("provider %q %s: %w", providerID, key, err)
			}
			if name != "" {
				referenced[name] = true
			}
		}
		return nil
	case map[string]any:
		for k, item := range v {
			if err := validateValueEnvSecrets(providerID, k, item, referenced); err != nil {
				return err
			}
		}
		return nil
	case []any:
		for _, item := range v {
			if err := validateValueEnvSecrets(providerID, key, item, referenced); err != nil {
				return err
			}
		}
		return nil
	case []string:
		for _, item := range v {
			if isSecretKey(key) || strings.HasPrefix(item, "env.") {
				name, err := validateEnvSecretReference(item)
				if err != nil {
					return fmt.Errorf("provider %q %s: %w", providerID, key, err)
				}
				if name != "" {
					referenced[name] = true
				}
			}
		}
		return nil
	default:
		return nil
	}
}

func validateEnvSecretReference(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if !strings.HasPrefix(value, "env.") {
		return "", errors.New("literal secret is forbidden; use env.NAME")
	}
	name := strings.TrimPrefix(value, "env.")
	for index, char := range name {
		if (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || char == '_' || (index > 0 && char >= '0' && char <= '9') {
			continue
		}
		return "", fmt.Errorf("invalid environment variable name %q", name)
	}
	if name == "" {
		return "", errors.New("empty environment variable name")
	}
	return name, nil
}

func resolveSecret(value string) (string, error) {
	if !strings.HasPrefix(value, "env.") {
		return value, nil
	}
	name := strings.TrimPrefix(value, "env.")
	if name == "" {
		return "", errors.New("empty environment variable name")
	}
	secret, ok := os.LookupEnv(name)
	if !ok || secret == "" {
		return "", fmt.Errorf("environment variable %s is not set", name)
	}
	return secret, nil
}
