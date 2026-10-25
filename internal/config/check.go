package config

import "fmt"

// CheckConfig validates a configuration file as requested by the "check" CLI command.
func CheckConfig(path string, requireEnvSecrets bool, allowedEnv string) error {
	cfg, err := DecodeConfig(path)
	if err != nil {
		return err
	}
	if requireEnvSecrets {
		if err := ValidateEnvSecretReferences(cfg, ParseAllowedEnv(allowedEnv)); err != nil {
			return fmt.Errorf("validate config: %w", err)
		}
	}
	if _, err := CompileConfig(cfg); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}
	return nil
}
