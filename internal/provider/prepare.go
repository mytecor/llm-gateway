package provider

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mytecor/llm-gateway/internal/executor/external"
)

// Prepare normalizes provider settings, discovers driver capabilities and
// validates driver-specific configuration. It does not compile routing rules.
func Prepare(ctx context.Context, provider Provider) (Provider, error) {
	provider.ID = strings.TrimSpace(provider.ID)
	provider.ModelsURL = strings.TrimSpace(provider.ModelsURL)
	provider.Driver = strings.ToLower(strings.TrimSpace(provider.Driver))
	if provider.Driver == "" && len(provider.Command) == 0 {
		provider.Driver = "http"
	}
	if provider.ID == "" {
		return Provider{}, errors.New("every provider requires id")
	}
	if provider.RequestTimeout.Duration < 0 || provider.Cooldown.Duration < 0 {
		return Provider{}, fmt.Errorf("provider %q: durations must not be negative", provider.ID)
	}
	if provider.ModelsURL != "" {
		if err := validateEndpoint(provider.ModelsURL); err != nil {
			return Provider{}, fmt.Errorf("provider %q models_url: %w", provider.ID, err)
		}
	}

	desc, err := external.Describe(ctx, provider.DriverSettings(nil))
	if err != nil {
		return Provider{}, fmt.Errorf("provider %q: %w", provider.ID, err)
	}
	provider.Description = &desc
	provider.ToolStrategy = desc.ToolStrategy
	provider.Config = desc.Config

	if provider.Capabilities().Stateful {
		if provider.Session == nil {
			provider.Session = &SessionSettings{}
		}
		settings := *provider.Session
		if settings.DefaultCWD == "" {
			var cwdErr error
			settings.DefaultCWD, cwdErr = os.Getwd()
			if cwdErr != nil {
				return Provider{}, cwdErr
			}
		}
		var pathErr error
		settings.DefaultCWD, pathErr = filepath.Abs(settings.DefaultCWD)
		if pathErr != nil {
			return Provider{}, fmt.Errorf("provider %q: resolve session.default_cwd: %w", provider.ID, pathErr)
		}
		settings.AllowedRoots = append([]string(nil), settings.AllowedRoots...)
		for i, root := range settings.AllowedRoots {
			if strings.TrimSpace(root) == "" {
				return Provider{}, fmt.Errorf("provider %q: allowed roots must not be empty", provider.ID)
			}
			settings.AllowedRoots[i], pathErr = filepath.Abs(root)
			if pathErr != nil {
				return Provider{}, fmt.Errorf("provider %q: resolve allowed root: %w", provider.ID, pathErr)
			}
		}
		if settings.TTL.Duration < 0 || settings.TurnTimeout.Duration < 0 {
			return Provider{}, fmt.Errorf("provider %q: session durations must not be negative", provider.ID)
		}
		if settings.TTL.Duration == 0 {
			settings.TTL.Duration = 10 * time.Minute
		}
		if settings.TurnTimeout.Duration == 0 {
			settings.TurnTimeout.Duration = 30 * time.Minute
		}
		provider.Session = &settings
		if provider.RequestTimeout.Duration == 0 {
			provider.RequestTimeout.Duration = settings.TurnTimeout.Duration
		}
	} else {
		if provider.Session != nil {
			return Provider{}, fmt.Errorf("provider %q: session requires a stateful driver", provider.ID)
		}
		if provider.RequestTimeout.Duration == 0 {
			provider.RequestTimeout.Duration = 60 * time.Second
		}
	}

	if provider.Cooldown.Duration == 0 {
		provider.Cooldown.Duration = 15 * time.Second
	}

	if provider.MaxConcurrent <= 0 {
		provider.MaxConcurrent = 16
	}
	return provider, nil
}

func validateEndpoint(value string) error {
	parsed, err := url.Parse(value)
	if err != nil {
		return errors.New("invalid URL")
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("URL must use http or https and include a host")
	}
	if parsed.User != nil {
		return errors.New("URL must not contain credentials")
	}
	return nil
}
