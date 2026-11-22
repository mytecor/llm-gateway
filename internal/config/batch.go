package config

import (
	"errors"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/mytecor/llm-gateway/internal/model"
)

var (
	batchBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	batchPrefix = regexp.MustCompile(`^[a-zA-Z0-9_-]+(?:/[a-zA-Z0-9_-]+)*$`)
	batchNative = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*(?:/[a-zA-Z0-9][a-zA-Z0-9._-]*)?$`)
)

func (c *CompiledConfig) validateBatch() error {
	b := c.Raw.Batch
	if b == nil {
		return nil
	}
	if _, ok := c.ClientKeys[b.Client]; !ok {
		return errors.New("batch.client must name a configured client API key")
	}
	if !batchPrefix.MatchString(b.Prefix) || len(b.Prefix) > 160 ||
		!filepath.IsAbs(b.StateDir) || filepath.Clean(b.StateDir) == "/" {
		return errors.New("batch requires a valid prefix and absolute state directory")
	}
	if (b.Bucket == "") != (b.StorageProvider == "") {
		return errors.New("batch storage provider and bucket must be configured together")
	}
	if b.Bucket != "" {
		p, ok := c.Providers[b.StorageProvider]
		if !ok || !p.Capabilities().Batch || p.Config["base_provider"] != "vertex" ||
			!batchBucket.MatchString(b.Bucket) || strings.Contains(b.Bucket, "..") {
			return errors.New("batch storage requires a Vertex provider and a valid bucket")
		}
	}
	if _, ok := b.Routes[b.Model]; !ok {
		return errors.New("batch.model must name a batch route")
	}
	for logical, bat := range b.Routes {
		p, ok := c.Providers[bat.Provider]
		if !ok || !p.Capabilities().Batch || !batchNative.MatchString(bat.Native) {
			return errors.New("batch route needs a batch-capable provider and a valid model")
		}
		switch bat.Backend {
		case "vertex":
			if bat.Provider != b.StorageProvider || strings.Contains(bat.Native, "/") {
				return errors.New("Vertex batch requires the configured Vertex storage provider and a publisher model ID")
			}
		case "openrouter":
			if p.Config["base_provider"] != "openai" || !strings.Contains(bat.Native, "/") {
				return errors.New("OpenRouter batch requires its HTTP provider and an author/model ID")
			}
			switch bat.ReasoningEffort {
			case "", "none", "minimal", "low", "medium", "high", "xhigh", "max":
			default:
				return errors.New("OpenRouter batch reasoning_effort must be one of none, minimal, low, medium, high, xhigh, max")
			}
		default:
			return errors.New("invalid batch backend")
		}
		route := c.Models[logical]
		if route == nil || !slices.ContainsFunc(route.Pool, func(target model.Target) bool {
			return target.Provider == bat.Provider && target.Model == bat.Native
		}) {
			return errors.New("batch model must include the configured provider and native model in its entry pool")
		}
	}
	return nil
}
