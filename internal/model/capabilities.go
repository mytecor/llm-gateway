package model

import (
	"context"
	"github.com/mytecor/llm-gateway/driverproto"
)

type Capabilities = driverproto.Capabilities
type ModelInfo = driverproto.ModelInfo

// ModelLister returns current native discovery metadata; caching belongs to Catalog.
type ModelLister interface {
	Models(context.Context) ([]ModelInfo, error)
}

// ModelListFunc adapts a transport operation or cached provider view.
type ModelListFunc func(context.Context) ([]ModelInfo, error)

func (f ModelListFunc) Models(ctx context.Context) ([]ModelInfo, error) { return f(ctx) }

type ProviderModelLister interface {
	Models(context.Context, string) ([]ModelInfo, error)
}
