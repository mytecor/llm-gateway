package policy

import (
	"errors"

	"github.com/mytecor/llm-gateway/internal/provider"
)

// CompiledPolicy is the validated routing graph consumed by the scheduler.
type CompiledPolicy struct {
	Routes     map[string]*CompiledRoute
	Models     map[string]*CompiledRoute
	LogicalIDs []string
}

// Compile builds a routing graph against prepared providers. It does not load
// files, resolve secrets, configure listeners or discover driver capabilities.
func Compile(rules Rules, providers map[string]provider.Provider) (*CompiledPolicy, error) {
	result, err := compileRoutes(rules, providers)
	if err != nil {
		return nil, err
	}
	if len(result.models) == 0 {
		return nil, errors.New("at least one logical model with an entry route is required")
	}
	if err := validateSessionRoutes(result.routes, providers); err != nil {
		return nil, err
	}
	return &CompiledPolicy{Routes: result.routes, Models: result.entries, LogicalIDs: result.models}, nil
}
