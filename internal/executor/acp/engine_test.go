package acp

import (
	"context"
	"errors"
	"testing"

	"github.com/dmora/agentrun"
)

type catalogEngine struct {
	models []agentrun.ModelInfo
	err    error
	lists  int
}

func (e *catalogEngine) Validate() error { return nil }
func (e *catalogEngine) Start(context.Context, agentrun.Session, ...agentrun.Option) (agentrun.Process, error) {
	return nil, errors.New("not used")
}
func (e *catalogEngine) ListModels(context.Context, agentrun.Session) ([]agentrun.ModelInfo, error) {
	e.lists++
	return e.models, e.err
}
func TestEffortSelectionAndCatalogCache(t *testing.T) {
	source := &catalogEngine{models: []agentrun.ModelInfo{
		{ID: "model[low]", Name: "Model low"},
		{ID: "model[high]", Name: "Model high", Aliases: []string{"alias"}},
	}}
	engine := &Engine{engine: source}
	models, err := engine.Models(context.Background())
	if err != nil || len(models) != 1 || len(models[0].ReasoningEfforts) != 2 {
		t.Fatal(models, err)
	}
	for _, tc := range []struct{ model, effort, want string }{
		{"model", "high", "model[high]"}, {"model", "", "model[low]"}, {"default", "", ""},
	} {
		got, err := engine.Resolve(context.Background(), tc.model, tc.effort)
		if err != nil || got != tc.want {
			t.Fatal(got, err)
		}
	}
	if _, err := engine.Resolve(context.Background(), "model", "unknown"); err == nil {
		t.Fatal("accepted unknown effort")
	}
	if _, err := engine.Resolve(context.Background(), "default", "high"); err == nil {
		t.Fatal("accepted effort for backend default")
	}
	if !engine.acceptsModel("model[high]", "alias") || engine.acceptsModel("model[high]", "other") {
		t.Fatal("bad effective model validation")
	}
	if _, err := engine.Models(context.Background()); err != nil || source.lists != 1 {
		t.Fatal("catalog not cached", source.lists, err)
	}
	engine.catalogAt = engine.catalogAt.Add(-2 * catalogTTL)
	source.err = errors.New("temporarily unavailable")
	if models, err = engine.Models(context.Background()); err != nil || len(models) != 1 {
		t.Fatal("lost last known catalog", models, err)
	}
}
