// Package executor builds native executors and dispatches provider instances.
package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/executor/external"
	sessionexecutor "github.com/mytecor/llm-gateway/internal/executor/session"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/session"
)

// Middleware has per-attempt preparation, semantic stream observation and a
// completion hook. Finish runs after the stream ends (or its context cancels),
// not when Stream returns a channel. Configure hooks before serving requests.
type Middleware struct {
	Prepare func(context.Context, model.Target, *model.ExecuteRequest) *model.CallError
	Event   func(context.Context, model.Target, model.ExecuteRequest, *model.StreamEvent)
	Finish  func(context.Context, model.Target, model.ExecuteRequest, *model.CallError)
}
type Binding struct {
	Executor     model.Executor
	Capabilities model.Capabilities
}
type Registry struct {
	bindings   map[string]Binding
	owned      []model.Executor
	drivers    []*external.Driver
	middleware []Middleware
	close      sync.Once
	closeErr   error
}

func NewRegistry(bindings map[string]Binding, middleware ...Middleware) *Registry {
	copy := make(map[string]Binding, len(bindings))
	for id, binding := range bindings {
		copy[id] = binding
	}
	return &Registry{bindings: copy, middleware: append([]Middleware(nil), middleware...)}
}
func New(ctx context.Context, cfg *config.CompiledConfig) (*Registry, error) {
	registry := NewRegistry(nil)
	fail := func(err error) (*Registry, error) { _ = registry.Close(); return nil, err }
	var sessionStore *session.Store
	for id, p := range cfg.Providers {
		if p.Description == nil {
			return fail(fmt.Errorf("provider %q: missing prepared driver description", id))
		}
		drv, err := external.New(ctx, p.DriverSettings(cfg.Logger))
		if err != nil {
			return fail(fmt.Errorf("provider %q: %w", id, err))
		}
		registry.drivers = append(registry.drivers, drv)
		caps := drv.Capabilities()
		if caps.Stateful {
			if sessionStore == nil {
				sessionStore, err = session.NewStore(cfg.Raw.SessionFile)
				if err != nil {
					_ = drv.Close()
					return fail(fmt.Errorf("session_file: %w", err))
				}
			}
			s := p.Session
			exec, err := sessionexecutor.New(drv, session.Config{
				Provider:          id,
				DefaultCWD:        s.DefaultCWD,
				AllowedRoots:      s.AllowedRoots,
				SessionTTL:        s.TTL.Duration,
				TurnTimeout:       s.TurnTimeout.Duration,
				StorePath:         cfg.Raw.SessionFile,
				Store:             sessionStore,
				HeartbeatInterval: s.Heartbeat.Duration,
			})
			if err != nil {
				_ = drv.Close()
				return fail(fmt.Errorf("provider %q: %w", id, err))
			}
			registry.owned = append(registry.owned, exec)
			registry.bindings[id] = Binding{Executor: exec, Capabilities: caps}
		} else {
			registry.owned = append(registry.owned, drv)
			registry.bindings[id] = Binding{Executor: drv, Capabilities: caps}
		}
	}
	return registry, nil
}
func (r *Registry) binding(target model.Target, req model.ExecuteRequest, stream bool) (Binding, *model.CallError) {
	binding, ok := r.bindings[target.Provider]
	if !ok || binding.Executor == nil {
		return Binding{}, &model.CallError{Class: model.ErrorInvalid, Status: 503, Scope: model.FailureScopeTarget, Replay: model.ReplayNotSent}
	}
	c := binding.Capabilities
	if (req.Kind != model.RequestChat && req.Kind != model.RequestResponses) || (req.Kind == model.RequestChat && !c.Chat) || (req.Kind == model.RequestResponses && !c.Responses) || (stream && !c.Streaming) {
		return Binding{}, &model.CallError{Class: model.ErrorInvalid, Status: 400, Scope: model.FailureScopeRequest, Replay: model.ReplayNotSent}
	}
	return binding, nil
}
func (r *Registry) prepare(ctx context.Context, target model.Target, request model.ExecuteRequest) (model.ExecuteRequest, *model.CallError) {
	request.Body = append([]byte(nil), request.Body...)
	for _, m := range r.middleware {
		if m.Prepare != nil {
			if err := m.Prepare(ctx, target, &request); err != nil {
				return request, err
			}
		}
	}
	return request, nil
}
func (r *Registry) finish(ctx context.Context, target model.Target, req model.ExecuteRequest, err *model.CallError) {
	for i := len(r.middleware) - 1; i >= 0; i-- {
		if m := r.middleware[i]; m.Finish != nil {
			m.Finish(ctx, target, req, err)
		}
	}
}
func (r *Registry) Do(ctx context.Context, target model.Target, request model.ExecuteRequest) ([]byte, *model.CallError) {
	binding, err := r.binding(target, request, false)
	if err != nil {
		return nil, err
	}
	request, err = r.prepare(ctx, target, request)
	if err != nil {
		r.finish(ctx, target, request, err)
		return nil, err
	}
	body, err := binding.Executor.Do(ctx, target, request)
	r.finish(ctx, target, request, err)
	return body, err
}
func (r *Registry) Stream(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
	binding, err := r.binding(target, request, true)
	if err != nil {
		return nil, err
	}
	request, err = r.prepare(ctx, target, request)
	if err != nil {
		r.finish(ctx, target, request, err)
		return nil, err
	}
	stream, err := binding.Executor.Stream(ctx, target, request)
	if err != nil {
		r.finish(ctx, target, request, err)
		return nil, err
	}
	out := make(chan model.StreamEvent, 1)
	go func() {
		var terminal *model.CallError
		defer close(out)
		defer func() { r.finish(ctx, target, request, terminal) }()
		for {
			select {
			case <-ctx.Done():
				terminal = &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
				return
			case event, ok := <-stream:
				if !ok {
					return
				}
				for _, m := range r.middleware {
					if m.Event != nil {
						m.Event(ctx, target, request, &event)
					}
				}
				if event.Err != nil {
					terminal = event.Err
				}
				select {
				case out <- event:
				case <-ctx.Done():
					terminal = &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
					return
				}
				if event.Err != nil || event.Done {
					return
				}
			}
		}
	}()
	return out, nil
}
func (r *Registry) Close() error {
	r.close.Do(func() {
		for i := len(r.owned) - 1; i >= 0; i-- {
			r.closeErr = errors.Join(r.closeErr, r.owned[i].Close())
		}
	})
	return r.closeErr
}

// UseCatalog installs one cached model view for every stateful resolver.
// Call before serving requests. Fresh discovery continues to use Driver.Models.
func (r *Registry) UseCatalog(catalog model.ProviderModelLister) {
	for _, d := range r.drivers {
		provider := d.ID()
		d.SetModelLister(model.ModelListFunc(func(ctx context.Context) ([]model.ModelInfo, error) { return catalog.Models(ctx, provider) }))
	}
}
