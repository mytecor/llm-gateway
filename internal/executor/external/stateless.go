package external

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mytecor/llm-gateway/driverproto"
	"github.com/mytecor/llm-gateway/internal/model"
)

const (
	workerRestartInitial = 100 * time.Millisecond
	workerRestartMax     = 5 * time.Second
	workerStartupTimeout = 15 * time.Second
	requestQueueSize     = 64
)

type pendingRequest struct {
	events chan wireEvent
	done   chan struct{}
	once   sync.Once
	err    *model.CallError // published by closing done
}

func (p *pendingRequest) fail(err *model.CallError) bool {
	changed := false
	p.once.Do(func() { p.err = err; close(p.done); changed = true })
	return changed
}

func (p *pendingRequest) next(ctx context.Context) (wireEvent, *model.CallError) {
	// Preserve events already read from the wire when EOF follows a final frame.
	select {
	case event := <-p.events:
		return event, nil
	default:
	}
	select {
	case event := <-p.events:
		return event, nil
	case <-ctx.Done():
		return wireEvent{}, classifyContextError(ctx)
	case <-p.done:
		select {
		case event := <-p.events:
			return event, nil
		default:
			return wireEvent{}, p.err
		}
	}
}

// Each generation owns its own pending map. The supervisor can replace a dead
// worker without ever resubmitting a request accepted by the old generation.
type statelessWorker struct {
	process *processTransport
	mu      sync.Mutex
	pending map[string]*pendingRequest
	cancels chan string
	done    chan struct{}
	started time.Time
}

func providerFailure(cause error) *model.CallError {
	return &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: cause,
		Scope: model.FailureScopeProvider, Replay: model.ReplayUnknown}
}

func (d *Driver) newWorker(ctx context.Context) (*statelessWorker, error) {
	ctx, cancel := context.WithTimeout(ctx, workerStartupTimeout)
	defer cancel()
	process, err := startProcess(d.command, d.env, d.defaultCWD)
	if err != nil {
		return nil, err
	}
	id := d.id
	if id == "" {
		id = "default"
	}
	event, err := process.exchange(ctx, wireInbound{
		Type: "start", ProtocolVersion: driverproto.Version, ID: id,
		ModelsURL: d.modelsURL, RequestTimeoutMs: d.requestTimeout.Milliseconds(), Config: d.driverConfig,
	}, false)
	if err == nil && event.Type == "error" {
		err = errors.New(event.Error)
	}
	if err == nil {
		err = driverproto.ValidateReady(event)
	}
	if err == nil && *event.Capabilities != d.capabilities {
		err = errors.New("driver capabilities changed after preparation")
	}
	if err != nil {
		_ = process.Close()
		return nil, err
	}
	w := &statelessWorker{process: process, pending: make(map[string]*pendingRequest),
		cancels: make(chan string, requestQueueSize), done: make(chan struct{}), started: time.Now()}
	go w.dispatch()
	go w.cancelLoop()
	return w, nil
}

func (w *statelessWorker) dispatch() {
	defer close(w.done)
	defer func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		for id, request := range w.pending {
			request.fail(providerFailure(errors.New("external driver closed before completion")))
			delete(w.pending, id)
		}
	}()
	for event := range w.process.events {
		if event.ID == "" {
			// Global protocol errors invalidate all calls on this generation.
			if event.Type == "error" {
				w.process.abort()
				return
			}
			continue
		}
		w.mu.Lock()
		request := w.pending[event.ID]
		if request == nil {
			w.mu.Unlock()
			continue
		}
		select {
		case request.events <- event:
			if event.Type == "response" || event.Type == "done" || event.Type == "error" {
				delete(w.pending, event.ID)
			}
			w.mu.Unlock()
		default:
			delete(w.pending, event.ID)
			w.mu.Unlock()
			failure := &model.CallError{Class: model.ErrorUpstream, Status: 502,
				Cause: errors.New("external driver stream consumer is too slow"), Scope: model.FailureScopeRequest, Replay: model.ReplayUnknown}
			if request.fail(failure) {
				w.queueCancel(event.ID)
			}
		}
	}
}

func (w *statelessWorker) queueCancel(id string) {
	select {
	case <-w.process.closed:
		return
	case <-w.done:
		return
	case w.cancels <- id:
	default:
		// A driver unable to keep up with bounded cancellation traffic must be
		// replaced; the common stdout reader must never block on control writes.
		w.process.abort()
	}
}

func (w *statelessWorker) cancelLoop() {
	for {
		select {
		case <-w.done:
			return
		case <-w.process.closed:
			return
		case id := <-w.cancels:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = w.process.write(ctx, wireInbound{Type: "cancel", ID: id})
			cancel()
		}
	}
}

func (w *statelessWorker) unregister(id string) {
	w.mu.Lock()
	delete(w.pending, id)
	w.mu.Unlock()
}

func (w *statelessWorker) cancel(id string, request *pendingRequest, err *model.CallError) {
	w.unregister(id)
	if request.fail(err) {
		w.queueCancel(id)
	}
}

func (d *Driver) startStateless(ctx context.Context) error {
	w, err := d.newWorker(ctx)
	if err != nil {
		return err
	}
	d.worker = w
	d.changed = make(chan struct{})
	d.closed = make(chan struct{})
	d.supervisorDone = make(chan struct{})
	supervisorCtx, cancel := context.WithCancel(context.Background())
	d.stopSupervisor = cancel
	go d.supervise(supervisorCtx, w)
	return nil
}

func (d *Driver) supervise(ctx context.Context, worker *statelessWorker) {
	defer close(d.supervisorDone)
	defer func() {
		if worker != nil {
			_ = worker.process.Close()
		}
	}()
	backoff := workerRestartInitial
	for {
		select {
		case <-ctx.Done():
			return
		case <-worker.done:
		}
		if time.Since(worker.started) >= 30*time.Second {
			backoff = workerRestartInitial
		}
		d.workerMu.Lock()
		d.worker = nil
		close(d.changed)
		d.changed = make(chan struct{})
		d.workerMu.Unlock()
		_ = worker.process.Close()
		worker = nil
		for worker == nil {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			var err error
			worker, err = d.newWorker(ctx)
			backoff = min(2*backoff, workerRestartMax)
			if err != nil {
				continue
			}
			d.workerMu.Lock()
			d.worker = worker
			close(d.changed)
			d.changed = make(chan struct{})
			d.workerMu.Unlock()
		}
	}
}

func (d *Driver) currentWorker(ctx context.Context) (*statelessWorker, *model.CallError) {
	for {
		if ctx.Err() != nil {
			return nil, classifyContextError(ctx)
		}
		select {
		case <-d.closed:
			return nil, providerFailure(errors.New("external driver closed"))
		default:
		}
		d.workerMu.Lock()
		worker, changed := d.worker, d.changed
		d.workerMu.Unlock()
		if worker != nil {
			select {
			case <-worker.done:
				// The supervisor has not yet published the next generation.
				select {
				case <-ctx.Done():
					return nil, classifyContextError(ctx)
				case <-d.closed:
					return nil, providerFailure(errors.New("external driver closed"))
				case <-changed:
				}
			default:
				return worker, nil
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil, classifyContextError(ctx)
		case <-d.closed:
			return nil, providerFailure(errors.New("external driver closed"))
		case <-changed:
		}
	}
}

func (d *Driver) submit(ctx context.Context, target model.Target, request model.ExecuteRequest, stream bool) (*statelessWorker, string, *pendingRequest, *model.CallError) {
	if request.Kind != model.RequestChat && request.Kind != model.RequestResponses {
		return nil, "", nil, &model.CallError{Class: model.ErrorInvalid, Status: 400, Replay: model.ReplayNotSent}
	}
	worker, err := d.currentWorker(ctx)
	if err != nil {
		return nil, "", nil, err
	}
	id := fmt.Sprintf("req-%d", d.nextID.Add(1))
	pending := &pendingRequest{events: make(chan wireEvent, requestQueueSize), done: make(chan struct{})}
	worker.mu.Lock()
	worker.pending[id] = pending
	worker.mu.Unlock()
	select {
	case <-worker.done:
		worker.unregister(id)
		return nil, "", nil, providerFailure(errors.New("external driver closed before request"))
	default:
	}
	if writeErr := worker.process.write(ctx, wireInbound{
		Type: "request", ID: id, Provider: target.Provider, Model: target.Model,
		Kind: string(request.Kind), Stream: stream, Body: json.RawMessage(request.Body),
		StreamIdleTimeoutMs: request.StreamIdleTimeout.Milliseconds(),
	}); writeErr != nil {
		worker.unregister(id)
		if ctx.Err() != nil {
			return nil, "", nil, classifyContextError(ctx)
		}
		return nil, "", nil, providerFailure(writeErr)
	}
	return worker, id, pending, nil
}

func (d *Driver) Do(ctx context.Context, target model.Target, request model.ExecuteRequest) ([]byte, *model.CallError) {
	started := time.Now()
	worker, id, pending, err := d.submit(ctx, target, request, false)
	if err != nil {
		return nil, err
	}
	defer worker.unregister(id)
	event, err := pending.next(ctx)
	if err != nil {
		worker.cancel(id, pending, err)
		d.logResult(ctx, target, request.Kind, started, nil, err)
		return nil, err
	}
	if event.Type == "error" {
		err = fromWireCallError(ctx, event.CallError, event.Error)
		d.logResult(ctx, target, request.Kind, started, event.CallError, err)
		return nil, err
	}
	if event.Type != "response" {
		return nil, providerFailure(errors.New("unexpected external driver response event"))
	}
	d.logUpstreamSuccess(ctx, target, request.Kind, started)
	return event.Data, nil
}

func (d *Driver) Stream(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
	started := time.Now()
	worker, id, pending, err := d.submit(ctx, target, request, true)
	if err != nil {
		return nil, err
	}
	event, err := pending.next(ctx)
	if err != nil {
		worker.cancel(id, pending, err)
		return nil, err
	}
	if event.Type == "error" {
		worker.unregister(id)
		return nil, fromWireCallError(ctx, event.CallError, event.Error)
	}
	if event.Type != "stream_opened" {
		err = providerFailure(errors.New("unexpected external driver stream event"))
		worker.cancel(id, pending, err)
		return nil, err
	}
	d.logUpstreamSuccess(ctx, target, request.Kind, started)
	out := make(chan model.StreamEvent, 8)
	go func() {
		defer close(out)
		defer worker.unregister(id)
		terminal := func(err *model.CallError) {
			d.logResult(ctx, target, request.Kind, started, nil, err)
			select {
			case out <- model.StreamEvent{Err: err}:
			case <-ctx.Done():
			case <-d.closed:
			}
		}
		for {
			event, err := pending.next(ctx)
			if err != nil {
				worker.cancel(id, pending, err)
				terminal(err)
				return
			}
			switch event.Type {
			case "done":
				return
			case "error":
				terminal(fromWireCallError(ctx, event.CallError, event.Error))
				return
			case "chunk":
				select {
				case out <- model.StreamEvent{Data: event.Data, Event: event.Event, Meaningful: event.Meaningful}:
				case <-ctx.Done():
					worker.cancel(id, pending, classifyContextError(ctx))
					return
				case <-pending.done:
					terminal(pending.err)
					return
				case <-d.closed:
					return
				}
			default:
				err := providerFailure(errors.New("unexpected external driver stream event"))
				worker.cancel(id, pending, err)
				terminal(err)
				return
			}
		}
	}()
	return out, nil
}

func (d *Driver) Close() error {
	if d.supervisorDone == nil {
		return nil
	}
	d.once.Do(func() {
		close(d.closed)
		d.stopSupervisor()
		d.workerMu.Lock()
		worker := d.worker
		d.workerMu.Unlock()
		if worker != nil {
			d.closeErr = worker.process.Close()
		}
		<-d.supervisorDone
	})
	return d.closeErr
}
