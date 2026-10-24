package external

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

type httpProviderWire struct {
	ID                    string                     `json:"id"`
	BaseProvider          string                     `json:"base_provider"`
	InferenceURL          string                     `json:"inference_url"`
	VertexProjectID       string                     `json:"vertex_project_id,omitempty"`
	VertexProjectNumber   string                     `json:"vertex_project_number,omitempty"`
	VertexRegion          string                     `json:"vertex_region,omitempty"`
	VertexAuthCredentials string                     `json:"vertex_auth_credentials,omitempty"`
	APIKey                string                     `json:"api_key,omitempty"`
	RequestTimeoutSeconds int                        `json:"request_timeout_seconds,omitempty"`
	BifrostMaxRetries     int                        `json:"bifrost_max_retries,omitempty"`
	AllowPrivateNetwork   bool                       `json:"allow_private_network,omitempty"`
	Headers               map[string]string          `json:"headers,omitempty"`
	StripParams           []string                   `json:"strip_params,omitempty"`
	SetParams             map[string]json.RawMessage `json:"set_params,omitempty"`
}

type httpCallErrorWire struct {
	Code   string `json:"code,omitempty"`
	Class  string `json:"class"`
	Status int    `json:"status"`
	Scope  string `json:"scope,omitempty"`
	Replay string `json:"replay,omitempty"`
	Detail string `json:"detail,omitempty"`
	Log    string `json:"log,omitempty"`
}

type httpInboundMessage struct {
	Type                string             `json:"type"`
	ID                  string             `json:"id,omitempty"`
	Providers           []httpProviderWire `json:"providers,omitempty"`
	Provider            string             `json:"provider,omitempty"`
	Model               string             `json:"model,omitempty"`
	Kind                string             `json:"kind,omitempty"`
	Stream              bool               `json:"stream,omitempty"`
	StreamIdleTimeoutMs int64              `json:"stream_idle_timeout_ms,omitempty"`
	Body                json.RawMessage    `json:"body,omitempty"`
}

type httpOutboundMessage struct {
	Type       string             `json:"type"`
	ID         string             `json:"id,omitempty"`
	Data       json.RawMessage    `json:"data,omitempty"`
	Event      string             `json:"event,omitempty"`
	Meaningful bool               `json:"meaningful,omitempty"`
	Error      string             `json:"error,omitempty"`
	CallError  *httpCallErrorWire `json:"call_error,omitempty"`
}

// HTTPDriverExecutor manages the persistent external http-driver process over
// multiplexed NDJSON stdio.
type HTTPDriverExecutor struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	bw       *bufio.Writer
	writeMu  sync.Mutex
	closed   chan struct{}
	readDone chan struct{}
	waitDone chan struct{}
	once     sync.Once
	nextID   atomic.Uint64
	mu       sync.Mutex
	pending  map[string]chan httpOutboundMessage
	logger   *slog.Logger
}

func NewHTTP(ctx context.Context, config *policy.CompiledConfig) (*HTTPDriverExecutor, error) {
	binPath, err := ResolveDriverBinary(DriverHTTP)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binPath)
	cmd.Stderr = io.Discard

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}

	e := &HTTPDriverExecutor{
		cmd:      cmd,
		stdin:    stdin,
		bw:       bufio.NewWriter(stdin),
		closed:   make(chan struct{}),
		readDone: make(chan struct{}),
		waitDone: make(chan struct{}),
		pending:  make(map[string]chan httpOutboundMessage),
		logger:   config.Logger,
	}

	initCh := make(chan httpOutboundMessage, 1)
	go func() {
		defer close(e.readDone)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 16<<20)
		initialized := false
		for scanner.Scan() {
			var msg httpOutboundMessage
			if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
				msg = httpOutboundMessage{Type: "error", Error: "invalid HTTP driver event"}
			}
			if !initialized && msg.ID == "" && (msg.Type == "ready" || msg.Type == "error") {
				initialized = true
				initCh <- msg
				continue
			}
			if msg.ID != "" {
				e.mu.Lock()
				ch, ok := e.pending[msg.ID]
				e.mu.Unlock()
				if ok {
					select {
					case ch <- msg:
					case <-e.closed:
						return
					}
				}
			}
		}
	}()

	go func() {
		<-e.readDone
		_ = cmd.Wait()
		close(e.waitDone)
	}()

	ids := make([]string, 0, len(config.Providers))
	for id := range config.Providers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	wireProviders := make([]httpProviderWire, 0, len(ids))
	for _, id := range ids {
		p := config.Providers[id]
		wireProviders = append(wireProviders, httpProviderWire{
			ID:                    p.ID,
			BaseProvider:          p.BaseProvider,
			InferenceURL:          p.InferenceURL,
			VertexProjectID:       p.VertexProjectID,
			VertexProjectNumber:   p.VertexProjectNumber,
			VertexRegion:          p.VertexRegion,
			VertexAuthCredentials: p.VertexAuthCredentials,
			APIKey:                p.APIKey,
			RequestTimeoutSeconds: max(1, int(p.RequestTimeout.Duration/time.Second)),
			BifrostMaxRetries:     p.BifrostMaxRetries,
			AllowPrivateNetwork:   p.AllowPrivateNetwork,
			Headers:               p.Headers,
			StripParams:           p.StripParams,
			SetParams:             p.SetParams,
		})
	}

	if err := e.write(httpInboundMessage{Type: "start", Providers: wireProviders}); err != nil {
		_ = e.Close()
		return nil, err
	}

	select {
	case <-ctx.Done():
		_ = e.Close()
		return nil, ctx.Err()
	case msg := <-initCh:
		if msg.Type == "error" {
			_ = e.Close()
			return nil, errors.New(msg.Error)
		}
		if msg.Type != "ready" {
			_ = e.Close()
			return nil, errors.New("HTTP driver did not become ready")
		}
		return e, nil
	case <-e.readDone:
		_ = e.Close()
		return nil, errors.New("HTTP driver closed during startup")
	}
}

func (e *HTTPDriverExecutor) write(msg httpInboundMessage) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	select {
	case <-e.closed:
		return errors.New("HTTP driver closed")
	default:
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := e.bw.Write(append(data, '\n')); err != nil {
		return err
	}
	return e.bw.Flush()
}

func (e *HTTPDriverExecutor) register(id string, buf int) chan httpOutboundMessage {
	ch := make(chan httpOutboundMessage, buf)
	e.mu.Lock()
	e.pending[id] = ch
	e.mu.Unlock()
	return ch
}

func (e *HTTPDriverExecutor) unregister(id string) {
	e.mu.Lock()
	delete(e.pending, id)
	e.mu.Unlock()
}

func (e *HTTPDriverExecutor) Do(ctx context.Context, target model.Target, request model.ExecuteRequest) ([]byte, *model.CallError) {
	if request.Kind != model.RequestChat && request.Kind != model.RequestResponses {
		return nil, &model.CallError{Class: model.ErrorInvalid, Status: 400}
	}
	started := time.Now()
	id := fmt.Sprintf("req-%d", e.nextID.Add(1))
	ch := e.register(id, 4)
	defer e.unregister(id)

	err := e.write(httpInboundMessage{
		Type:     "request",
		ID:       id,
		Provider: target.Provider,
		Model:    target.Model,
		Kind:     string(request.Kind),
		Stream:   false,
		Body:     json.RawMessage(request.Body),
	})
	if err != nil {
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: err, Scope: model.FailureScopeProvider}
	}

	select {
	case <-ctx.Done():
		_ = e.write(httpInboundMessage{Type: "cancel", ID: id})
		callErr := classifyContextError(ctx)
		e.logResult(ctx, target, request.Kind, started, nil, callErr)
		return nil, callErr
	case msg := <-ch:
		if msg.Type == "error" {
			callErr := fromWireCallError(ctx, msg.CallError)
			e.logResult(ctx, target, request.Kind, started, msg.CallError, callErr)
			return nil, callErr
		}
		e.logUpstreamSuccess(ctx, target, request.Kind, started)
		return []byte(msg.Data), nil
	case <-e.readDone:
		callErr := &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: errors.New("HTTP driver closed"), Scope: model.FailureScopeProvider}
		e.logResult(ctx, target, request.Kind, started, nil, callErr)
		return nil, callErr
	}
}

func (e *HTTPDriverExecutor) Stream(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
	if request.Kind != model.RequestChat && request.Kind != model.RequestResponses {
		return nil, &model.CallError{Class: model.ErrorInvalid, Status: 400}
	}
	started := time.Now()
	id := fmt.Sprintf("req-%d", e.nextID.Add(1))
	ch := e.register(id, 32)

	err := e.write(httpInboundMessage{
		Type:                "request",
		ID:                  id,
		Provider:            target.Provider,
		Model:               target.Model,
		Kind:                string(request.Kind),
		Stream:              true,
		StreamIdleTimeoutMs: request.StreamIdleTimeout.Milliseconds(),
		Body:                json.RawMessage(request.Body),
	})
	if err != nil {
		e.unregister(id)
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: err, Scope: model.FailureScopeProvider}
	}

	select {
	case <-ctx.Done():
		_ = e.write(httpInboundMessage{Type: "cancel", ID: id})
		e.unregister(id)
		callErr := classifyContextError(ctx)
		e.logResult(ctx, target, request.Kind, started, nil, callErr)
		return nil, callErr
	case msg := <-ch:
		if msg.Type == "error" {
			e.unregister(id)
			callErr := fromWireCallError(ctx, msg.CallError)
			e.logResult(ctx, target, request.Kind, started, msg.CallError, callErr)
			return nil, callErr
		}
		if msg.Type != "stream_opened" {
			e.unregister(id)
			return nil, &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: errors.New("unexpected HTTP driver stream event"), Scope: model.FailureScopeProvider}
		}
	case <-e.readDone:
		e.unregister(id)
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: errors.New("HTTP driver closed"), Scope: model.FailureScopeProvider}
	}

	e.logUpstreamSuccess(ctx, target, request.Kind, started)
	output := make(chan model.StreamEvent, 8)
	go func() {
		defer close(output)
		defer e.unregister(id)
		for {
			select {
			case <-ctx.Done():
				_ = e.write(httpInboundMessage{Type: "cancel", ID: id})
				return
			case msg := <-ch:
				switch msg.Type {
				case "chunk":
					select {
					case output <- model.StreamEvent{Data: []byte(msg.Data), Event: msg.Event, Meaningful: msg.Meaningful}:
					case <-ctx.Done():
						_ = e.write(httpInboundMessage{Type: "cancel", ID: id})
						return
					}
				case "error":
					callErr := fromWireCallError(ctx, msg.CallError)
					e.logResult(ctx, target, request.Kind, started, msg.CallError, callErr)
					select {
					case output <- model.StreamEvent{Err: callErr}:
					case <-ctx.Done():
					}
					return
				case "done":
					return
				}
			case <-e.readDone:
				select {
				case output <- model.StreamEvent{Err: &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: errors.New("HTTP driver stream closed"), Scope: model.FailureScopeProvider}}:
				case <-ctx.Done():
				}
				return
			}
		}
	}()
	return output, nil
}

func classifyContextError(ctx context.Context) *model.CallError {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &model.CallError{Class: model.ErrorTimeout, Status: 504, Cause: ctx.Err()}
	}
	return &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
}

func fromWireCallError(ctx context.Context, w *httpCallErrorWire) *model.CallError {
	if ctx.Err() != nil {
		return classifyContextError(ctx)
	}
	if w == nil {
		return &model.CallError{Class: model.ErrorUpstream, Status: 502, Scope: model.FailureScopeProvider}
	}
	var cause error
	if w.Detail != "" {
		cause = errors.New(w.Detail)
	}
	return &model.CallError{
		Code:   w.Code,
		Class:  model.ErrorClass(w.Class),
		Status: w.Status,
		Cause:  cause,
		Scope:  model.FailureScope(w.Scope),
		Replay: model.ReplaySafety(w.Replay),
	}
}

func (e *HTTPDriverExecutor) logResult(ctx context.Context, target model.Target, kind model.RequestKind, started time.Time, wireErr *httpCallErrorWire, callErr *model.CallError) {
	if wireErr != nil && wireErr.Log == "provider_request_rejected" {
		attrs := []any{
			"provider", target.Provider,
			"native_model", target.Model,
			"kind", kind,
			"status_code", callErr.Status,
			"error_type", string(callErr.Class),
		}
		if callErr.Cause != nil {
			attrs = append(attrs, "detail", telemetry.SafeLogDetail(callErr.Cause.Error()))
		}
		telemetry.LogEvent(ctx, e.logger, slog.LevelWarn, "provider_request_rejected", attrs...)
		return
	}
	attrs := []any{
		"provider", target.Provider,
		"native_model", target.Model,
		"kind", kind,
		"status_code", callErr.Status,
		"error_type", string(callErr.Class),
		"duration_ms", time.Since(started).Milliseconds(),
	}
	if wireErr != nil && wireErr.Detail != "" {
		if detail := telemetry.SafeLogDetail(wireErr.Detail); detail != "" {
			attrs = append(attrs, "detail", detail)
		}
	}
	if callErr.Class == model.ErrorCancelled {
		telemetry.LogEvent(ctx, e.logger, slog.LevelDebug, "upstream_request_cancelled", attrs...)
		return
	}
	telemetry.LogEvent(ctx, e.logger, slog.LevelWarn, "upstream_request_failed", attrs...)
}

func (e *HTTPDriverExecutor) logUpstreamSuccess(ctx context.Context, target model.Target, kind model.RequestKind, started time.Time) {
	telemetry.LogEvent(ctx, e.logger, slog.LevelDebug, "upstream_request_accepted",
		"provider", target.Provider,
		"native_model", target.Model,
		"kind", kind,
		"status_code", 200,
		"duration_ms", time.Since(started).Milliseconds(),
	)
}

func (e *HTTPDriverExecutor) Close() error {
	e.once.Do(func() {
		close(e.closed)
		_ = e.stdin.Close()
		_ = e.cmd.Process.Signal(os.Interrupt)
		select {
		case <-e.waitDone:
			return
		case <-time.After(500 * time.Millisecond):
			_ = e.cmd.Process.Kill()
		}
	})
	select {
	case <-e.waitDone:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("HTTP driver did not stop")
	}
}
