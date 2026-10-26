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
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mytecor/llm-gateway/driverproto"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/session"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

type wireCapabilities = driverproto.Capabilities
type wireCallError = driverproto.CallError
type wireModelInfo = driverproto.ModelInfo
type wireInbound = driverproto.Command[map[string]any]
type wireEvent = driverproto.Event

func effectiveDriverConfig(cfg Config) map[string]any {
	out := make(map[string]any, len(cfg.DriverConfig)+1)
	for k, v := range cfg.DriverConfig {
		out[k] = v
	}
	if strings.TrimSpace(cfg.Driver) != "" && len(cfg.Command) > 0 {
		if _, exists := out["command"]; !exists {
			out["command"] = append([]string(nil), cfg.Command...)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func appendEnv(base []string, extra map[string]string) []string {
	if len(extra) == 0 {
		return base
	}
	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := append([]string(nil), base...)
	for _, k := range keys {
		out = append(out, k+"="+extra[k])
	}
	return out
}

// Describe spawns a short-lived driver process with {"type":"init",...} to
// validate driver-specific configuration and discover its capabilities.
func Describe(ctx context.Context, cfg Config) (Description, error) {
	execCmd, err := ResolveDriverCommand(cfg.Driver, cfg.Command)
	if err != nil {
		return Description{}, err
	}
	driverCfg := effectiveDriverConfig(cfg)
	id := strings.TrimSpace(cfg.ID)
	if id == "" {
		id = "default"
	}
	initMsg := wireInbound{
		Type:             "init",
		ProtocolVersion:  driverproto.Version,
		ID:               id,
		ToolStrategy:     cfg.ToolStrategy,
		ModelsURL:        cfg.ModelsURL,
		HasSession:       cfg.HasSession,
		RequestTimeoutMs: cfg.RequestTimeout.Milliseconds(),
		Config:           driverCfg,
	}
	initData, err := json.Marshal(initMsg)
	if err != nil {
		return Description{}, err
	}

	initCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(initCtx, execCmd[0], execCmd[1:]...)
	if len(cfg.Env) > 0 {
		cmd.Env = appendEnv(os.Environ(), cfg.Env)
	}
	cmd.Stderr = io.Discard
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return Description{}, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return Description{}, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return Description{}, err
	}
	defer func() {
		_ = stdin.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	if _, err := stdin.Write(append(initData, '\n')); err != nil {
		return Description{}, err
	}
	_ = stdin.Close()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return Description{}, err
		}
		return Description{}, errors.New("external driver closed during initialization")
	}

	var ev wireEvent
	if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
		return Description{}, fmt.Errorf("invalid driver initialization event: %w", err)
	}
	if ev.Type == "error" {
		return Description{}, errors.New(ev.Error)
	}
	if err := driverproto.ValidateReady(ev); err != nil {
		return Description{}, err
	}
	caps := *ev.Capabilities

	strategy := strings.ToLower(strings.TrimSpace(ev.ToolStrategy))
	if caps.Stateful {
		if strategy == "" {
			strategy = strings.ToLower(strings.TrimSpace(cfg.ToolStrategy))
			if strategy == "" || strategy == ToolStrategyAuto {
				strategy = ToolStrategyBoth
			}
		}
		if strategy != ToolStrategyMCP && strategy != ToolStrategyPrompt && strategy != ToolStrategyWire && strategy != ToolStrategyBoth {
			return Description{}, fmt.Errorf("unknown tool strategy %q", cfg.ToolStrategy)
		}
	}

	normalizedCfg := ev.Config
	if normalizedCfg == nil {
		normalizedCfg = driverCfg
	}

	desc := Description{
		Capabilities: caps,
		ToolStrategy: strategy,
		Catalog:      ev.Catalog,
		Config:       normalizedCfg,
	}

	return desc, nil
}

func cloneDescription(d Description) Description {
	out := d
	if d.Catalog != nil {
		cat := *d.Catalog
		out.Catalog = &cat
	}
	if d.Config != nil {
		cfgCopy := make(map[string]any, len(d.Config))
		for k, v := range d.Config {
			cfgCopy[k] = v
		}
		out.Config = cfgCopy
	}
	return out
}

// Driver is the unified external driver client over the gateway NDJSON wire
// protocol. It implements model.Executor for stateless (multiplexed) drivers
// and session.Engine for stateful (per-session) drivers.
type Driver struct {
	id             string
	command        []string
	env            map[string]string
	defaultCWD     string
	toolStrategy   string
	modelsURL      string
	requestTimeout time.Duration
	driverConfig   map[string]any
	capabilities   model.Capabilities
	description    Description
	logger         *slog.Logger

	mu          sync.Mutex
	modelLister model.ModelLister

	// Stateless multiplexed worker state
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	bw       *bufio.Writer
	writeMu  sync.Mutex
	closed   chan struct{}
	readDone chan struct{}
	waitDone chan struct{}
	once     sync.Once
	nextID   atomic.Uint64
	pendMu   sync.Mutex
	pending  map[string]chan wireEvent
}

type DriverEngine = Driver

func newDriver(ctx context.Context, cfg Config) (*Driver, error) {
	if strings.TrimSpace(cfg.Driver) == "" && len(cfg.Command) == 0 {
		return nil, errors.New("driver or command is required")
	}
	execCmd, err := ResolveDriverCommand(cfg.Driver, cfg.Command)
	if err != nil {
		return nil, err
	}
	var desc Description
	if cfg.Description != nil {
		desc = cloneDescription(*cfg.Description)
	} else {
		desc, err = Describe(ctx, cfg)
		if err != nil {
			return nil, err
		}
	}
	d := &Driver{
		id:             strings.TrimSpace(cfg.ID),
		command:        execCmd,
		env:            cfg.Env,
		defaultCWD:     cfg.DefaultCWD,
		toolStrategy:   desc.ToolStrategy,
		modelsURL:      cfg.ModelsURL,
		requestTimeout: cfg.RequestTimeout,
		driverConfig:   desc.Config,
		capabilities:   desc.Capabilities,
		description:    desc,
		logger:         cfg.Logger,
	}
	if !desc.Capabilities.Stateful {
		if err := d.startMultiplexed(ctx); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (d *Driver) Capabilities() model.Capabilities {
	return d.capabilities
}

func (d *Driver) Description() Description {
	return cloneDescription(d.description)
}

func (d *Driver) startMultiplexed(ctx context.Context) error {
	cmd := exec.Command(d.command[0], d.command[1:]...)
	if len(d.env) > 0 {
		cmd.Env = appendEnv(os.Environ(), d.env)
	}
	cmd.Stderr = io.Discard

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return err
	}

	d.cmd = cmd
	d.stdin = stdin
	d.bw = bufio.NewWriter(stdin)
	d.closed = make(chan struct{})
	d.readDone = make(chan struct{})
	d.waitDone = make(chan struct{})
	d.pending = make(map[string]chan wireEvent)

	initCh := make(chan wireEvent, 1)
	go func() {
		defer close(d.readDone)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 16<<20)
		initialized := false
		for scanner.Scan() {
			var msg wireEvent
			if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
				msg = wireEvent{Type: "error", Error: "invalid external driver event"}
			}
			if !initialized && msg.ID == "" && (msg.Type == "ready" || msg.Type == "error") {
				initialized = true
				initCh <- msg
				continue
			}
			if msg.ID != "" {
				d.pendMu.Lock()
				ch, ok := d.pending[msg.ID]
				d.pendMu.Unlock()
				if ok {
					select {
					case ch <- msg:
					case <-d.closed:
						return
					}
				}
			}
		}
	}()

	go func() {
		<-d.readDone
		_ = cmd.Wait()
		close(d.waitDone)
	}()

	id := d.id
	if id == "" {
		id = "default"
	}
	if err := d.writeMultiplexed(wireInbound{
		Type:             "start",
		ProtocolVersion:  driverproto.Version,
		ID:               id,
		ModelsURL:        d.modelsURL,
		RequestTimeoutMs: d.requestTimeout.Milliseconds(),
		Config:           d.driverConfig,
	}); err != nil {
		_ = d.Close()
		return err
	}

	select {
	case <-ctx.Done():
		_ = d.Close()
		return ctx.Err()
	case msg := <-initCh:
		if msg.Type == "error" {
			_ = d.Close()
			return errors.New(msg.Error)
		}
		if err := driverproto.ValidateReady(msg); err != nil {
			_ = d.Close()
			return err
		}
		if *msg.Capabilities != d.capabilities {
			_ = d.Close()
			return errors.New("driver capabilities changed after preparation")
		}

		return nil
	case <-d.readDone:
		_ = d.Close()
		return errors.New("external driver closed during startup")
	}
}

func (d *Driver) writeMultiplexed(msg wireInbound) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	select {
	case <-d.closed:
		return errors.New("external driver closed")
	default:
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := d.bw.Write(append(data, '\n')); err != nil {
		return err
	}
	return d.bw.Flush()
}

func (d *Driver) register(id string, buf int) chan wireEvent {
	ch := make(chan wireEvent, buf)
	d.pendMu.Lock()
	d.pending[id] = ch
	d.pendMu.Unlock()
	return ch
}

func (d *Driver) unregister(id string) {
	d.pendMu.Lock()
	delete(d.pending, id)
	d.pendMu.Unlock()
}

func (d *Driver) Do(ctx context.Context, target model.Target, request model.ExecuteRequest) ([]byte, *model.CallError) {
	if request.Kind != model.RequestChat && request.Kind != model.RequestResponses {
		return nil, &model.CallError{Class: model.ErrorInvalid, Status: 400}
	}
	started := time.Now()
	id := fmt.Sprintf("req-%d", d.nextID.Add(1))
	ch := d.register(id, 4)
	defer d.unregister(id)

	err := d.writeMultiplexed(wireInbound{
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
		_ = d.writeMultiplexed(wireInbound{Type: "cancel", ID: id})
		callErr := classifyContextError(ctx)
		d.logResult(ctx, target, request.Kind, started, nil, callErr)
		return nil, callErr
	case msg := <-ch:
		if msg.Type == "error" {
			callErr := fromWireCallError(ctx, msg.CallError, msg.Error)
			d.logResult(ctx, target, request.Kind, started, msg.CallError, callErr)
			return nil, callErr
		}
		d.logUpstreamSuccess(ctx, target, request.Kind, started)
		return []byte(msg.Data), nil
	case <-d.readDone:
		callErr := &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: errors.New("external driver closed"), Scope: model.FailureScopeProvider}
		d.logResult(ctx, target, request.Kind, started, nil, callErr)
		return nil, callErr
	}
}

func (d *Driver) Stream(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
	if request.Kind != model.RequestChat && request.Kind != model.RequestResponses {
		return nil, &model.CallError{Class: model.ErrorInvalid, Status: 400}
	}
	started := time.Now()
	id := fmt.Sprintf("req-%d", d.nextID.Add(1))
	ch := d.register(id, 32)

	err := d.writeMultiplexed(wireInbound{
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
		d.unregister(id)
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: err, Scope: model.FailureScopeProvider}
	}

	select {
	case <-ctx.Done():
		_ = d.writeMultiplexed(wireInbound{Type: "cancel", ID: id})
		d.unregister(id)
		callErr := classifyContextError(ctx)
		d.logResult(ctx, target, request.Kind, started, nil, callErr)
		return nil, callErr
	case msg := <-ch:
		if msg.Type == "error" {
			d.unregister(id)
			callErr := fromWireCallError(ctx, msg.CallError, msg.Error)
			d.logResult(ctx, target, request.Kind, started, msg.CallError, callErr)
			return nil, callErr
		}
		if msg.Type != "stream_opened" {
			d.unregister(id)
			return nil, &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: errors.New("unexpected external driver stream event"), Scope: model.FailureScopeProvider}
		}
	case <-d.readDone:
		d.unregister(id)
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: errors.New("external driver closed"), Scope: model.FailureScopeProvider}
	}

	d.logUpstreamSuccess(ctx, target, request.Kind, started)
	output := make(chan model.StreamEvent, 8)
	go func() {
		defer close(output)
		defer d.unregister(id)
		for {
			select {
			case <-ctx.Done():
				_ = d.writeMultiplexed(wireInbound{Type: "cancel", ID: id})
				return
			case msg := <-ch:
				switch msg.Type {
				case "chunk":
					select {
					case output <- model.StreamEvent{Data: []byte(msg.Data), Event: msg.Event, Meaningful: msg.Meaningful}:
					case <-ctx.Done():
						_ = d.writeMultiplexed(wireInbound{Type: "cancel", ID: id})
						return
					}
				case "error":
					callErr := fromWireCallError(ctx, msg.CallError, msg.Error)
					d.logResult(ctx, target, request.Kind, started, msg.CallError, callErr)
					select {
					case output <- model.StreamEvent{Err: callErr}:
					case <-ctx.Done():
					}
					return
				case "done":
					return
				}
			case <-d.readDone:
				select {
				case output <- model.StreamEvent{Err: &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: errors.New("external driver stream closed"), Scope: model.FailureScopeProvider}}:
				case <-ctx.Done():
				}
				return
			}
		}
	}()
	return output, nil
}

func (d *Driver) Close() error {
	if d.cmd == nil {
		return nil
	}
	d.once.Do(func() {
		close(d.closed)
		_ = d.stdin.Close()
		_ = d.cmd.Process.Signal(os.Interrupt)
		select {
		case <-d.waitDone:
			return
		case <-time.After(500 * time.Millisecond):
			_ = d.cmd.Process.Kill()
		}
	})
	select {
	case <-d.waitDone:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("external driver did not stop")
	}
}

func classifyContextError(ctx context.Context) *model.CallError {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &model.CallError{Class: model.ErrorTimeout, Status: 504, Cause: ctx.Err()}
	}
	return &model.CallError{Class: model.ErrorCancelled, Status: 499, Cause: ctx.Err()}
}

func fromWireCallError(ctx context.Context, w *wireCallError, fallbackErr string) *model.CallError {
	if ctx.Err() != nil {
		return classifyContextError(ctx)
	}
	if w == nil {
		var cause error
		if fallbackErr != "" {
			cause = errors.New(fallbackErr)
		}
		return &model.CallError{Class: model.ErrorUpstream, Status: 502, Cause: cause, Scope: model.FailureScopeProvider}
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

func (d *Driver) logResult(ctx context.Context, target model.Target, kind model.RequestKind, started time.Time, wireErr *wireCallError, callErr *model.CallError) {
	if d.logger == nil {
		return
	}
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
		telemetry.LogEvent(ctx, d.logger, slog.LevelWarn, "provider_request_rejected", attrs...)
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
		telemetry.LogEvent(ctx, d.logger, slog.LevelDebug, "upstream_request_cancelled", attrs...)
		return
	}
	telemetry.LogEvent(ctx, d.logger, slog.LevelWarn, "upstream_request_failed", attrs...)
}

func (d *Driver) logUpstreamSuccess(ctx context.Context, target model.Target, kind model.RequestKind, started time.Time) {
	if d.logger == nil {
		return
	}
	telemetry.LogEvent(ctx, d.logger, slog.LevelDebug, "upstream_request_accepted",
		"provider", target.Provider,
		"native_model", target.Model,
		"kind", kind,
		"status_code", 200,
		"duration_ms", time.Since(started).Milliseconds(),
	)
}

// Models performs fresh transport discovery. The gateway Catalog owns snapshots.
func (d *Driver) Models(ctx context.Context) ([]model.ModelInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return d.queryModels(ctx)
}

// SetModelLister installs the shared catalog before serving requests.
func (d *Driver) SetModelLister(lister model.ModelLister) { d.modelLister = lister }

// DiscoverModels performs discovery without starting a runtime or handshaking.
func DiscoverModels(ctx context.Context, cfg Config) ([]model.ModelInfo, error) {
	command, err := ResolveDriverCommand(cfg.Driver, cfg.Command)
	if err != nil {
		return nil, err
	}
	d := &Driver{id: cfg.ID, command: command, env: cfg.Env, defaultCWD: cfg.DefaultCWD,
		modelsURL: cfg.ModelsURL, driverConfig: cfg.DriverConfig, requestTimeout: cfg.RequestTimeout}
	return d.Models(ctx)
}

func (d *Driver) queryModels(ctx context.Context) ([]session.ModelInfo, error) {
	cmd := exec.CommandContext(ctx, d.command[0], d.command[1:]...)
	if d.defaultCWD != "" {
		cmd.Dir = d.defaultCWD
	}
	if len(d.env) > 0 {
		cmd.Env = appendEnv(os.Environ(), d.env)
	}
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
	defer func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	reqData, _ := json.Marshal(wireInbound{
		Type: "models",
		ID:   d.id, ModelsURL: d.modelsURL, ProtocolVersion: driverproto.Version,
		RequestTimeoutMs: d.requestTimeout.Milliseconds(),
		CWD:              d.defaultCWD,
		Config:           d.driverConfig,
	})
	if _, err := stdin.Write(append(reqData, '\n')); err != nil {
		return nil, err
	}
	_ = stdin.Close()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("driver closed without returning models")
	}
	var ev wireEvent
	if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
		return nil, err
	}
	if ev.Type == "error" {
		return nil, errors.New(ev.Error)
	}
	if ev.Type != "models" {
		return nil, fmt.Errorf("unexpected driver response %q to models request", ev.Type)
	}
	return ev.Models, nil
}

func (d *Driver) Resolve(ctx context.Context, modelName, effort string) (string, error) {
	if modelName == "default" {
		modelName = ""
	}
	if modelName == "" {
		if effort != "" {
			return "", errors.New("select a concrete model to choose effort")
		}
		return "", nil
	}

	lister := d.modelLister
	if lister == nil {
		lister = d
	}
	models, err := lister.Models(ctx)
	if err != nil {
		if effort != "" {
			return "", fmt.Errorf("reasoning effort discovery: %w", err)
		}
		return modelName, nil
	}

	for _, m := range models {
		if m.ID == modelName && len(m.ReasoningEfforts) > 0 {
			if effort == "" {
				return fmt.Sprintf("%s[%s]", modelName, m.ReasoningEfforts[0]), nil
			}
			if slices.Contains(m.ReasoningEfforts, effort) {
				return fmt.Sprintf("%s[%s]", modelName, effort), nil
			}
			return "", fmt.Errorf("reasoning effort %q is not available for model", effort)
		}
	}
	if effort != "" {
		return "", fmt.Errorf("reasoning effort %q is not available for model", effort)
	}
	return modelName, nil
}

type nativeSession struct {
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	bw           *bufio.Writer
	writeMu      sync.Mutex
	output       chan wireEvent
	closed       chan struct{}
	readDone     chan struct{}
	waitDone     chan struct{}
	once         sync.Once
	spec         session.Spec
	init         wireEvent
	toolStrategy string
}

func (d *Driver) Start(ctx context.Context, spec session.Spec) (session.Session, error) {
	cmd := exec.Command(d.command[0], d.command[1:]...)
	cmd.Dir = spec.CWD
	// Drivers own browser/external credentials; stderr is deliberately excluded from logs.
	cmd.Stderr = io.Discard

	if len(d.env) > 0 {
		cmd.Env = appendEnv(os.Environ(), d.env)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}

	s := &nativeSession{
		cmd:          cmd,
		stdin:        stdin,
		bw:           bufio.NewWriter(stdin),
		output:       make(chan wireEvent, 32),
		closed:       make(chan struct{}),
		readDone:     make(chan struct{}),
		waitDone:     make(chan struct{}),
		spec:         spec,
		toolStrategy: d.toolStrategy,
	}

	go func() {
		defer close(s.output)
		defer close(s.readDone)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 16<<20)
		for scanner.Scan() {
			var event wireEvent
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				event = wireEvent{Type: "error", Error: "invalid external driver event"}
			}
			select {
			case s.output <- event:
			case <-s.closed:
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case s.output <- wireEvent{Type: "error", Error: "external driver stream failed"}:
			case <-s.closed:
			}
		}
	}()

	go func() { <-s.readDone; _ = cmd.Wait(); close(s.waitDone) }()

	system := spec.System
	if (d.toolStrategy == ToolStrategyPrompt || d.toolStrategy == ToolStrategyBoth) && len(spec.Tools) > 0 {
		system += "\n\n" + ToolInstructions(spec.Tools)
	}

	startPayload := wireInbound{
		Type:            "start",
		ProtocolVersion: driverproto.Version,
		Config:          d.driverConfig,
		CWD:             spec.CWD,
		Model:           spec.Model,
		System:          system,
		ResumeID:        spec.ResumeID,
		Tools:           spec.Tools,
	}

	if err = s.write(ctx, startPayload); err != nil {
		_ = s.Close()
		return nil, err
	}

	select {
	case <-ctx.Done():
		_ = s.Close()
		return nil, ctx.Err()
	case event, ok := <-s.output:
		if !ok {
			_ = s.Close()
			return nil, errors.New("external driver closed during startup")
		}
		if event.Type == "session_not_found" {
			_ = s.Close()
			return nil, session.ErrSessionNotFound
		}
		if event.Type == "error" {
			_ = s.Close()
			return nil, errors.New("external driver: " + event.Error)
		}
		if event.Type != "ready" {
			_ = s.Close()
			return nil, errors.New("external driver did not become ready")
		}
		s.init = event
		return s, nil
	}
}

func (s *nativeSession) write(ctx context.Context, value any) error {
	if ctx.Err() != nil {
		_ = s.Close()
		return ctx.Err()
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		select {
		case <-s.closed:
			done <- errors.New("external driver closed")
			return
		default:
		}
		if _, err := s.bw.Write(append(data, '\n')); err != nil {
			done <- err
			return
		}
		done <- s.bw.Flush()
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = s.Close()
		return ctx.Err()
	case <-s.closed:
		return errors.New("external driver closed")
	}
}

func (s *nativeSession) Run(ctx context.Context, prompt string, emit func(session.Event) error) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	if s.init.Type != "" {
		if err := emit(session.Event{Kind: session.EventInit, ResumeID: s.init.ResumeID}); err != nil {
			return err
		}
		s.init = wireEvent{}
	}
	if err := s.write(ctx, wireInbound{Type: "prompt", Text: prompt}); err != nil {
		return err
	}
	callErrCh := make(chan error, 1)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-callErrCh:
			return err
		case event, ok := <-s.output:
			if !ok {
				return errors.New("external driver closed before completion")
			}
			switch event.Type {
			case "text", "reasoning":
				kind := session.EventText
				if event.Type == "reasoning" {
					kind = session.EventReasoning
				}
				if err := emit(session.Event{Kind: kind, Text: event.Text}); err != nil {
					return err
				}
			case "heartbeat":
				if err := emit(session.Event{Kind: session.EventHeartbeat}); err != nil {
					return err
				}
			case "usage":
				if err := emit(session.Event{Kind: session.EventUsage, Usage: &session.Usage{Input: event.Input, Output: event.Output}}); err != nil {
					return err
				}
			case "session_updated":
				if event.ResumeID == "" {
					return errors.New("external driver sent an empty session update")
				}
				if err := emit(session.Event{Kind: session.EventInit, ResumeID: event.ResumeID}); err != nil {
					return err
				}
			case "tool_call":
				if s.toolStrategy == ToolStrategyPrompt {
					continue
				}
				wg.Add(1)
				go func(id, name string, args json.RawMessage) {
					defer wg.Done()
					if err := s.call(ctx, id, name, args); err != nil {
						select {
						case callErrCh <- err:
						default:
						}
					}
				}(event.ID, event.Name, event.Arguments)
			case "message":
				if s.toolStrategy == ToolStrategyWire || s.toolStrategy == ToolStrategyMCP {
					continue
				}
				text, calls, err := DecodeToolResponse(event.Text)
				if err != nil {
					return err
				}
				if text != "" {
					if err = emit(session.Event{Kind: session.EventText, Text: text}); err != nil {
						return err
					}
				}
				for i, call := range calls {
					if err = s.call(ctx, fmt.Sprintf("%s/%d", event.ID, i), call.Name, call.Arguments); err != nil {
						return err
					}
				}
			case "done":
				return nil
			case "session_not_found":
				return session.ErrSessionNotFound
			case "error":
				return errors.New("external driver: " + event.Error)
			default:
				return fmt.Errorf("unknown external driver event %q", event.Type)
			}
		}
	}
}

func (s *nativeSession) call(ctx context.Context, id, name string, args json.RawMessage) error {
	if s.spec.CallTool == nil {
		return errors.New("tools are unavailable")
	}
	result, err := s.spec.CallTool(ctx, name, args)
	if err != nil {
		_ = s.write(ctx, wireInbound{Type: "tool_result", ID: id, Error: err.Error()})
		return err
	}
	return s.write(ctx, wireInbound{Type: "tool_result", ID: id, Content: result})
}

func (s *nativeSession) Close() error {
	s.once.Do(func() {
		close(s.closed)
		_ = s.stdin.Close()
		// Give external drivers an opportunity to clean up in a finally/deferred
		// block before escalating to a hard kill.
		_ = s.cmd.Process.Signal(os.Interrupt)
		select {
		case <-s.waitDone:
			return
		case <-time.After(500 * time.Millisecond):
			_ = s.cmd.Process.Kill()
		}
	})
	select {
	case <-eWait(s.waitDone):
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("external driver did not stop")
	}
}

func eWait(ch <-chan struct{}) <-chan struct{} {
	return ch
}

func (d *Driver) ID() string { return d.id }
