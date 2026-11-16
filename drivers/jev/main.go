// jev-driver multiplexes independent native System One decisions over NDJSON.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mytecor/llm-gateway/driverproto"
	"github.com/mytecor/llm-gateway/driverproto/systemone"
)

type command = driverproto.Command[json.RawMessage]

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if serve(ctx, os.Stdin, os.Stdout) != nil {
		os.Exit(1)
	}
}
func serve(ctx context.Context, input io.Reader, output io.Writer) error {
	var writeMu sync.Mutex
	emit := func(event driverproto.Event) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		if event.Type == "ready" {
			event.ProtocolVersion = driverproto.Version
		}
		return json.NewEncoder(output).Encode(event)
	}
	ctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	var wg sync.WaitGroup
	var mu sync.Mutex
	inflight := make(map[string]context.CancelFunc)
	defer func() { cancelAll(); wg.Wait() }()
	// Read independently so shutdown does not wait for stdin from a live parent.
	lines := make(chan []byte)
	scanErrors := make(chan error, 1)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(input)
		scan.Buffer(make([]byte, 4096), systemone.MaxCommandBytes)
		for scan.Scan() {
			line := bytes.Clone(scan.Bytes())
			select {
			case lines <- line:
			case <-ctx.Done():
				return
			}
		}
		scanErrors <- scan.Err()
	}()
	var exec *executor
	defaultTimeout := 60 * time.Second
	for {
		var line []byte
		var ok bool
		select {
		case <-ctx.Done():
			return nil
		case line, ok = <-lines:
			if !ok {
				return <-scanErrors
			}
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var msg command
		if json.Unmarshal(line, &msg) != nil {
			return errors.New("invalid JEV command")
		}
		if err := driverproto.CheckVersion(msg.ProtocolVersion); err != nil {
			_ = emit(driverproto.Event{Type: "error", Error: err.Error()})
			return err
		}
		switch msg.Type {
		case "init", "describe", "start", "models":
			if exec != nil {
				return errors.New("JEV worker already initialized")
			}
			if msg.RequestTimeoutMs > 0 {
				defaultTimeout = time.Duration(msg.RequestTimeoutMs) * time.Millisecond
			}
			var err error
			exec, err = newExecutor(msg.Config)
			if err != nil {
				_ = emit(driverproto.Event{Type: "error", Error: err.Error()})
				return err
			}
			if msg.HasSession {
				return emit(driverproto.Event{Type: "error", Error: "JEV does not support sessions"})
			}
			if msg.Type == "models" {
				models, err := exec.models(ctx, msg.ModelsURL)
				if err != nil {
					return emit(driverproto.Event{Type: "error", Error: "JEV model discovery failed"})
				}
				return emit(driverproto.Event{Type: "models", Models: models})
			}
			cfg := map[string]any{"dialect": exec.config.Dialect, "inference_url": exec.config.InferenceURL, "api_key": exec.config.APIKey}
			if err := emit(driverproto.Event{Type: "ready", Capabilities: ptr(exec.capabilities()), Config: cfg, Catalog: &driverproto.CatalogSource{URL: exec.config.InferenceURL + "/models", APIKey: exec.config.APIKey, Explicit: msg.ModelsURL != ""}}); err != nil {
				return err
			}
			if msg.Type != "start" {
				return nil
			}
		case "cancel":
			mu.Lock()
			if cancel := inflight[msg.ID]; cancel != nil {
				cancel()
			}
			mu.Unlock()
		case "request":
			if exec == nil || msg.ID == "" {
				return errors.New("JEV request requires initialized worker and ID")
			}
			requestCtx, cancel := context.WithCancel(ctx)
			timeout := defaultTimeout
			if msg.RequestTimeoutMs > 0 {
				timeout = time.Duration(msg.RequestTimeoutMs) * time.Millisecond
			}
			requestCtx, deadlineCancel := context.WithTimeout(requestCtx, timeout)
			stop := func() { cancel(); deadlineCancel() }
			mu.Lock()
			_, duplicate := inflight[msg.ID]
			if !duplicate {
				inflight[msg.ID] = stop
			}
			mu.Unlock()
			if duplicate {
				stop()
				return errors.New("duplicate in-flight JEV ID")
			}
			wg.Add(1)
			go func(msg command) {
				defer wg.Done()
				defer stop()
				defer func() { mu.Lock(); delete(inflight, msg.ID); mu.Unlock() }()
				body, err := exec.do(requestCtx, msg)
				event := driverproto.Event{Type: "response", ID: msg.ID, Data: body}
				if err != nil {
					event.Type = "error"
					event.CallError = err
				}
				if emit(event) != nil {
					cancelAll()
				}
			}(msg)
		default:
			return errors.New("unsupported JEV command")
		}
	}
}
func ptr[T any](v T) *T { return &v }
