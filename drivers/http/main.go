package main

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/mytecor/llm-gateway/driverproto"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Capabilities = driverproto.Capabilities
type CatalogSource = driverproto.CatalogSource

type inboundMessage = driverproto.Command[json.RawMessage]

type outboundMessage = driverproto.Event

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := serve(ctx, os.Stdin, os.Stdout); err != nil {
		os.Exit(1)
	}
}

func serve(ctx context.Context, r io.Reader, w io.Writer) error {
	var writeMu sync.Mutex
	bw := bufio.NewWriter(w)
	emit := func(ev outboundMessage) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		if ev.Type == "ready" {
			ev.ProtocolVersion = driverproto.Version
		}
		data, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		if _, err := bw.Write(append(data, '\n')); err != nil {
			return err
		}
		return bw.Flush()
	}

	var exec *Executor
	var inflightMu sync.Mutex
	inflight := make(map[string]context.CancelFunc)
	var wg sync.WaitGroup

	defer func() {
		inflightMu.Lock()
		for _, cancel := range inflight {
			cancel()
		}
		inflightMu.Unlock()
		wg.Wait()
		if exec != nil {
			_ = exec.Close()
		}
	}()

	lines := make(chan []byte)
	scanErr := make(chan error, 1)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 4096), 16<<20)
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case lines <- line:
			case <-ctx.Done():
				return
			}
		}
		scanErr <- scanner.Err()
	}()

	for {
		var line []byte
		var ok bool
		select {
		case <-ctx.Done():
			return nil
		case line, ok = <-lines:
			if !ok {
				select {
				case err := <-scanErr:
					return err
				default:
					return nil
				}
			}
		}
		if len(line) == 0 {
			continue
		}
		var msg inboundMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			_ = emit(outboundMessage{Type: "error", Error: "invalid inbound HTTP driver message"})
			return err
		}

		if err := driverproto.CheckVersion(msg.ProtocolVersion); err != nil {
			_ = emit(outboundMessage{Type: "error", Error: err.Error()})
			return nil
		}

		switch msg.Type {
		case "init", "describe":
			_, catalog, normalizedCfg, err := ValidateProviderConfig(msg)
			if err != nil {
				_ = emit(outboundMessage{Type: "error", Error: err.Error()})
				return nil
			}
			_ = emit(outboundMessage{
				Type:         "ready",
				Capabilities: defaultCapabilities(),
				Catalog:      catalog,
				Config:       normalizedCfg,
			})
			return nil

		case "models":
			models, err := listModels(ctx, msg)
			if err != nil {
				_ = emit(outboundMessage{Type: "error", Error: err.Error()})
			} else {
				_ = emit(outboundMessage{Type: "models", Models: models})
			}
			return nil

		case "start":
			if exec != nil {
				_ = emit(outboundMessage{Type: "error", Error: "HTTP driver already started"})
				continue
			}
			var providers []ProviderConfig
			for _, raw := range msg.Providers {
				var p ProviderConfig
				if err := json.Unmarshal(raw, &p); err != nil {
					return err
				}
				providers = append(providers, p)
			}
			var catalog *CatalogSource
			var normalizedCfg map[string]any
			if len(providers) == 0 {
				var single ProviderConfig
				var err error
				single, catalog, normalizedCfg, err = ValidateProviderConfig(msg)
				if err != nil {
					_ = emit(outboundMessage{Type: "error", Error: err.Error()})
					return nil
				}
				providers = []ProviderConfig{single}
			}
			var err error
			exec, err = NewExecutor(ctx, providers)
			if err != nil {
				_ = emit(outboundMessage{Type: "error", Error: err.Error()})
				return nil
			}
			_ = emit(outboundMessage{
				Type:         "ready",
				Capabilities: defaultCapabilities(),
				Catalog:      catalog,
				Config:       normalizedCfg,
			})

		case "cancel":
			if msg.ID != "" {
				inflightMu.Lock()
				cancel, exists := inflight[msg.ID]
				if exists {
					delete(inflight, msg.ID)
				}
				inflightMu.Unlock()
				if exists {
					cancel()
				}
			}

		case "request":
			if exec == nil {
				_ = emit(outboundMessage{
					Type:      "error",
					ID:        msg.ID,
					CallError: &CallError{Class: "invalid_response", Status: 503, Detail: "HTTP driver not initialized"},
				})
				continue
			}
			reqCtx, reqCancel := context.WithCancel(ctx)
			inflightMu.Lock()
			inflight[msg.ID] = reqCancel
			inflightMu.Unlock()

			wg.Add(1)
			go func(m inboundMessage, rCtx context.Context, rCancel context.CancelFunc) {
				defer wg.Done()
				defer func() {
					inflightMu.Lock()
					delete(inflight, m.ID)
					inflightMu.Unlock()
					rCancel()
				}()

				if !m.Stream {
					data, callErr := exec.Do(rCtx, m.Provider, m.Model, m.Kind, m.Body)
					if callErr != nil {
						_ = emit(outboundMessage{Type: "error", ID: m.ID, CallError: callErr})
						return
					}
					_ = emit(outboundMessage{Type: "response", ID: m.ID, Data: json.RawMessage(data)})
					return
				}

				idleTimeout := time.Duration(m.StreamIdleTimeoutMs) * time.Millisecond
				stream, callErr := exec.Stream(rCtx, m.Provider, m.Model, m.Kind, m.Body, idleTimeout)
				if callErr != nil {
					_ = emit(outboundMessage{Type: "error", ID: m.ID, CallError: callErr})
					return
				}
				if err := emit(outboundMessage{Type: "stream_opened", ID: m.ID}); err != nil {
					return
				}
				for chunk := range stream {
					if chunk.Err != nil {
						_ = emit(outboundMessage{Type: "error", ID: m.ID, CallError: chunk.Err})
						return
					}
					if err := emit(outboundMessage{
						Type:       "chunk",
						ID:         m.ID,
						Data:       chunk.Data,
						Event:      chunk.Event,
						Meaningful: chunk.Meaningful,
					}); err != nil {
						return
					}
				}
				_ = emit(outboundMessage{Type: "done", ID: m.ID})
			}(msg, reqCtx, reqCancel)

		default:
			_ = emit(outboundMessage{Type: "error", ID: msg.ID, Error: "unknown HTTP driver command: " + msg.Type})
		}
	}
}
