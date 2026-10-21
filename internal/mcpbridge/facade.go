package mcpbridge

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"time"
)

type ServerDescriptor struct {
	Name    string            `json:"name"`
	Command string            `json:"command"`
	Args    []string          `json:"args"`
	Env     map[string]string `json:"env,omitempty"`
}

type Facade struct {
	ctx        context.Context
	cancel     context.CancelFunc
	server     *http.Server
	descriptor ServerDescriptor
	token      string
	tools      []Tool
	call       func(context.Context, string, json.RawMessage) (string, error)
}

func NewFacade(parent context.Context, tools []Tool, call func(context.Context, string, json.RawMessage) (string, error)) (*Facade, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	f := &Facade{ctx: ctx, cancel: cancel, token: hex.EncodeToString(secret), tools: append([]Tool(nil), tools...), call: call}
	f.descriptor = ServerDescriptor{Name: "openai-functions", Command: exe, Args: []string{"__mcp-bridge", listener.Addr().String(), f.token}}
	f.server = &http.Server{Handler: f, ReadHeaderTimeout: 5 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() { _ = f.server.Serve(listener) }()
	return f, nil
}

func (f *Facade) Close() {
	f.cancel()
	_ = f.server.Close()
}

func (f *Facade) Descriptor() ServerDescriptor {
	return f.descriptor
}

func (f *Facade) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+f.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case "/tools":
		_ = json.NewEncoder(w).Encode(f.tools)
	case "/watch":
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-f.ctx.Done():
		case <-r.Context().Done():
			f.cancel()
		}
	case "/call":
		var call Call
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&call); err != nil {
			http.Error(w, "invalid tool call", 400)
			return
		}
		known := false
		for _, t := range f.tools {
			if t.Name == call.Name {
				known = true
				break
			}
		}
		if !known {
			http.Error(w, "unknown tool name", 400)
			return
		}
		if len(call.Arguments) == 0 {
			call.Arguments = json.RawMessage(`{}`)
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		result, err := f.call(r.Context(), call.Name, call.Arguments)
		if err != nil {
			_ = json.NewEncoder(w).Encode(Result{Error: err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(Result{Content: result})
	default:
		http.NotFound(w, r)
	}
}
