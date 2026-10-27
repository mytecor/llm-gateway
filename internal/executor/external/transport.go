package external

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	"github.com/mytecor/llm-gateway/driverproto"
)

type wireCapabilities = driverproto.Capabilities
type wireCallError = driverproto.CallError
type wireModelInfo = driverproto.ModelInfo
type wireInbound = driverproto.Command[map[string]any]
type wireEvent = driverproto.Event

// processTransport owns framing, cancellation of blocked pipe writes and process
// cleanup. Request multiplexing and native turn semantics belong to its callers.
type processTransport struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	stdout    io.ReadCloser
	writer    *bufio.Writer
	writeGate chan struct{}
	events    chan wireEvent
	closed    chan struct{}
	readDone  chan struct{}
	waitDone  chan struct{}
	stop      sync.Once
	readErr   error // published by closing events/readDone
}

func appendEnv(base []string, extra map[string]string) []string {
	keys := make([]string, 0, len(extra))
	for key := range extra {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := append([]string(nil), base...)
	for _, key := range keys {
		out = append(out, key+"="+extra[key])
	}
	return out
}

func startProcess(command []string, env map[string]string, cwd string) (*processTransport, error) {
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Dir = cwd
	if len(env) > 0 {
		cmd.Env = appendEnv(os.Environ(), env)
	}
	// Credentials and browser diagnostics must not escape through stderr.
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
	if err = cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	p := &processTransport{
		cmd: cmd, stdin: stdin, stdout: stdout, writer: bufio.NewWriter(stdin),
		writeGate: make(chan struct{}, 1), events: make(chan wireEvent, 32),
		closed: make(chan struct{}), readDone: make(chan struct{}), waitDone: make(chan struct{}),
	}
	go p.read()
	go func() { <-p.readDone; _ = cmd.Wait(); close(p.waitDone) }()
	return p, nil
}

func (p *processTransport) read() {
	defer close(p.readDone)
	defer close(p.events)
	scanner := bufio.NewScanner(p.stdout)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	for scanner.Scan() {
		var event wireEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			p.readErr = errors.New("invalid external driver event")
			p.abort()
			return
		}
		select {
		case p.events <- event:
		case <-p.closed:
			return
		}
	}
	p.readErr = scanner.Err()
	if p.readErr == nil {
		p.readErr = errors.New("external driver closed")
	}
}

func (p *processTransport) write(ctx context.Context, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		return errors.New("external driver closed")
	case p.writeGate <- struct{}{}:
	}
	// A waiter cancelled before acquiring the gate never writes its command.
	if err := ctx.Err(); err != nil {
		<-p.writeGate
		return err
	}
	done := make(chan error, 1)
	go func() {
		defer func() { <-p.writeGate }()
		if _, err := p.writer.Write(append(data, '\n')); err != nil {
			done <- err
			return
		}
		done <- p.writer.Flush()
	}()
	select {
	case err := <-done:
		if err != nil {
			p.abort()
		}
		return err
	case <-ctx.Done():
		// A partially written frame cannot be reused. Replacing this process
		// also releases a driver that stopped reading stdin.
		p.abort()
		return ctx.Err()
	case <-p.closed:
		return errors.New("external driver closed")
	}
}

func (p *processTransport) exchange(ctx context.Context, command wireInbound, closeInput bool) (wireEvent, error) {
	if err := p.write(ctx, command); err != nil {
		return wireEvent{}, err
	}
	if closeInput {
		_ = p.stdin.Close()
	}
	select {
	case <-ctx.Done():
		return wireEvent{}, ctx.Err()
	case event, ok := <-p.events:
		if !ok {
			return wireEvent{}, p.readErr
		}
		return event, nil
	}
}

func (p *processTransport) abort() {
	p.stop.Do(func() {
		close(p.closed)
		_ = p.stdin.Close()
		_ = p.stdout.Close()
		_ = p.cmd.Process.Kill()
	})
}

func (p *processTransport) Close() error {
	p.stop.Do(func() {
		close(p.closed)
		_ = p.stdin.Close()
		_ = p.cmd.Process.Signal(os.Interrupt)
	})
	select {
	case <-p.waitDone:
		return nil
	case <-time.After(500 * time.Millisecond):
		_ = p.cmd.Process.Kill()
		_ = p.stdout.Close()
	}
	select {
	case <-p.waitDone:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("external driver did not stop")
	}
}
