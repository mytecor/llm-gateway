package scheduler

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

type recordingStreamSink struct {
	events                   []model.StreamEvent
	begun, completed, failed bool
	write                    func(model.StreamEvent) bool
	keepAlive                func(StreamHeartbeat) bool
}

func (s *recordingStreamSink) Begin() { s.begun = true }
func (s *recordingStreamSink) Write(event model.StreamEvent) bool {
	s.events = append(s.events, event)
	return s.write == nil || s.write(event)
}
func (s *recordingStreamSink) KeepAlive(h StreamHeartbeat) bool {
	return s.keepAlive == nil || s.keepAlive(h)
}
func (s *recordingStreamSink) Complete()                   { s.completed = true }
func (s *recordingStreamSink) Fail(*model.CallError, bool) { s.failed = true }

func streamRunConfig(t *testing.T, providers []string) *config.CompiledConfig {
	t.Helper()
	cfg := config.RulesConfig(t, len(providers), policy.EntryRules(providers, 1))
	cfg.Models["standard"].Admission = policy.AdmissionConfig{MaxInFlight: 1, MaxPending: 1, WaitTimeout: time.Second}
	cfg.Models["standard"].Continue = policy.ContinueConfig{Enabled: true, Idle: time.Second, Wait: time.Millisecond}
	return cfg
}

func streamRunRequest() model.ExecuteRequest {
	return model.ExecuteRequest{Kind: model.RequestChat, Body: []byte(`{"messages":[{"role":"user","content":"hello"}]}`)}
}

func completionStream(text string, finish bool) <-chan model.StreamEvent {
	ch := make(chan model.StreamEvent, 2)
	ch <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{"content":"` + text + `"}}]}`), Meaningful: true}
	if finish {
		ch <- model.StreamEvent{Data: []byte(`{"choices":[{"delta":{},"finish_reason":"stop"}]}`)}
	}
	close(ch)
	return ch
}

func assertStreamRunReleased(t *testing.T, runner *Runner, cfg *config.CompiledConfig) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, err := runner.admission.Admit(ctx, "standard", cfg.Models["standard"].Admission, time.Time{})
	if err != nil {
		t.Fatalf("route permit leaked: %v", err)
	}
	release()
	for provider := range cfg.Providers {
		for {
			changed := runner.capacity.Changed()
			if runner.capacity.Count(provider) == 0 {
				break
			}
			select {
			case <-changed:
			case <-ctx.Done():
				t.Fatalf("provider %s capacity leaked", provider)
			}
		}
	}
}

func TestRunStreamContinuationWithoutHTTP(t *testing.T) {
	cfg := streamRunConfig(t, []string{"a", "b"})
	var calls atomic.Int32
	executor := &fakeExecutor{stream: func(_ context.Context, target model.Target, req model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		calls.Add(1)
		if target.Provider == "a" {
			return completionStream("partial", false), nil
		}
		if !strings.Contains(string(req.Body), "partial") {
			t.Error("continuation lost delivered history")
		}
		return completionStream("recovered", true), nil
	}}
	runner := NewRunner(cfg, subsystem.NewCatalog(cfg), executor)
	defer runner.Close()
	sink := &recordingStreamSink{}
	runner.RunStream(context.Background(), "standard", streamRunRequest(), time.Now(), sink)
	if !sink.begun || !sink.completed || sink.failed || calls.Load() != 2 {
		t.Fatalf("continuation failed: begun=%v completed=%v failed=%v calls=%d", sink.begun, sink.completed, sink.failed, calls.Load())
	}
	var text string
	for _, event := range sink.events {
		text += string(event.Data)
	}
	if !strings.Contains(text, "partial") || !strings.Contains(text, "recovered") {
		t.Fatalf("missing output: %s", text)
	}
	assertStreamRunReleased(t, runner, cfg)
}

func TestRunStreamRulesInspectContinuationPrelude(t *testing.T) {
	cfg := streamRunConfig(t, []string{"a", "b", "c"})
	cfg.Models["standard"].Repetition = policy.RepetitionConfig{
		Enabled: true, Repeats: 4, MinLen: 6, MaxLen: 256,
	}
	var calls atomic.Int32
	executor := &fakeExecutor{stream: func(_ context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		calls.Add(1)
		switch target.Provider {
		case "a":
			return completionStream("partial", false), nil
		case "b":
			// The first meaningful chunk is buffered by selection. It must go
			// through the loop guard even though this is a continuation winner.
			return completionStream("Tool call. Tool call. Tool call. Tool call.", true), nil
		default:
			return completionStream("recovered", true), nil
		}
	}}
	runner := NewRunner(cfg, subsystem.NewCatalog(cfg), executor)
	defer runner.Close()
	sink := &recordingStreamSink{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	runner.RunStream(ctx, "standard", streamRunRequest(), time.Now(), sink)
	if !sink.completed || sink.failed || calls.Load() != 3 {
		t.Fatalf("buffered continuation bypassed rules: complete=%v failed=%v calls=%d", sink.completed, sink.failed, calls.Load())
	}
	var text string
	for _, event := range sink.events {
		text += subsystem.StreamTextDelta(string(event.Data))
	}
	if text != "partialrecovered" {
		t.Fatalf("loop or duplicate buffered output reached the client: %q", text)
	}
	assertStreamRunReleased(t, runner, cfg)
}

func TestRunStreamWriteFailureReleasesWinner(t *testing.T) {
	for _, duringContinuation := range []bool{false, true} {
		name := "initial"
		if duringContinuation {
			name = "continuation"
		}
		t.Run(name, func(t *testing.T) {
			cfg := streamRunConfig(t, []string{"a", "b"})
			var calls atomic.Int32
			executor := &fakeExecutor{stream: func(_ context.Context, target model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
				calls.Add(1)
				return completionStream(target.Provider, target.Provider == "b"), nil
			}}
			runner := NewRunner(cfg, subsystem.NewCatalog(cfg), executor)
			defer runner.Close()
			sink := &recordingStreamSink{write: func(event model.StreamEvent) bool {
				return duringContinuation && !strings.Contains(string(event.Data), `"content":"b"`)
			}}
			runner.RunStream(context.Background(), "standard", streamRunRequest(), time.Now(), sink)
			wantCalls := int32(1)
			if duringContinuation {
				wantCalls = 2
			}
			if sink.completed || sink.failed || calls.Load() != wantCalls {
				t.Fatalf("run continued after transport exit: complete=%v fail=%v calls=%d", sink.completed, sink.failed, calls.Load())
			}
			assertStreamRunReleased(t, runner, cfg)
		})
	}
}

func TestRunStreamKeepAliveFailureCancelsSelection(t *testing.T) {
	oldKeepAlive := policy.ContinueRecoveryKeepAlive
	policy.ContinueRecoveryKeepAlive = 5 * time.Millisecond
	t.Cleanup(func() { policy.ContinueRecoveryKeepAlive = oldKeepAlive })
	cfg := streamRunConfig(t, []string{"a"})
	upstreamCancelled := make(chan struct{})
	executor := &fakeExecutor{stream: func(ctx context.Context, _ model.Target, _ model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		ch := make(chan model.StreamEvent)
		go func() { <-ctx.Done(); close(upstreamCancelled); close(ch) }()
		return ch, nil
	}}
	runner := NewRunner(cfg, subsystem.NewCatalog(cfg), executor)
	defer runner.Close()
	sink := &recordingStreamSink{keepAlive: func(StreamHeartbeat) bool { return false }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	runner.RunStream(ctx, "standard", streamRunRequest(), time.Now(), sink)
	select {
	case <-upstreamCancelled:
	case <-ctx.Done():
		t.Fatal("transport exit left selection running")
	}
	if sink.completed || sink.failed {
		t.Fatal("terminal output sent after transport failure")
	}
	assertStreamRunReleased(t, runner, cfg)
}

func TestRunStreamSelectionFailurePrecedesBegin(t *testing.T) {
	cfg := streamRunConfig(t, []string{"a"})
	executor := &fakeExecutor{stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		return nil, &model.CallError{Class: model.ErrorUpstream, Status: 502}
	}}
	runner := NewRunner(cfg, subsystem.NewCatalog(cfg), executor)
	defer runner.Close()
	sink := &recordingStreamSink{}
	runner.RunStream(context.Background(), "standard", streamRunRequest(), time.Now(), sink)
	if sink.begun || !sink.failed || sink.completed {
		t.Fatalf("selection error committed stream: %+v", sink)
	}
	assertStreamRunReleased(t, runner, cfg)
}

// Even a branch that produced meaningful output can lose the race before the
// caller accepts its selected stream. Cancelling it must free provider capacity.
func TestCancelledSelectedBranchReleasesCapacity(t *testing.T) {
	cfg := streamRunConfig(t, []string{"a"})
	executor := &fakeExecutor{stream: func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
		return completionStream("answer", true), nil
	}}
	runner := NewRunner(cfg, subsystem.NewCatalog(cfg), executor)
	defer runner.Close()
	permit := runner.capacity.Acquire("a")
	if permit == nil {
		t.Fatal("cannot reserve provider")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc := &schedule{r: runner, stream: true, request: streamRunRequest(), results: make(chan *branchResult, 1)}
	sc.runBranch(ctx, 1, cancel, model.Target{Provider: "a", Model: "native-model"}, permit)
	result := <-sc.results
	if result.selected == nil {
		t.Fatal("branch did not produce a selected stream")
	}
	cancel()
	assertStreamRunReleased(t, runner, cfg)
	result.selected.Cancel()
	if runner.capacity.Count("a") != 0 {
		t.Fatal("repeated cancellation changed released capacity")
	}
}
