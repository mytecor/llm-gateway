package config

import (
	"github.com/mytecor/llm-gateway/internal/testsupport/policytest"

	"context"
	"strings"
	"testing"

	"github.com/mytecor/llm-gateway/internal/executor/external"
	"github.com/mytecor/llm-gateway/internal/model"
)

func TestCompilationDoesNotExecutePreparedDriver(t *testing.T) {
	cfg := defaultConfig()
	cfg.Providers = cfg.Providers[:1]
	cfg.Providers[0].Driver = ""
	cfg.Providers[0].Command = []string{"/does/not/exist/driver"}
	cfg.Providers[0].Description = &external.Description{Capabilities: model.Capabilities{Chat: true, Streaming: true, SafeReplay: true}}
	cfg.Rules = policytest.EntryRules([]string{"a"}, 1)
	if _, err := CompileConfig(PreparedConfig{config: cfg}); err != nil {
		t.Fatalf("pure compilation depended on driver binary: %v", err)
	}
	cfg.Providers[0].Description = nil
	if _, err := CompileConfig(PreparedConfig{config: cfg}); err == nil || !strings.Contains(err.Error(), "prepare") {
		t.Fatalf("accepted unknown capabilities: %v", err)
	}
}

func TestPreparationHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := PrepareConfig(ctx, defaultConfig()); err == nil {
		t.Fatal("cancelled preparation succeeded")
	}
}

func TestCompilationRejectsUnpreparedZeroValue(t *testing.T) {
	if _, err := CompileConfig(PreparedConfig{}); err == nil {
		t.Fatal("zero prepared configuration was accepted")
	}
}
