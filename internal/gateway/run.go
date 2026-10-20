package gateway

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mytecor/llm-gateway/internal/executor/bifrost"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/scheduler"
	"github.com/mytecor/llm-gateway/internal/server"
	"github.com/mytecor/llm-gateway/internal/subsystem"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

// Run executes the llm-gateway CLI commands ("check" or "serve").
func Run(arguments []string) error {
	flags := flag.NewFlagSet("llm-gateway", flag.ContinueOnError)
	configPath := flags.String("config", "", "path to runtime JSON config")
	requireEnvSecrets := flags.Bool("require-env-secrets", false, "reject literal secrets in public config")
	allowedEnv := flags.String("allowed-env", "", "comma-separated env names declared by the service wrapper")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *configPath == "" || flags.NArg() != 1 {
		return errors.New("usage: llm-gateway --config PATH <check|serve>")
	}
	command := flags.Arg(0)
	if command != "check" && command != "serve" {
		return errors.New("usage: llm-gateway --config PATH <check|serve>")
	}
	if command == "check" {
		cfg, err := policy.DecodeConfig(*configPath)
		if err != nil {
			return err
		}
		if *requireEnvSecrets {
			if err := policy.ValidateEnvSecretReferences(cfg, parseAllowedEnv(*allowedEnv)); err != nil {
				return fmt.Errorf("validate config: %w", err)
			}
		}
		if _, err := policy.CompileConfig(cfg); err != nil {
			return fmt.Errorf("validate config: %w", err)
		}
		return nil
	}
	cfg, err := policy.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	compiled, err := policy.CompileConfig(cfg)
	if err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	catalog := subsystem.NewCatalog(compiled)
	reportCatalogErrors(compiled.Logger, catalog.Refresh(ctx))
	catalog.Start(ctx, compiled.Raw.CatalogRefreshInterval.Duration, func(errorsByGroup map[string]error) {
		reportCatalogErrors(compiled.Logger, errorsByGroup)
	})
	executor, err := bifrost.NewBifrostExecutor(ctx, compiled)
	if err != nil {
		return err
	}
	defer executor.Close()
	runner := scheduler.NewRunner(compiled, catalog, executor)
	defer runner.Close()
	httpServer := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", compiled.Raw.Host, compiled.Raw.Port),
		Handler:           server.NewServer(compiled, catalog, runner),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	telemetry.LogEvent(context.Background(), compiled.Logger, slog.LevelInfo, "gateway_starting",
		"address", httpServer.Addr,
		"metrics_address", fmt.Sprintf("%s:%d", compiled.Raw.MetricsHost, compiled.Raw.MetricsPort),
		"log_level", compiled.Raw.LogLevel,
		"providers", len(compiled.Providers),
		"models", len(compiled.LogicalIDs),
	)

	// The metrics listener is a dedicated loopback endpoint (metrics_host /
	// metrics_port, default 127.0.0.1:9209). It serves only /metrics without
	// client authentication; the API listener never exposes it. The same metric
	// registry backs both listeners, so a scrape of the dedicated address and a
	// scrape of the API address observe one consistent surface.
	metricsServer := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", compiled.Raw.MetricsHost, compiled.Raw.MetricsPort),
		Handler:           server.NewMetricsHandler(runner.Metrics()),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	serveErrors := make(chan error, 2)
	go func() { serveErrors <- httpServer.ListenAndServe() }()
	go func() { serveErrors <- metricsServer.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
		return httpServer.Shutdown(shutdownCtx)
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func parseAllowedEnv(value string) map[string]bool {
	allowed := make(map[string]bool)
	if value == "" {
		return allowed
	}
	for _, name := range strings.Split(value, ",") {
		allowed[name] = true
	}
	return allowed
}

func reportCatalogErrors(logger *slog.Logger, errorsByProvider map[string]error) {
	for providerID, err := range errorsByProvider {
		telemetry.LogEvent(context.Background(), logger, slog.LevelWarn, "catalog_refresh_failed",
			"provider", providerID,
			"detail", telemetry.SafeLogDetail(err.Error()),
		)
	}
}
