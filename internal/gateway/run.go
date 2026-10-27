package gateway

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/executor"
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
		return config.CheckConfig(*configPath, *requireEnvSecrets, *allowedEnv)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	prepared, err := config.PrepareConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("prepare config: %w", err)
	}
	compiled, err := config.CompileConfig(prepared)
	if err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	catalog := subsystem.NewCatalog(compiled)
	reportCatalogErrors(compiled.Logger, catalog.Refresh(ctx))
	catalog.Start(ctx, compiled.Raw.CatalogRefreshInterval.Duration, func(errorsByGroup map[string]error) {
		reportCatalogErrors(compiled.Logger, errorsByGroup)
	})
	executors, err := executor.New(ctx, compiled)
	if err != nil {
		return err
	}
	defer executors.Close()
	plan := compiled.RoutingPlan()
	metrics := telemetry.NewMetrics()
	capacity := subsystem.NewProviderCapacity(metrics)
	affinity := subsystem.NewAffinityStore(time.Now, compiled.Raw.AffinityFile, func(err error) {
		telemetry.LogEvent(context.Background(), compiled.Logger, slog.LevelError, "affinity_persistence_failed", "detail", telemetry.SafeLogDetail(err.Error()))
	})
	defer affinity.Close()
	runner := scheduler.New(plan, scheduler.Dependencies{
		Catalog: catalog, Executor: executors, Logger: compiled.Logger, Metrics: metrics,
		Capacity: capacity,
		Scores:   subsystem.NewScoreStoreWithOptions(time.Now, capacity, subsystem.ScoreStoreOptions{MaxWindow: plan.MaxBalanceWindow()}),
		Leases:   subsystem.NewLeaseStore(time.Now), Affinity: affinity,
		Admission: subsystem.NewAdmissionController(metrics, time.Now),
	})
	defer runner.Close()
	httpServer := &http.Server{
		Addr:              net.JoinHostPort(compiled.Raw.Host, strconv.Itoa(compiled.Raw.Port)),
		Handler:           server.NewServer(compiled, catalog, runner),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	telemetry.LogEvent(context.Background(), compiled.Logger, slog.LevelInfo, "gateway_starting",
		"address", httpServer.Addr,
		"metrics_address", net.JoinHostPort(compiled.Raw.MetricsHost, strconv.Itoa(compiled.Raw.MetricsPort)),
		"log_level", compiled.Raw.LogLevel,
		"providers", len(compiled.Providers),
		"models", len(compiled.LogicalIDs),
	)

	// The metrics listener is a dedicated loopback endpoint (metrics_host /
	// metrics_port, default 127.0.0.1:9209). It serves only /metrics without
	// client authentication; the API listener never exposes it.
	metricsServer := &http.Server{
		Addr:              net.JoinHostPort(compiled.Raw.MetricsHost, strconv.Itoa(compiled.Raw.MetricsPort)),
		Handler:           server.NewMetricsHandler(runner.Metrics()),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	return serve(ctx, httpServer, metricsServer)
}

// Bind all listeners before serving so partial startup cannot leak an API
// listener when another bind fails. Every return path closes both servers.
func serve(ctx context.Context, api, metrics *http.Server) error {
	apiListener, err := net.Listen("tcp", api.Addr)
	if err != nil {
		return err
	}
	defer apiListener.Close()
	metricsListener, err := net.Listen("tcp", metrics.Addr)
	if err != nil {
		return err
	}
	defer metricsListener.Close()
	defer api.Close()
	defer metrics.Close()
	serveErrors := make(chan error, 2)
	go func() { serveErrors <- api.Serve(apiListener) }()
	go func() { serveErrors <- metrics.Serve(metricsListener) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		results := make(chan error, 2)
		go func() { results <- api.Shutdown(shutdownCtx) }()
		go func() { results <- metrics.Shutdown(shutdownCtx) }()
		return errors.Join(<-results, <-results)
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func reportCatalogErrors(logger *slog.Logger, errorsByProvider map[string]error) {
	for providerID, err := range errorsByProvider {
		telemetry.LogEvent(context.Background(), logger, slog.LevelWarn, "catalog_refresh_failed",
			"provider", providerID,
			"detail", telemetry.SafeLogDetail(err.Error()),
		)
	}
}
