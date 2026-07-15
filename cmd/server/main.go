// Command server runs the FeatureFlagService gRPC server with OpenTelemetry
// tracing, structured logging, Prometheus metrics, and graceful shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	featureflagv1 "github.com/mehdi/feature-flag-service/gen/featureflag/v1"
	"github.com/mehdi/feature-flag-service/internal/config"
	"github.com/mehdi/feature-flag-service/internal/flagstore"
	healthprobe "github.com/mehdi/feature-flag-service/internal/health"
	"github.com/mehdi/feature-flag-service/internal/interceptors"
	"github.com/mehdi/feature-flag-service/internal/metrics"
	"github.com/mehdi/feature-flag-service/internal/server"
	"github.com/mehdi/feature-flag-service/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()
	logger := telemetry.NewLogger(cfg.LogLevel, cfg.ServiceName)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.InitTracerProvider(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	store := flagstore.NewStore()
	flagServer := server.New(store, logger)

	grpcHealth := health.NewServer()
	probe := healthprobe.NewChecker()

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(interceptors.Unary(logger, m)),
		grpc.ChainStreamInterceptor(interceptors.Stream(logger, m)),
	)
	featureflagv1.RegisterFeatureFlagServiceServer(grpcServer, flagServer)
	healthpb.RegisterHealthServer(grpcServer, grpcHealth)
	reflection.Register(grpcServer)
	grpcHealth.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.HandleFunc("/healthz", probe.LivenessHandler())
	mux.HandleFunc("/readyz", probe.ReadinessHandler())
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("grpc server listening", slog.String("addr", cfg.GRPCAddr))
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpc server: %w", err)
		}
	}()
	go func() {
		logger.Info("http server listening", slog.String("addr", cfg.HTTPAddr))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		logger.Error("server error, shutting down", slog.Any("error", err))
	}

	return shutdown(logger, cfg, grpcServer, httpServer, grpcHealth, probe, shutdownTracing)
}

func shutdown(
	logger *slog.Logger,
	cfg config.Config,
	grpcServer *grpc.Server,
	httpServer *http.Server,
	grpcHealth *health.Server,
	probe *healthprobe.Checker,
	shutdownTracing telemetry.Shutdown,
) error {
	// Fail readiness first so load balancers / k8s stop routing new
	// traffic to this instance while we drain what's already in flight.
	probe.SetReady(false)
	grpcHealth.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	grpcStopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcStopped)
	}()

	select {
	case <-grpcStopped:
		logger.Info("grpc server drained in-flight requests")
	case <-ctx.Done():
		logger.Warn("shutdown timeout exceeded, forcing grpc server stop")
		grpcServer.Stop()
	}

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("http server shutdown error", slog.Any("error", err))
	}

	if err := shutdownTracing(ctx); err != nil {
		logger.Error("tracer shutdown error", slog.Any("error", err))
	}

	logger.Info("shutdown complete")
	return nil
}
