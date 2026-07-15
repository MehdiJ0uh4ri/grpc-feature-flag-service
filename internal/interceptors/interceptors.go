// Package interceptors provides gRPC server interceptors for panic
// recovery, structured request logging, and Prometheus metrics. OpenTelemetry
// tracing is intentionally *not* one of these: it is installed as a
// grpc.StatsHandler (otelgrpc.NewServerHandler()) so that the span is
// already in context by the time these interceptors run.
package interceptors

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mehdi/feature-flag-service/internal/metrics"
)

// Unary chains panic recovery, metrics, and access logging for unary RPCs.
func Unary(logger *slog.Logger, m *metrics.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		method := info.FullMethod
		start := time.Now()

		m.RequestsInFlight.WithLabelValues(method).Inc()
		defer m.RequestsInFlight.WithLabelValues(method).Dec()

		defer func() {
			if r := recover(); r != nil {
				logger.ErrorContext(ctx, "panic recovered in grpc handler",
					slog.String("method", method),
					slog.Any("panic", r),
					slog.String("stack", string(debug.Stack())),
				)
				err = status.Error(codes.Internal, "internal server error")
			}

			code := status.Code(err)
			duration := time.Since(start)
			m.RequestsTotal.WithLabelValues(method, code.String()).Inc()
			m.RequestDuration.WithLabelValues(method, code.String()).Observe(duration.Seconds())

			logFn := logger.InfoContext
			if code != codes.OK {
				logFn = logger.WarnContext
			}
			logFn(ctx, "grpc request completed",
				slog.String("method", method),
				slog.String("code", code.String()),
				slog.Duration("duration", duration),
			)
		}()

		resp, err = handler(ctx, req)
		return resp, err
	}
}

// Stream chains panic recovery, metrics, and access logging for streaming
// RPCs. The whole stream lifetime (open -> close) counts as a single
// observation, which is the right granularity for server-streaming feeds
// like WatchFlags.
func Stream(logger *slog.Logger, m *metrics.Metrics) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		method := info.FullMethod
		ctx := ss.Context()
		start := time.Now()

		m.RequestsInFlight.WithLabelValues(method).Inc()
		defer m.RequestsInFlight.WithLabelValues(method).Dec()

		defer func() {
			if r := recover(); r != nil {
				logger.ErrorContext(ctx, "panic recovered in grpc stream handler",
					slog.String("method", method),
					slog.Any("panic", r),
					slog.String("stack", string(debug.Stack())),
				)
				err = status.Error(codes.Internal, "internal server error")
			}

			code := status.Code(err)
			duration := time.Since(start)
			m.RequestsTotal.WithLabelValues(method, code.String()).Inc()
			m.RequestDuration.WithLabelValues(method, code.String()).Observe(duration.Seconds())

			logFn := logger.InfoContext
			if code != codes.OK {
				logFn = logger.WarnContext
			}
			logFn(ctx, "grpc stream completed",
				slog.String("method", method),
				slog.String("code", code.String()),
				slog.Duration("duration", duration),
			)
		}()

		err = handler(srv, ss)
		return err
	}
}
