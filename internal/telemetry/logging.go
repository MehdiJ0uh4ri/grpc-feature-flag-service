package telemetry

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// traceContextHandler wraps an slog.Handler and, for every log record made
// with a *Context variant (InfoContext, ErrorContext, ...), attaches the
// active span's trace_id/span_id. This is what lets logs and traces be
// correlated in a backend like Grafana/Loki/Tempo without any call-site
// boilerplate.
type traceContextHandler struct {
	slog.Handler
}

func (h *traceContextHandler) Handle(ctx context.Context, record slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, record)
}

func (h *traceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceContextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *traceContextHandler) WithGroup(name string) slog.Handler {
	return &traceContextHandler{Handler: h.Handler.WithGroup(name)}
}

// NewLogger builds a structured JSON logger that automatically injects the
// active OpenTelemetry trace_id/span_id into every log line logged with a
// context (e.g. logger.InfoContext(ctx, ...)).
func NewLogger(levelName, serviceName string) *slog.Logger {
	level := parseLevel(levelName)
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	handler := &traceContextHandler{Handler: base}
	return slog.New(handler).With(slog.String("service", serviceName))
}

func parseLevel(name string) slog.Level {
	switch name {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
