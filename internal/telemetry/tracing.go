package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/mehdi/feature-flag-service/internal/config"
)

// Shutdown flushes and stops the tracer provider. Callers should invoke it
// during graceful shutdown so no spans are lost mid-export.
type Shutdown func(context.Context) error

// InitTracerProvider wires up an OpenTelemetry TracerProvider and installs
// it (plus the W3C trace-context/baggage propagators) as the process-wide
// default, which is what lets otelgrpc automatically extract/inject trace
// context on every RPC.
func InitTracerProvider(ctx context.Context, cfg config.Config) (Shutdown, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", cfg.ServiceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}

	var exporter sdktrace.SpanExporter
	switch cfg.TraceExporter {
	case "none":
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
		return func(context.Context) error { return nil }, nil
	case "stdout":
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
	default: // "otlp"
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint)}
		if cfg.OTLPInsecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exporter, err = otlptracegrpc.New(ctx, opts...)
	}
	if err != nil {
		return nil, fmt.Errorf("build trace exporter %q: %w", cfg.TraceExporter, err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.TraceSampleRate))),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp.Shutdown, nil
}
