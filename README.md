# feature-flag-service

A production-pattern gRPC service in Go, built to demonstrate the
observability and lifecycle patterns SRE/platform teams screen for:
OpenTelemetry tracing, structured logging correlated to traces, Prometheus
metrics, graceful shutdown, and Kubernetes-style health probes.

The domain is a feature flag service: create/update/delete flags, evaluate
a flag for a given subject (deterministic sticky rollout by percentage,
plus explicit targeting rules), and watch flag changes as a server-streamed
feed.

## What's inside

| Concern | Implementation |
|---|---|
| RPC | gRPC over protobuf, defined in [proto/featureflag/v1/featureflag.proto](proto/featureflag/v1/featureflag.proto), generated with `buf` |
| Tracing | OpenTelemetry SDK + `otelgrpc` stats handler (server *and* client); OTLP or stdout exporter |
| Logging | `log/slog` JSON logs; every context-aware log line carries `trace_id`/`span_id` |
| Metrics | `prometheus/client_golang`: request count, latency histogram, and in-flight gauge, all labeled by method + status code |
| Graceful shutdown | `SIGINT`/`SIGTERM` → flip readiness false → `grpc.Server.GracefulStop()` (with a hard-stop timeout) → drain HTTP → flush traces |
| Health | `grpc.health.v1.Health` service (for gRPC-aware load balancers) + `/healthz` and `/readyz` HTTP probes (for k8s) |

## Project layout

```
proto/featureflag/v1/       proto source
gen/featureflag/v1/         generated pb.go / grpc.pb.go (checked in, via buf)
internal/config/            env-driven configuration
internal/telemetry/         tracer provider + trace-aware slog handler
internal/metrics/           Prometheus collectors
internal/interceptors/      unary/stream interceptors: recovery, logging, metrics
internal/flagstore/         domain logic: in-memory store, evaluation, pub/sub
internal/server/            gRPC service implementation (proto <-> domain)
internal/health/            liveness/readiness HTTP handlers
cmd/server/                 main: wires everything together
cmd/client/                 demo CLI client showing trace propagation
```

## Prerequisites

- Go 1.23+ (not installed in the environment this was scaffolded in —
  install it, e.g. `scoop install go` on Windows)
- [buf](https://buf.build) CLI (already used to generate `gen/`; only
  needed again if you edit the `.proto`)
- Docker + Docker Compose, if you want the full observability stack
  (Jaeger, Prometheus, Grafana)

## First-time setup

```sh
go mod tidy   # resolves indirect deps and writes go.sum (required — none is checked in)
```

If you change `proto/featureflag/v1/featureflag.proto`, regenerate stubs with:

```sh
buf generate
```

## Running locally

```sh
go run ./cmd/server
```

This starts:
- gRPC on `:50051`
- HTTP (metrics + probes) on `:8080` → `/metrics`, `/healthz`, `/readyz`

By default traces export via OTLP/gRPC to `localhost:4317`. If you don't
have a collector running, set `OTEL_TRACES_EXPORTER=stdout` to print spans
to stdout instead, or `OTEL_TRACES_EXPORTER=none` to disable tracing.

In another terminal, run the demo client (creates a flag, evaluates it for
a few subjects, watches for changes):

```sh
go run ./cmd/client
```

Or use `grpcurl` (reflection is enabled):

```sh
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -d '{"key":"new-ui","enabled":true,"rollout_percentage":50}' \
  localhost:50051 featureflag.v1.FeatureFlagService/CreateFlag
```

### Graceful shutdown

Send `SIGTERM` (or Ctrl+C) to the server process. It will:
1. Mark `/readyz` unhealthy and flip the gRPC health status to
   `NOT_SERVING` immediately, so load balancers stop sending new traffic.
2. Call `GracefulStop()`, letting in-flight RPCs (including open
   `WatchFlags` streams) finish, up to `SHUTDOWN_TIMEOUT` (default 15s),
   after which it force-stops.
3. Shut down the HTTP server and flush any buffered trace spans.

## Running the full observability stack

```sh
docker compose up --build
```

This brings up the service plus an OTel Collector, Jaeger (trace UI on
[localhost:16686](http://localhost:16686)), Prometheus
([localhost:9090](http://localhost:9090)), and Grafana
([localhost:3000](http://localhost:3000), anonymous admin access).

## Configuration

All configuration is via environment variables (see
[internal/config/config.go](internal/config/config.go)):

| Variable | Default | Purpose |
|---|---|---|
| `SERVICE_NAME` | `feature-flag-service` | Resource attribute for traces/logs |
| `GRPC_ADDR` | `:50051` | gRPC listen address |
| `HTTP_ADDR` | `:8080` | HTTP listen address (metrics + probes) |
| `OTEL_TRACES_EXPORTER` | `otlp` | `otlp`, `stdout`, or `none` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTLP/gRPC collector endpoint |
| `OTEL_EXPORTER_OTLP_INSECURE` | `true` | Skip TLS to the collector (local dev) |
| `OTEL_TRACE_SAMPLE_RATE` | `1.0` | Parent-based trace-id-ratio sampler |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` | `15s` | Hard-stop deadline during graceful shutdown |

## Notes / production caveats

- The flag store is in-memory and single-process; a real deployment would
  back it with a database and the `WatchFlags` feed with something durable
  (outbox table, Kafka, etc.) instead of best-effort in-memory pub/sub.
- gRPC reflection is enabled unconditionally for easy `grpcurl` exploration;
  consider gating it behind an env var in a real production build.
- `go.sum` isn't checked in since this environment has no Go toolchain to
  generate it — run `go mod tidy` before your first build.
