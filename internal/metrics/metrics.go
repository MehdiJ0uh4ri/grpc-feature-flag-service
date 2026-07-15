// Package metrics defines the Prometheus instrumentation exposed by the
// gRPC server: request counts (with a "code" label so error rate is just a
// PromQL ratio), and per-method latency histograms.
package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	RequestsTotal    *prometheus.CounterVec
	RequestDuration  *prometheus.HistogramVec
	RequestsInFlight *prometheus.GaugeVec
}

// New creates the metric collectors and registers them against reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_server_requests_total",
			Help: "Total number of gRPC requests processed, labeled by method and status code.",
		}, []string{"method", "code"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "grpc_server_request_duration_seconds",
			Help:    "Latency of gRPC requests in seconds, labeled by method and status code.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "code"}),
		RequestsInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "grpc_server_requests_in_flight",
			Help: "Number of gRPC requests currently being served, labeled by method.",
		}, []string{"method"}),
	}
	reg.MustRegister(m.RequestsTotal, m.RequestDuration, m.RequestsInFlight)
	return m
}
