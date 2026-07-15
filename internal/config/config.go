// Package config loads runtime configuration from environment variables.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName     string
	ServiceVersion  string
	GRPCAddr        string
	HTTPAddr        string
	TraceExporter   string // "otlp", "stdout", or "none"
	OTLPEndpoint    string
	OTLPInsecure    bool
	TraceSampleRate float64
	LogLevel        string
	ShutdownTimeout time.Duration
}

func Load() Config {
	return Config{
		ServiceName:     getEnv("SERVICE_NAME", "feature-flag-service"),
		ServiceVersion:  getEnv("SERVICE_VERSION", "dev"),
		GRPCAddr:        getEnv("GRPC_ADDR", ":50051"),
		HTTPAddr:        getEnv("HTTP_ADDR", ":8080"),
		TraceExporter:   getEnv("OTEL_TRACES_EXPORTER", "otlp"),
		OTLPEndpoint:    getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		OTLPInsecure:    getBool("OTEL_EXPORTER_OTLP_INSECURE", true),
		TraceSampleRate: getFloat("OTEL_TRACE_SAMPLE_RATE", 1.0),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		ShutdownTimeout: getDuration("SHUTDOWN_TIMEOUT", 15*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func getFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

func getDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
