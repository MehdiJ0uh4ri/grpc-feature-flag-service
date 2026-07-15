// Package health provides Kubernetes-style liveness/readiness HTTP probes
// alongside the standard gRPC health service
// (grpc.health.v1.Health/Check, Watch).
//
// Liveness answers "is the process alive" and should basically never fail
// while the process is running. Readiness answers "can this instance take
// traffic right now" and is what graceful shutdown flips to false during
// the drain window, so a load balancer stops routing new requests while
// in-flight ones finish.
package health

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

type Checker struct {
	ready atomic.Bool
}

func NewChecker() *Checker {
	c := &Checker{}
	c.ready.Store(true)
	return c
}

// SetReady flips the readiness probe. Call SetReady(false) at the start of
// graceful shutdown, before starting to drain connections.
func (c *Checker) SetReady(ready bool) {
	c.ready.Store(ready)
}

func (c *Checker) Ready() bool {
	return c.ready.Load()
}

func (c *Checker) LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeStatus(w, http.StatusOK, "ok")
	}
}

func (c *Checker) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !c.Ready() {
			writeStatus(w, http.StatusServiceUnavailable, "shutting down")
			return
		}
		writeStatus(w, http.StatusOK, "ok")
	}
}

func writeStatus(w http.ResponseWriter, code int, status string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
}
