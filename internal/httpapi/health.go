// Package httpapi holds the server's HTTP handlers.
package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

const defaultCheckTimeout = 2 * time.Second

// Check is one dependency the server needs in order to do its job.
type Check struct {
	Name string
	Ping func(ctx context.Context) error
}

// Health serves the liveness and readiness endpoints.
//
//	GET /healthz  200 as long as the process is running. It checks nothing else,
//	              so a database outage never gets the process killed for it.
//	GET /readyz   200 when every Check passes, 503 when one fails or the server
//	              is shutting down.
type Health struct {
	checks   []Check
	timeout  time.Duration
	draining atomic.Bool
}

// NewHealth builds the handlers. Each check gets timeout to answer (2 s if
// timeout is zero or negative); a check that hangs counts as failed.
func NewHealth(timeout time.Duration, checks ...Check) *Health {
	if timeout <= 0 {
		timeout = defaultCheckTimeout
	}
	return &Health{checks: checks, timeout: timeout}
}

// SetDraining makes /readyz report 503 from now on. Call it when shutdown
// begins, so whatever routes traffic to this server stops sending it more.
func (h *Health) SetDraining() { h.draining.Store(true) }

// Register adds the two endpoints to mux.
func (h *Health) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.live)
	mux.HandleFunc("GET /readyz", h.ready)
}

type readiness struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

func (h *Health) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Health) ready(w http.ResponseWriter, r *http.Request) {
	if h.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, readiness{Status: "shutting down"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// Run the checks side by side so a slow one doesn't delay the others.
	type outcome struct {
		i   int
		err error
	}
	done := make(chan outcome, len(h.checks)) // buffered: a late finisher never blocks
	for i, c := range h.checks {
		go func() { done <- outcome{i, c.Ping(ctx)} }()
	}

	// Stop waiting when the deadline passes. Some clients (Kafka's) don't return
	// promptly when their context ends, and one stuck dependency must not make
	// the whole endpoint hang; the straggler finishes in the background.
	results := make([]string, len(h.checks))
	pending := len(h.checks)
	for pending > 0 {
		select {
		case o := <-done:
			results[o.i] = "ok"
			if o.err != nil {
				results[o.i] = o.err.Error()
			}
			pending--
		case <-ctx.Done():
			for i := range results {
				if results[i] == "" {
					results[i] = "timed out after " + h.timeout.String()
				}
			}
			pending = 0
		}
	}

	out := readiness{Status: "ok", Checks: make(map[string]string, len(h.checks))}
	code := http.StatusOK
	for i, c := range h.checks {
		out.Checks[c.Name] = results[i]
		if results[i] != "ok" {
			out.Status = "unavailable"
			code = http.StatusServiceUnavailable
		}
	}
	writeJSON(w, code, out)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
