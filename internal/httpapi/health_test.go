package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func ok(context.Context) error { return nil }

func fail(msg string) func(context.Context) error {
	return func(context.Context) error { return errors.New(msg) }
}

func get(t *testing.T, h *Health, path string) (int, readiness, string) {
	t.Helper()
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

	var body readiness
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s did not return JSON: %v\n%s", path, err, rec.Body.String())
	}
	return rec.Code, body, rec.Header().Get("Content-Type")
}

func TestHealthzIsOKEvenWhenDependenciesAreDown(t *testing.T) {
	h := NewHealth(time.Second, Check{"postgres", fail("connection refused")})

	code, body, ctype := get(t, h, "/healthz")

	if code != http.StatusOK || body.Status != "ok" {
		t.Fatalf("liveness must not depend on dependencies: got %d %+v", code, body)
	}
	if ctype != "application/json" {
		t.Errorf("content type: want application/json, got %q", ctype)
	}
}

func TestReadyzAllChecksPass(t *testing.T) {
	h := NewHealth(time.Second, Check{"postgres", ok}, Check{"redpanda", ok})

	code, body, _ := get(t, h, "/readyz")

	if code != http.StatusOK || body.Status != "ok" {
		t.Fatalf("want 200 ok, got %d %+v", code, body)
	}
	if body.Checks["postgres"] != "ok" || body.Checks["redpanda"] != "ok" {
		t.Errorf("both checks should be reported ok, got %v", body.Checks)
	}
}

func TestReadyzFailsAndNamesTheBrokenDependency(t *testing.T) {
	h := NewHealth(time.Second, Check{"postgres", fail("connection refused")}, Check{"redpanda", ok})

	code, body, _ := get(t, h, "/readyz")

	if code != http.StatusServiceUnavailable || body.Status != "unavailable" {
		t.Fatalf("want 503 unavailable, got %d %+v", code, body)
	}
	if body.Checks["postgres"] != "connection refused" {
		t.Errorf("the failing check should carry its error, got %q", body.Checks["postgres"])
	}
	if body.Checks["redpanda"] != "ok" {
		t.Errorf("the healthy check should still be reported ok, got %q", body.Checks["redpanda"])
	}
}

func TestReadyzTreatsAHangingCheckAsFailed(t *testing.T) {
	hang := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	h := NewHealth(50*time.Millisecond, Check{"postgres", hang})

	start := time.Now()
	code, body, _ := get(t, h, "/readyz")

	if code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d %+v", code, body)
	}
	if time.Since(start) > time.Second {
		t.Errorf("a hanging check must be cut off by the timeout, took %v", time.Since(start))
	}
}

func TestReadyzRunsChecksConcurrently(t *testing.T) {
	slow := func(context.Context) error { time.Sleep(150 * time.Millisecond); return nil }
	h := NewHealth(time.Second, Check{"a", slow}, Check{"b", slow}, Check{"c", slow})

	start := time.Now()
	code, _, _ := get(t, h, "/readyz")

	if code != http.StatusOK {
		t.Fatalf("want 200, got %d", code)
	}
	if time.Since(start) > 350*time.Millisecond {
		t.Errorf("three 150 ms checks should overlap, took %v", time.Since(start))
	}
}

func TestReadyzReportsShuttingDownEvenIfChecksPass(t *testing.T) {
	h := NewHealth(time.Second, Check{"postgres", ok})
	h.SetDraining()

	code, body, _ := get(t, h, "/readyz")

	if code != http.StatusServiceUnavailable || body.Status != "shutting down" {
		t.Fatalf("want 503 shutting down, got %d %+v", code, body)
	}
	if code, _, _ := get(t, h, "/healthz"); code != http.StatusOK {
		t.Errorf("liveness should stay 200 while draining, got %d", code)
	}
}

func TestReadyzWithNoChecksIsReady(t *testing.T) {
	code, body, _ := get(t, NewHealth(0), "/readyz")
	if code != http.StatusOK || body.Status != "ok" {
		t.Fatalf("want 200 ok, got %d %+v", code, body)
	}
}

func TestOnlyGETIsAllowed(t *testing.T) {
	mux := http.NewServeMux()
	NewHealth(time.Second).Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/readyz", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST should be rejected with 405, got %d", rec.Code)
	}
}

// Regression: a check that ignores its context (the Kafka client's Ping does
// when a broker is frozen) used to hang /readyz far past the timeout, because
// the handler waited for every check to return.
func TestReadyzDoesNotWaitForACheckThatIgnoresItsContext(t *testing.T) {
	stuck := func(context.Context) error {
		time.Sleep(3 * time.Second) // never looks at ctx
		return nil
	}
	h := NewHealth(50*time.Millisecond, Check{"redpanda", stuck}, Check{"postgres", ok})

	start := time.Now()
	code, body, _ := get(t, h, "/readyz")

	if time.Since(start) > time.Second {
		t.Fatalf("/readyz must answer within its timeout even if a check hangs, took %v", time.Since(start))
	}
	if code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d %+v", code, body)
	}
	if body.Checks["redpanda"] == "ok" || body.Checks["redpanda"] == "" {
		t.Errorf("the stuck check should be reported as timed out, got %q", body.Checks["redpanda"])
	}
	if body.Checks["postgres"] != "ok" {
		t.Errorf("a check that finished in time should keep its result, got %q", body.Checks["postgres"])
	}
}
