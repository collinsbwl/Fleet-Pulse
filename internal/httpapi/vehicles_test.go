package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/collinsbwl/Fleet-Pulse/internal/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeSource struct {
	vehicles    []store.Vehicle
	err         error
	sawDeadline bool
}

func (f *fakeSource) Latest(ctx context.Context) ([]store.Vehicle, error) {
	_, f.sawDeadline = ctx.Deadline()
	return f.vehicles, f.err
}

func do(t *testing.T, src VehicleSource, method string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	RegisterVehicles(mux, src, quiet)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(method, "/vehicles", nil))
	return rec
}

func TestVehiclesReturnsAJSONArray(t *testing.T) {
	at := time.Date(2026, 10, 26, 9, 30, 0, 123456000, time.UTC)
	src := &fakeSource{vehicles: []store.Vehicle{
		{ID: "v-00001", EventTime: at, Lat: 37.7749, Lon: -122.4194, SpeedKph: 48.5, FuelPct: 73.25, EngineTempC: 91.5},
		{ID: "v-00002", EventTime: at.Add(time.Second), Lat: 37.8, Lon: -122.4, SpeedKph: 0, FuelPct: 100, EngineTempC: 80},
	}}

	rec := do(t, src, http.MethodGet)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content type: want application/json, got %q", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("cache control: want no-store so the map never shows old positions, got %q", cc)
	}

	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("body is not a JSON array: %v\n%s", err, rec.Body.String())
	}
	if len(got) != 2 {
		t.Fatalf("want 2 vehicles, got %d", len(got))
	}
	first := got[0]
	want := map[string]any{
		"id": "v-00001", "event_time": "2026-10-26T09:30:00.123456Z",
		"lat": 37.7749, "lon": -122.4194,
		"speed_kph": 48.5, "fuel_pct": 73.25, "engine_temp_c": 91.5,
	}
	for k, v := range want {
		if first[k] != v {
			t.Errorf("field %q: want %v, got %v", k, v, first[k])
		}
	}
	if len(first) != len(want) {
		t.Errorf("want exactly %d fields, got %d: %v", len(want), len(first), first)
	}
	if got[1]["id"] != "v-00002" {
		t.Errorf("order should be preserved, got %v second", got[1]["id"])
	}
}

func TestVehiclesEmptyFleetIsAnEmptyArrayNotNull(t *testing.T) {
	for name, vs := range map[string][]store.Vehicle{"empty": {}, "nil": nil} {
		t.Run(name, func(t *testing.T) {
			rec := do(t, &fakeSource{vehicles: vs}, http.MethodGet)

			if rec.Code != http.StatusOK {
				t.Fatalf("want 200, got %d", rec.Code)
			}
			if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
				t.Fatalf("want [] so the map can loop over it, got %q", body)
			}
		})
	}
}

func TestVehiclesErrorHidesTheCause(t *testing.T) {
	src := &fakeSource{err: errors.New("password authentication failed for user fleet at 10.0.0.5")}

	rec := do(t, src, http.MethodGet)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "password") || strings.Contains(rec.Body.String(), "10.0.0.5") {
		t.Errorf("the response must not leak internals: %s", rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
		t.Errorf("want a JSON error message, got %q", rec.Body.String())
	}
}

func TestVehiclesGivesTheQueryADeadline(t *testing.T) {
	src := &fakeSource{}
	do(t, src, http.MethodGet)
	if !src.sawDeadline {
		t.Error("the query should run with a timeout so a stuck database can't hold requests forever")
	}
}

func TestVehiclesOnlyAllowsGET(t *testing.T) {
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		if rec := do(t, &fakeSource{}, m); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: want 405, got %d", m, rec.Code)
		}
	}
}
