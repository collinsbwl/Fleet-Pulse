package telemetry

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLoggerWritesJSONWithService(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "simulator", slog.LevelInfo)

	log.Info("batch flushed", "rows", 42)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not one JSON object: %v\n%s", err, buf.String())
	}
	want := map[string]any{
		"level":   "INFO",
		"msg":     "batch flushed",
		"service": "simulator",
		"rows":    float64(42), // JSON numbers decode as float64
	}
	for k, v := range want {
		if rec[k] != v {
			t.Errorf("field %q: want %v, got %v", k, v, rec[k])
		}
	}
	if _, ok := rec["time"]; !ok {
		t.Error("record has no time field")
	}
}

func TestNewLoggerFiltersByLevel(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "server", slog.LevelWarn)

	log.Debug("hidden")
	log.Info("hidden")
	log.Warn("shown")
	log.Error("shown")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 records at warn and above, got %d:\n%s", len(lines), buf.String())
	}
	for _, l := range lines {
		if !strings.Contains(l, `"msg":"shown"`) {
			t.Errorf("unexpected record: %s", l)
		}
	}
}
