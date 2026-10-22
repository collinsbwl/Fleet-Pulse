// Package telemetry sets up logging, and later metrics and tracing, for both
// binaries.
package telemetry

import (
	"io"
	"log/slog"
)

// NewLogger returns a JSON logger that writes to w, drops records below level,
// and tags every record with the service name so logs from different binaries
// can be told apart once they share a backend.
func NewLogger(w io.Writer, service string, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(h).With(slog.String("service", service))
}
