// Command server consumes vehicle positions from Redpanda and stores them in
// PostgreSQL. It serves health endpoints; later tasks add the WebSocket and
// HTTP API and metrics.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/collinsbwl/Fleet-Pulse/internal/config"
	"github.com/collinsbwl/Fleet-Pulse/internal/httpapi"
	"github.com/collinsbwl/Fleet-Pulse/internal/kafka"
	"github.com/collinsbwl/Fleet-Pulse/internal/pipeline"
	"github.com/collinsbwl/Fleet-Pulse/internal/store"
	"github.com/collinsbwl/Fleet-Pulse/internal/telemetry"
)

const (
	connectTimeout  = 10 * time.Second
	httpStopTimeout = 3 * time.Second
)

// The pipeline depends on interfaces; these fail the build if the real
// consumer or store stops satisfying them.
var (
	_ pipeline.Source = (*kafka.Consumer)(nil)
	_ pipeline.Sink   = (*store.Store)(nil)
)

func main() {
	if err := run(); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := telemetry.NewLogger(os.Stdout, "server", cfg.LogLevel)
	slog.SetDefault(log)

	// sigCtx ends on SIGTERM or Ctrl-C. runCtx also ends if the HTTP server dies,
	// so a failed listener takes the whole process down instead of leaving a
	// pipeline running with no health endpoint.
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runCtx, cancelRun := context.WithCancel(sigCtx)
	defer cancelRun()

	connectCtx, cancel := context.WithTimeout(sigCtx, connectTimeout)
	defer cancel()
	db, err := store.New(connectCtx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	consumer, err := kafka.NewConsumer(connectCtx, cfg.KafkaBrokers, cfg.KafkaGroup, cfg.KafkaTopic)
	if err != nil {
		return err
	}
	defer consumer.Close()

	health := httpapi.NewHealth(0,
		httpapi.Check{Name: "postgres", Ping: db.Ping},
		httpapi.Check{Name: "redpanda", Ping: consumer.Ping},
	)
	// As soon as shutdown starts, report not-ready so nothing sends us new work.
	context.AfterFunc(sigCtx, health.SetDraining)

	mux := http.NewServeMux()
	health.Register(mux)
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	httpErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			httpErr <- fmt.Errorf("http server: %w", err)
			cancelRun()
		}
	}()

	log.Info("server starting",
		"brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup, "http", cfg.HTTPAddr)

	// Blocks until shutdown. On the way out the pipeline stops reading and makes
	// a final attempt to write and commit its buffered batch.
	p := pipeline.New(consumer, db, log, pipeline.Options{})
	runErr := p.Run(runCtx)

	log.Info("shutting down; stopping http server")
	stopCtx, cancelStop := context.WithTimeout(context.WithoutCancel(sigCtx), httpStopTimeout)
	defer cancelStop()
	if err := srv.Shutdown(stopCtx); err != nil {
		log.Warn("http server did not stop cleanly", "error", err)
	}

	s := p.Stats()
	log.Info("server stopped",
		"written", s.Written, "dropped", s.Dropped, "batches", s.Batches, "write_failures", s.WriteFail)

	select {
	case err := <-httpErr:
		return err
	default:
	}
	return runErr
}
