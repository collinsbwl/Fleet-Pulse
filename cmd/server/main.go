// Command server consumes vehicle positions from Redpanda and stores them in
// PostgreSQL. Later tasks add the WebSocket and HTTP API and metrics.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/collinsbwl/Fleet-Pulse/internal/config"
	"github.com/collinsbwl/Fleet-Pulse/internal/kafka"
	"github.com/collinsbwl/Fleet-Pulse/internal/pipeline"
	"github.com/collinsbwl/Fleet-Pulse/internal/store"
	"github.com/collinsbwl/Fleet-Pulse/internal/telemetry"
)

const connectTimeout = 10 * time.Second

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
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

	log.Info("server starting",
		"brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup)

	p := pipeline.New(consumer, db, log, pipeline.Options{})
	if err := p.Run(ctx); err != nil {
		return err
	}

	s := p.Stats()
	log.Info("server stopped",
		"written", s.Written, "dropped", s.Dropped, "batches", s.Batches, "write_failures", s.WriteFail)
	return nil
}
