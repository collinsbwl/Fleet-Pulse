// Command server consumes vehicle positions from Redpanda. In this first cut it
// only decodes and logs them; later tasks add Postgres, WebSocket and metrics.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/collinsbwl/Fleet-Pulse/internal/config"
	"github.com/collinsbwl/Fleet-Pulse/internal/kafka"
	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
	"github.com/collinsbwl/Fleet-Pulse/internal/telemetry"
)

const (
	connectTimeout = 10 * time.Second
	commitTimeout  = 10 * time.Second
	maxPoll        = 1000
	statsEvery     = 5 * time.Second
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
	consumer, err := kafka.NewConsumer(connectCtx, cfg.KafkaBrokers, cfg.KafkaGroup, cfg.KafkaTopic)
	cancel()
	if err != nil {
		return err
	}
	defer consumer.Close()

	log.Info("server starting",
		"brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup)

	var (
		total, bad uint64
		lastTotal  uint64
		lastReport = time.Now()
	)
	for {
		recs, err := consumer.Poll(ctx, maxPoll)
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, kgo.ErrClientClosed):
			log.Info("server stopped", "consumed", total, "undecodable", bad)
			return nil
		case err != nil:
			log.Warn("fetch problem", "error", err)
		}
		if len(recs) == 0 {
			continue
		}

		positions, undecodable := decodeBatch(recs)
		total += uint64(len(positions))
		bad += uint64(undecodable)
		log.Debug("batch consumed",
			"records", len(recs), "decoded", len(positions), "undecodable", undecodable,
			"partitions", partitionsOf(recs))

		// Commit after the batch is handled. An undecodable record is logged and
		// skipped, then committed with the rest, so one bad message can't block
		// its partition forever.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		err = consumer.Commit(commitCtx, recs)
		cancel()
		if err != nil {
			log.Warn("commit failed; records will be redelivered", "error", err)
		}

		if since := time.Since(lastReport); since >= statsEvery {
			log.Info("server throughput",
				"msgs_per_sec", float64(total-lastTotal)/since.Seconds(),
				"consumed_total", total, "undecodable_total", bad)
			lastTotal, lastReport = total, time.Now()
		}
	}
}

// decodeBatch turns raw records into positions. Records that don't decode are
// counted and skipped; the caller decides what that means.
func decodeBatch(recs []*kgo.Record) (positions []*fleetpulsev1.Position, undecodable int) {
	positions = make([]*fleetpulsev1.Position, 0, len(recs))
	for _, r := range recs {
		var p fleetpulsev1.Position
		if err := proto.Unmarshal(r.Value, &p); err != nil {
			slog.Warn("undecodable record",
				"partition", r.Partition, "offset", r.Offset, "key", string(r.Key), "error", err)
			undecodable++
			continue
		}
		positions = append(positions, &p)
	}
	return positions, undecodable
}

// partitionsOf lists the distinct partitions in a batch, in order.
func partitionsOf(recs []*kgo.Record) []string {
	seen := make(map[int32]struct{})
	var parts []int32
	for _, r := range recs {
		if _, ok := seen[r.Partition]; !ok {
			seen[r.Partition] = struct{}{}
			parts = append(parts, r.Partition)
		}
	}
	slices.Sort(parts)

	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = fmt.Sprint(p)
	}
	return out
}
