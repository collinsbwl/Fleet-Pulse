// Command simulator drives a fleet of simulated vehicles and publishes each
// reading to Redpanda as a Protobuf Position, keyed by vehicle ID.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/collinsbwl/Fleet-Pulse/internal/config"
	"github.com/collinsbwl/Fleet-Pulse/internal/kafka"
	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
	"github.com/collinsbwl/Fleet-Pulse/internal/sim"
	"github.com/collinsbwl/Fleet-Pulse/internal/telemetry"
)

const (
	connectTimeout = 10 * time.Second
	flushTimeout   = 10 * time.Second
	statsEvery     = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("simulator failed", "error", err)
		os.Exit(1)
	}
}

// counters are shared by every vehicle goroutine and the stats loop.
type counters struct {
	sent    atomic.Uint64
	failed  atomic.Uint64
	lastErr atomic.Value // string
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := telemetry.NewLogger(os.Stdout, "simulator", cfg.LogLevel)
	slog.SetDefault(log)

	vehicles := flag.Int("vehicles", cfg.SimVehicles, "number of simulated vehicles")
	rate := flag.Float64("rate", 1/cfg.SimInterval.Seconds(), "readings per second, per vehicle")
	seed := flag.Uint64("seed", 0, "random seed for the fleet (0 picks one from the clock)")
	flag.Parse()

	if *vehicles < 1 {
		return fmt.Errorf("-vehicles must be at least 1, got %d", *vehicles)
	}
	if *rate <= 0 {
		return fmt.Errorf("-rate must be positive, got %g", *rate)
	}
	interval := time.Duration(float64(time.Second) / *rate)
	if *seed == 0 {
		*seed = uint64(time.Now().UnixNano())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	producer, err := kafka.NewProducer(connectCtx, cfg.KafkaBrokers, cfg.KafkaTopic)
	cancel()
	if err != nil {
		return err
	}
	defer producer.Close()

	log.Info("simulator starting",
		"vehicles", *vehicles, "rate_hz", *rate, "interval", interval.String(),
		"seed", *seed, "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic)

	var (
		stats counters
		wg    sync.WaitGroup
	)
	for _, v := range sim.NewFleet(*vehicles, sim.London, *seed) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runVehicle(ctx, v, producer, interval, &stats)
		}()
	}

	logStats(ctx, log, &stats)

	wg.Wait()
	flushCtx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	if err := producer.Flush(flushCtx); err != nil {
		return err
	}
	log.Info("simulator stopped", "sent", stats.sent.Load(), "failed", stats.failed.Load())
	return nil
}

// runVehicle steps one vehicle and publishes a reading every interval until
// ctx is cancelled. Each vehicle starts at a random offset inside the first
// interval so that thousands of vehicles don't all publish in the same instant.
func runVehicle(ctx context.Context, v *sim.Vehicle, p *kafka.Producer, interval time.Duration, stats *counters) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(rand.N(interval)):
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		v.Step(interval)
		value, err := proto.Marshal(toPosition(v, time.Now()))
		if err != nil {
			stats.fail(fmt.Errorf("marshalling position for %s: %w", v.ID, err))
			continue
		}
		p.Produce(ctx, v.ID, value, func(err error) {
			if err != nil {
				stats.fail(err)
				return
			}
			stats.sent.Add(1)
		})
	}
}

// toPosition converts a vehicle's current state into the wire message.
func toPosition(v *sim.Vehicle, at time.Time) *fleetpulsev1.Position {
	return &fleetpulsev1.Position{
		VehicleId:   v.ID,
		EventTime:   timestamppb.New(at),
		Lat:         v.Lat,
		Lon:         v.Lon,
		SpeedKph:    float32(v.SpeedKph),
		FuelPct:     float32(v.FuelPct),
		EngineTempC: float32(v.EngineTempC),
	}
}

// fail records a failed send. A cancelled context just means we are shutting
// down, so it is not counted.
func (c *counters) fail(err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	c.failed.Add(1)
	c.lastErr.Store(err.Error())
}

// logStats logs throughput every statsEvery until ctx is cancelled.
func logStats(ctx context.Context, log *slog.Logger, stats *counters) {
	ticker := time.NewTicker(statsEvery)
	defer ticker.Stop()

	var lastSent, lastFailed uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		sent, failed := stats.sent.Load(), stats.failed.Load()
		args := []any{
			"msgs_per_sec", float64(sent-lastSent) / statsEvery.Seconds(),
			"sent_total", sent,
			"failed_total", failed,
		}
		if failed > lastFailed {
			if msg, ok := stats.lastErr.Load().(string); ok {
				args = append(args, "last_error", msg)
			}
			log.Warn("simulator throughput", args...)
		} else {
			log.Info("simulator throughput", args...)
		}
		lastSent, lastFailed = sent, failed
	}
}
