// Package pipeline moves positions from Redpanda to PostgreSQL with
// at-least-once delivery: a batch is committed back to Redpanda only after it
// has been written, so a crash replays records instead of losing them. Writes
// are idempotent, so replays do no harm.
package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
)

// Source is where records come from; *kafka.Consumer satisfies it.
type Source interface {
	Poll(ctx context.Context, max int) ([]*kgo.Record, error)
	Commit(ctx context.Context, recs []*kgo.Record) error
}

// Sink is where positions are stored; *store.Store satisfies it.
type Sink interface {
	WriteBatch(ctx context.Context, ps []*fleetpulsev1.Position) error
}

// Options tune batching. Zero values take the defaults.
type Options struct {
	MaxBatch     int           // flush once this many records are buffered (default 1000)
	FlushEvery   time.Duration // flush this long after the first buffered record (default 250ms)
	RetryBackoff time.Duration // first wait after a failed write; doubles up to 10s (default 1s)
	CommitWait   time.Duration // time allowed for one commit (default 10s)
	StatsEvery   time.Duration // how often to log throughput (default 5s)

	// WriteTimeout bounds one write attempt. A write already running when
	// shutdown starts is allowed to finish, up to this long (default 5s).
	WriteTimeout time.Duration
	// ShutdownFlush bounds the final flush of the buffered batch (default 5s).
	// Docker waits 10 s after SIGTERM before killing the container.
	ShutdownFlush time.Duration
}

func (o *Options) setDefaults() {
	if o.MaxBatch <= 0 {
		o.MaxBatch = 1000
	}
	if o.FlushEvery <= 0 {
		o.FlushEvery = 250 * time.Millisecond
	}
	if o.RetryBackoff <= 0 {
		o.RetryBackoff = time.Second
	}
	if o.CommitWait <= 0 {
		o.CommitWait = 10 * time.Second
	}
	if o.StatsEvery <= 0 {
		o.StatsEvery = 5 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 5 * time.Second
	}
	if o.ShutdownFlush <= 0 {
		o.ShutdownFlush = 5 * time.Second
	}
}

// Stats counts what the pipeline has done since it started.
type Stats struct {
	Written   uint64 // positions stored
	Dropped   uint64 // records skipped because they could not be decoded or were invalid
	Batches   uint64 // batches written
	WriteFail uint64 // failed write attempts, including ones that later succeeded
}

// Pipeline is one consume → write → commit loop. Run it from one goroutine.
type Pipeline struct {
	src  Source
	sink Sink
	log  *slog.Logger
	opts Options

	stats Stats
}

// New builds a pipeline. It does nothing until Run is called.
func New(src Source, sink Sink, log *slog.Logger, opts Options) *Pipeline {
	opts.setDefaults()
	return &Pipeline{src: src, sink: sink, log: log, opts: opts}
}

// Stats returns the counters. Call it only after Run has returned.
func (p *Pipeline) Stats() Stats { return p.stats }

// Run loops until ctx ends, then stops reading, makes a final attempt to write
// and commit whatever is buffered (bounded by Options.ShutdownFlush), and
// returns nil. A batch that could not be written is left uncommitted, so it is
// redelivered after a restart.
func (p *Pipeline) Run(ctx context.Context) error {
	var (
		recs      []*kgo.Record // every polled record, for the commit
		positions []*fleetpulsev1.Position
		firstAt   time.Time // when the oldest buffered record arrived

		lastReport  = time.Now()
		lastWritten = p.stats.Written
	)

	for {
		// While a batch is waiting, poll only until its flush deadline.
		pollCtx, cancel := ctx, context.CancelFunc(func() {})
		if len(recs) > 0 {
			pollCtx, cancel = context.WithDeadline(ctx, firstAt.Add(p.opts.FlushEvery))
		}
		got, err := p.src.Poll(pollCtx, p.opts.MaxBatch-len(recs))
		cancel()

		stopping := ctx.Err() != nil || errors.Is(err, kgo.ErrClientClosed)
		if err != nil && !stopping && !errors.Is(err, context.DeadlineExceeded) {
			p.log.Warn("fetch problem", "error", err)
			if len(got) == 0 {
				sleep(ctx, 100*time.Millisecond) // don't spin on a persistent error
			}
		}

		// Keep records that arrived even on the way out: the client has already
		// counted them as consumed.
		if len(got) > 0 {
			if len(recs) == 0 {
				firstAt = time.Now()
			}
			recs = append(recs, got...)
			decoded, dropped := decode(p.log, got)
			positions = append(positions, decoded...)
			p.stats.Dropped += uint64(dropped)
		}

		if stopping {
			p.drain(ctx, recs, positions)
			return nil
		}

		if len(recs) >= p.opts.MaxBatch || (len(recs) > 0 && time.Since(firstAt) >= p.opts.FlushEvery) {
			if !p.flush(ctx, recs, positions) {
				return nil // shutting down mid-write; nothing committed
			}
			recs, positions = recs[:0], positions[:0]
		}

		if since := time.Since(lastReport); since >= p.opts.StatsEvery {
			p.log.Info("pipeline throughput",
				"msgs_per_sec", float64(p.stats.Written-lastWritten)/since.Seconds(),
				"written_total", p.stats.Written, "dropped_total", p.stats.Dropped,
				"batches_total", p.stats.Batches, "write_failures_total", p.stats.WriteFail)
			lastReport, lastWritten = time.Now(), p.stats.Written
		}
	}
}

// flush writes the batch and then commits it. A failed write is retried until
// it succeeds, and nothing else is read in the meantime, so a database outage
// shows up as growing lag instead of lost data. It returns false only if ctx
// ended before the batch was written.
func (p *Pipeline) flush(ctx context.Context, recs []*kgo.Record, positions []*fleetpulsev1.Position) bool {
	backoff := p.opts.RetryBackoff
	for {
		start := time.Now()
		// ctx decides whether to try again, not whether a write already under
		// way may finish: cancelling it mid-write would roll the batch back.
		writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), p.opts.WriteTimeout)
		err := p.sink.WriteBatch(writeCtx, positions)
		cancelWrite()
		if err == nil {
			p.stats.Written += uint64(len(positions))
			p.stats.Batches++
			p.log.Debug("batch written",
				"records", len(recs), "positions", len(positions), "write_ms", time.Since(start).Milliseconds())
			break
		}
		if ctx.Err() != nil {
			return false
		}
		p.stats.WriteFail++
		p.log.Warn("write failed; retrying the same batch", "error", err, "records", len(recs), "retry_in", backoff.String())
		if !sleep(ctx, backoff) {
			return false
		}
		backoff = min(backoff*2, 10*time.Second)
	}

	// Commit even the records that were dropped, so one bad message can't
	// hold its partition back forever. A failed commit is not fatal: the batch
	// is redelivered and the idempotent write absorbs it.
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.opts.CommitWait)
	defer cancel()
	if err := p.src.Commit(commitCtx, recs); err != nil {
		p.log.Warn("commit failed; records will be redelivered", "error", err)
	}
	return true
}

// drain is the last step of a shutdown: write and commit what is buffered, within
// Options.ShutdownFlush. If that fails the batch stays uncommitted and is
// redelivered on the next start.
func (p *Pipeline) drain(ctx context.Context, recs []*kgo.Record, positions []*fleetpulsev1.Position) {
	if len(recs) == 0 {
		p.log.Info("shutting down; nothing buffered to flush")
		return
	}
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.opts.ShutdownFlush)
	defer cancel()

	if p.flush(flushCtx, recs, positions) {
		p.log.Info("shutting down; final batch flushed", "records", len(recs), "positions", len(positions))
		return
	}
	p.log.Warn("shutting down; final batch not written and left uncommitted for redelivery", "records", len(recs))
}

// decode turns records into positions. A record that is not valid Protobuf, or
// has no vehicle ID or event time, can never be stored, so it is logged and
// dropped here instead of failing a whole batch later.
func decode(log *slog.Logger, recs []*kgo.Record) (positions []*fleetpulsev1.Position, dropped int) {
	positions = make([]*fleetpulsev1.Position, 0, len(recs))
	for _, r := range recs {
		var p fleetpulsev1.Position
		if err := proto.Unmarshal(r.Value, &p); err != nil {
			log.Warn("dropping undecodable record",
				"partition", r.Partition, "offset", r.Offset, "key", string(r.Key), "error", err)
			dropped++
			continue
		}
		if p.GetVehicleId() == "" || p.GetEventTime() == nil {
			log.Warn("dropping invalid record",
				"partition", r.Partition, "offset", r.Offset, "key", string(r.Key),
				"reason", "missing vehicle id or event time")
			dropped++
			continue
		}
		positions = append(positions, &p)
	}
	return positions, dropped
}

// sleep waits d or until ctx ends, and reports whether the full wait happened.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
