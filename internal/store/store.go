// Package store writes vehicle positions to PostgreSQL.
package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
)

// ErrInvalidPosition is returned for a position with no vehicle ID or no event
// time. Such a position can never be stored, so retrying cannot help.
var ErrInvalidPosition = errors.New("invalid position")

const (
	defaultRetries = 5
	defaultBackoff = 200 * time.Millisecond
	maxBackoff     = 5 * time.Second
)

// Both statements take the same seven column arrays and insert one row per
// array index, so a whole batch costs one statement per table.
//
// History is append-only; a reading seen twice (Redpanda redelivers after a
// crash) is skipped. Latest keeps one row per vehicle and only moves forward in
// time, so a late or replayed reading can never roll a vehicle back.
const (
	historySQL = `
INSERT INTO positions_history (vehicle_id, event_ts, lat, lon, speed_kph, fuel_pct, engine_temp_c)
SELECT * FROM unnest($1::text[], $2::timestamptz[], $3::float8[], $4::float8[], $5::real[], $6::real[], $7::real[])
ON CONFLICT (vehicle_id, event_ts) DO NOTHING`

	latestSQL = `
INSERT INTO vehicles_latest (vehicle_id, event_ts, lat, lon, speed_kph, fuel_pct, engine_temp_c)
SELECT * FROM unnest($1::text[], $2::timestamptz[], $3::float8[], $4::float8[], $5::real[], $6::real[], $7::real[])
ON CONFLICT (vehicle_id) DO UPDATE SET
    event_ts      = EXCLUDED.event_ts,
    lat           = EXCLUDED.lat,
    lon           = EXCLUDED.lon,
    speed_kph     = EXCLUDED.speed_kph,
    fuel_pct      = EXCLUDED.fuel_pct,
    engine_temp_c = EXCLUDED.engine_temp_c,
    updated_at    = now()
WHERE EXCLUDED.event_ts > vehicles_latest.event_ts`
)

// Store is a connection pool plus the batch writer. It is safe for concurrent use.
type Store struct {
	pool    *pgxpool.Pool
	retries int           // extra attempts after the first
	backoff time.Duration // delay before the first retry; doubles each time

	// write performs one attempt. It is a field so tests can inject failures.
	write func(ctx context.Context, history, latest *columns) error
}

// New opens a pool and checks the database is reachable, so a wrong DSN fails at
// startup instead of on the first write.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("creating postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("reaching postgres: %w", err)
	}
	s := &Store{pool: pool, retries: defaultRetries, backoff: defaultBackoff}
	s.write = s.writeOnce
	return s, nil
}

// Ping reports whether the database answers.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close closes every pooled connection.
func (s *Store) Close() {
	s.pool.Close()
}

// WriteBatch stores a batch of positions in one transaction: every reading goes
// to positions_history and the newest reading per vehicle goes to
// vehicles_latest. Writing the same batch again changes nothing, which is what
// makes at-least-once delivery safe.
//
// Connection-level failures are retried with exponential backoff. If any
// position is invalid, nothing is written and ErrInvalidPosition is returned.
func (s *Store) WriteBatch(ctx context.Context, ps []*fleetpulsev1.Position) error {
	if len(ps) == 0 {
		return nil
	}
	history, latest, err := buildColumns(ps)
	if err != nil {
		return err
	}

	delay := s.backoff
	for attempt := 0; ; attempt++ {
		err = s.write(ctx, history, latest)
		if err == nil {
			return nil
		}
		if attempt >= s.retries || ctx.Err() != nil || !retryable(err) {
			return fmt.Errorf("writing batch of %d positions (attempt %d): %w", len(ps), attempt+1, err)
		}

		// Wait delay/2 to delay, so concurrent writers don't retry in lockstep.
		wait := delay/2 + rand.N(delay/2+1)
		select {
		case <-ctx.Done():
			return fmt.Errorf("writing batch of %d positions: %w", len(ps), errors.Join(err, ctx.Err()))
		case <-time.After(wait):
		}
		delay = min(delay*2, maxBackoff)
	}
}

// writeOnce runs both inserts in one transaction and one round trip.
func (s *Store) writeOnce(ctx context.Context, history, latest *columns) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	// After Commit this is a no-op; after a failure it releases the connection.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	batch := &pgx.Batch{}
	batch.Queue(historySQL, history.args()...)
	batch.Queue(latestSQL, latest.args()...)

	results := tx.SendBatch(ctx, batch)
	for range 2 {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return err
		}
	}
	if err := results.Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// retryable reports whether an error is worth another attempt: the connection
// dropped, the server is shutting down or restarting, or the transaction lost a
// race. Anything else (bad SQL, a constraint violation) would fail again.
func retryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if pgconn.SafeToRetry(err) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57P01", "57P02", "57P03", // admin shutdown, crash shutdown, cannot connect now
			"40001", "40P01": // serialization failure, deadlock
			return true
		}
		return strings.HasPrefix(pgErr.Code, "08") // connection exception class
	}

	var netErr net.Error
	return errors.As(err, &netErr) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// columns holds a batch as one array per column, ready to pass to unnest.
type columns struct {
	ids   []string
	times []time.Time
	lat   []float64
	lon   []float64
	speed []float32
	fuel  []float32
	temp  []float32
}

func (c *columns) add(p *fleetpulsev1.Position) {
	c.ids = append(c.ids, p.GetVehicleId())
	c.times = append(c.times, p.GetEventTime().AsTime())
	c.lat = append(c.lat, p.GetLat())
	c.lon = append(c.lon, p.GetLon())
	c.speed = append(c.speed, p.GetSpeedKph())
	c.fuel = append(c.fuel, p.GetFuelPct())
	c.temp = append(c.temp, p.GetEngineTempC())
}

func (c *columns) args() []any {
	return []any{c.ids, c.times, c.lat, c.lon, c.speed, c.fuel, c.temp}
}

// buildColumns validates ps and splits it into the history columns (every
// reading) and the latest columns (the newest reading per vehicle). Latest must
// hold each vehicle once, because one INSERT cannot update the same row twice.
func buildColumns(ps []*fleetpulsev1.Position) (history, latest *columns, err error) {
	newest := make(map[string]int, len(ps)) // vehicle ID -> index of its newest reading
	for i, p := range ps {
		if p.GetVehicleId() == "" || p.GetEventTime() == nil {
			return nil, nil, fmt.Errorf("%w: position %d has no vehicle id or no event time", ErrInvalidPosition, i)
		}
		if j, seen := newest[p.GetVehicleId()]; !seen || !p.GetEventTime().AsTime().Before(ps[j].GetEventTime().AsTime()) {
			newest[p.GetVehicleId()] = i
		}
	}

	history, latest = &columns{}, &columns{}
	for i, p := range ps {
		history.add(p)
		if newest[p.GetVehicleId()] == i {
			latest.add(p)
		}
	}
	return history, latest, nil
}
