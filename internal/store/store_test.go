package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/collinsbwl/Fleet-Pulse/internal/config"
	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
)

var t0 = time.Date(2026, 10, 25, 9, 0, 0, 0, time.UTC)

func pos(id string, at time.Time, lat float64) *fleetpulsev1.Position {
	return &fleetpulsev1.Position{
		VehicleId: id, EventTime: timestamppb.New(at),
		Lat: lat, Lon: -0.12, SpeedKph: 40, FuelPct: 80, EngineTempC: 90,
	}
}

// ---- integration tests: need `make up`, skip when Postgres isn't reachable ----

// testStore connects to the Compose Postgres and removes this test's rows when
// the test ends. It returns the prefix that makes this test's vehicle IDs unique.
func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s, err := New(ctx, config.Defaults().PostgresDSN)
	if err != nil {
		t.Skipf("Postgres not reachable (run `make up`): %v", err)
	}
	prefix := fmt.Sprintf("t09-%d-", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, table := range []string{"positions_history", "vehicles_latest"} {
			_, _ = s.pool.Exec(context.Background(), "DELETE FROM "+table+" WHERE vehicle_id LIKE $1", prefix+"%")
		}
		s.Close()
	})
	return s, prefix
}

func count(t *testing.T, s *Store, table, prefix string) int {
	t.Helper()
	var n int
	err := s.pool.QueryRow(context.Background(),
		"SELECT count(*) FROM "+table+" WHERE vehicle_id LIKE $1", prefix+"%").Scan(&n)
	if err != nil {
		t.Fatalf("counting %s: %v", table, err)
	}
	return n
}

func latestOf(t *testing.T, s *Store, id string) (at time.Time, lat float64) {
	t.Helper()
	err := s.pool.QueryRow(context.Background(),
		"SELECT event_ts, lat FROM vehicles_latest WHERE vehicle_id = $1", id).Scan(&at, &lat)
	if err != nil {
		t.Fatalf("reading latest for %s: %v", id, err)
	}
	return at.UTC(), lat
}

func fleetBatch(prefix string, n int, at time.Time) []*fleetpulsev1.Position {
	ps := make([]*fleetpulsev1.Position, n)
	for i := range ps {
		ps[i] = pos(fmt.Sprintf("%s%05d", prefix, i), at, 51.5)
	}
	return ps
}

func TestWriteBatchTwoRounds(t *testing.T) {
	s, prefix := testStore(t)
	ctx := context.Background()

	// 1,000 vehicles report, then report again a second later.
	if err := s.WriteBatch(ctx, fleetBatch(prefix, 1000, t0)); err != nil {
		t.Fatalf("first batch: %v", err)
	}
	if err := s.WriteBatch(ctx, fleetBatch(prefix, 1000, t0.Add(time.Second))); err != nil {
		t.Fatalf("second batch: %v", err)
	}

	if got := count(t, s, "vehicles_latest", prefix); got != 1000 {
		t.Errorf("vehicles_latest: want 1000 rows, got %d", got)
	}
	if got := count(t, s, "positions_history", prefix); got != 2000 {
		t.Errorf("positions_history: want 2000 rows, got %d", got)
	}
	if at, _ := latestOf(t, s, prefix+"00000"); !at.Equal(t0.Add(time.Second)) {
		t.Errorf("latest should be the second reading, got %v", at)
	}
}

func TestWriteBatchRedeliveryIsHarmless(t *testing.T) {
	s, prefix := testStore(t)
	ctx := context.Background()
	batch := fleetBatch(prefix, 100, t0)

	for range 3 {
		if err := s.WriteBatch(ctx, batch); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	if got := count(t, s, "positions_history", prefix); got != 100 {
		t.Errorf("history after 3 identical writes: want 100 rows, got %d", got)
	}
	if got := count(t, s, "vehicles_latest", prefix); got != 100 {
		t.Errorf("latest after 3 identical writes: want 100 rows, got %d", got)
	}
}

func TestWriteBatchLateReadingDoesNotRollBack(t *testing.T) {
	s, prefix := testStore(t)
	ctx := context.Background()
	id := prefix + "late"

	if err := s.WriteBatch(ctx, []*fleetpulsev1.Position{pos(id, t0.Add(10*time.Second), 52.0)}); err != nil {
		t.Fatalf("newer write: %v", err)
	}
	if err := s.WriteBatch(ctx, []*fleetpulsev1.Position{pos(id, t0, 51.0)}); err != nil {
		t.Fatalf("older write: %v", err)
	}

	at, lat := latestOf(t, s, id)
	if !at.Equal(t0.Add(10*time.Second)) || lat != 52.0 {
		t.Errorf("latest rolled back to %v lat %v", at, lat)
	}
	if got := count(t, s, "positions_history", id); got != 2 {
		t.Errorf("history should keep both readings, got %d", got)
	}
}

func TestWriteBatchSameVehicleTwiceInOneBatch(t *testing.T) {
	s, prefix := testStore(t)
	id := prefix + "dup"

	// Out of time order on purpose: the newest reading is in the middle.
	err := s.WriteBatch(context.Background(), []*fleetpulsev1.Position{
		pos(id, t0.Add(1*time.Second), 51.1),
		pos(id, t0.Add(3*time.Second), 51.3),
		pos(id, t0.Add(2*time.Second), 51.2),
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	if at, lat := latestOf(t, s, id); !at.Equal(t0.Add(3*time.Second)) || lat != 51.3 {
		t.Errorf("latest should be the newest reading, got %v lat %v", at, lat)
	}
	if got := count(t, s, "positions_history", id); got != 3 {
		t.Errorf("history: want 3 rows, got %d", got)
	}
}

func TestWriteBatchRejectsInvalidAndWritesNothing(t *testing.T) {
	s, prefix := testStore(t)
	good := pos(prefix+"ok", t0, 51.5)

	for name, bad := range map[string]*fleetpulsev1.Position{
		"no vehicle id": pos("", t0, 51.5),
		"no event time": {VehicleId: prefix + "notime"},
	} {
		t.Run(name, func(t *testing.T) {
			err := s.WriteBatch(context.Background(), []*fleetpulsev1.Position{good, bad})
			if !errors.Is(err, ErrInvalidPosition) {
				t.Fatalf("want ErrInvalidPosition, got %v", err)
			}
			if got := count(t, s, "positions_history", prefix); got != 0 {
				t.Errorf("a batch with an invalid position should write nothing, got %d rows", got)
			}
		})
	}
}

// ---- unit tests: no database needed ----

func TestWriteBatchEmpty(t *testing.T) {
	s := &Store{write: func(context.Context, *columns, *columns) error {
		t.Fatal("write should not be called for an empty batch")
		return nil
	}}
	if err := s.WriteBatch(context.Background(), nil); err != nil {
		t.Fatalf("want nil, got %v", err)
	}
}

func TestBuildColumnsKeepsNewestPerVehicle(t *testing.T) {
	history, latest, err := buildColumns([]*fleetpulsev1.Position{
		pos("a", t0.Add(2*time.Second), 2),
		pos("b", t0, 5),
		pos("a", t0.Add(1*time.Second), 1),
		pos("a", t0.Add(3*time.Second), 3),
	})
	if err != nil {
		t.Fatalf("buildColumns: %v", err)
	}
	if len(history.ids) != 4 {
		t.Errorf("history: want all 4 readings, got %d", len(history.ids))
	}
	if len(latest.ids) != 2 {
		t.Fatalf("latest: want one row per vehicle (2), got %d", len(latest.ids))
	}
	for i, id := range latest.ids {
		if id == "a" && latest.lat[i] != 3 {
			t.Errorf("latest for a: want lat 3 (newest), got %v", latest.lat[i])
		}
	}
}

func fastRetryStore(write func(context.Context, *columns, *columns) error) *Store {
	return &Store{retries: 3, backoff: time.Millisecond, write: write}
}

var connDown = &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"}

func TestWriteBatchRetriesThenSucceeds(t *testing.T) {
	calls := 0
	s := fastRetryStore(func(context.Context, *columns, *columns) error {
		calls++
		if calls < 3 {
			return connDown
		}
		return nil
	})

	if err := s.WriteBatch(context.Background(), []*fleetpulsev1.Position{pos("a", t0, 1)}); err != nil {
		t.Fatalf("want success on the third attempt, got %v", err)
	}
	if calls != 3 {
		t.Errorf("want 3 attempts, got %d", calls)
	}
}

func TestWriteBatchGivesUpAfterRetries(t *testing.T) {
	calls := 0
	s := fastRetryStore(func(context.Context, *columns, *columns) error {
		calls++
		return connDown
	})

	err := s.WriteBatch(context.Background(), []*fleetpulsev1.Position{pos("a", t0, 1)})
	if !errors.Is(err, connDown) {
		t.Fatalf("want the underlying error, got %v", err)
	}
	if calls != 4 { // first attempt + 3 retries
		t.Errorf("want 4 attempts, got %d", calls)
	}
}

func TestWriteBatchDoesNotRetryPermanentErrors(t *testing.T) {
	calls := 0
	s := fastRetryStore(func(context.Context, *columns, *columns) error {
		calls++
		return &pgconn.PgError{Code: "23505", Message: "duplicate key"} // unique violation
	})

	if err := s.WriteBatch(context.Background(), []*fleetpulsev1.Position{pos("a", t0, 1)}); err == nil {
		t.Fatal("want an error")
	}
	if calls != 1 {
		t.Errorf("a permanent error should not be retried, got %d attempts", calls)
	}
}

func TestWriteBatchStopsRetryingWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	s := &Store{retries: 10, backoff: time.Hour, write: func(context.Context, *columns, *columns) error {
		calls++
		cancel() // the caller gives up while we are about to back off
		return connDown
	}}

	start := time.Now()
	err := s.WriteBatch(ctx, []*fleetpulsev1.Position{pos("a", t0, 1)})
	if err == nil {
		t.Fatal("want an error")
	}
	if time.Since(start) > time.Second {
		t.Errorf("should return promptly on cancellation, took %v", time.Since(start))
	}
	if calls != 1 {
		t.Errorf("want 1 attempt, got %d", calls)
	}
}

func TestRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"server shutting down", &pgconn.PgError{Code: "57P01"}, true},
		{"connection exception class", &pgconn.PgError{Code: "08006"}, true},
		{"serialization failure", &pgconn.PgError{Code: "40001"}, true},
		{"deadlock", &pgconn.PgError{Code: "40P01"}, true},
		{"connection closed mid-write", io.ErrUnexpectedEOF, true},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"syntax error", &pgconn.PgError{Code: "42601"}, false},
		{"context cancelled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"unknown error", errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryable(tt.err); got != tt.want {
				t.Errorf("retryable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
