package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

var t0 = time.Date(2026, 10, 25, 9, 0, 0, 0, time.UTC)

// rec builds a valid record for vehicle n.
func rec(t *testing.T, n int) *kgo.Record {
	t.Helper()
	b, err := proto.Marshal(&fleetpulsev1.Position{
		VehicleId: fmt.Sprintf("v-%05d", n), EventTime: timestamppb.New(t0.Add(time.Duration(n) * time.Second)),
		Lat: 51.5, Lon: -0.12,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &kgo.Record{Key: []byte(fmt.Sprintf("v-%05d", n)), Value: b}
}

func recs(t *testing.T, from, n int) []*kgo.Record {
	out := make([]*kgo.Record, n)
	for i := range out {
		out[i] = rec(t, from+i)
	}
	return out
}

type pollResult struct {
	recs []*kgo.Record
	err  error
}

// fakeSource returns scripted poll results in order, then blocks until the
// poll's context ends, like a real consumer with nothing new to read.
type fakeSource struct {
	mu        sync.Mutex
	script    []pollResult
	committed [][]*kgo.Record
	commitErr []error // returned by successive Commit calls
	maxAsked  []int
}

func (s *fakeSource) Poll(ctx context.Context, max int) ([]*kgo.Record, error) {
	s.mu.Lock()
	s.maxAsked = append(s.maxAsked, max)
	if len(s.script) > 0 {
		r := s.script[0]
		s.script = s.script[1:]
		s.mu.Unlock()
		return r.recs, r.err
	}
	s.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *fakeSource) Commit(_ context.Context, recs []*kgo.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.committed = append(s.committed, recs)
	if len(s.commitErr) > 0 {
		err := s.commitErr[0]
		s.commitErr = s.commitErr[1:]
		return err
	}
	return nil
}

func (s *fakeSource) commits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.committed)
}

// fakeSink records batches; fail holds one error per call, nil meaning success.
type fakeSink struct {
	mu      sync.Mutex
	batches [][]*fleetpulsev1.Position
	fail    []error
	calls   int
	written chan struct{} // signalled after each successful write
}

func newSink() *fakeSink { return &fakeSink{written: make(chan struct{}, 100)} }

func (s *fakeSink) WriteBatch(_ context.Context, ps []*fleetpulsev1.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(s.fail) > 0 {
		err := s.fail[0]
		s.fail = s.fail[1:]
		if err != nil {
			return err
		}
	}
	s.batches = append(s.batches, append([]*fleetpulsev1.Position(nil), ps...))
	s.written <- struct{}{}
	return nil
}

func (s *fakeSink) sizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, len(s.batches))
	for i, b := range s.batches {
		out[i] = len(b)
	}
	return out
}

// run starts the pipeline, waits for wantWrites successful writes, then stops it
// and returns its stats.
func run(t *testing.T, src *fakeSource, sink *fakeSink, opts Options, wantWrites int) Stats {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := New(src, sink, quiet, opts)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	for i := 0; i < wantWrites; i++ {
		select {
		case <-sink.written:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out after %d of %d writes", i, wantWrites)
		}
	}
	// Give the commit that follows the last write time to happen.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after the context was cancelled")
	}
	return p.Stats()
}

func TestFlushesWhenBatchIsFull(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: recs(t, 0, 3)}, {recs: recs(t, 3, 3)}}}
	sink := newSink()

	run(t, src, sink, Options{MaxBatch: 3, FlushEvery: time.Hour}, 2)

	if got := sink.sizes(); len(got) != 2 || got[0] != 3 || got[1] != 3 {
		t.Fatalf("want two batches of 3, got %v", got)
	}
	if src.commits() != 2 {
		t.Errorf("want 2 commits, got %d", src.commits())
	}
}

func TestFlushesOnTimerWhenBatchIsNotFull(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: recs(t, 0, 2)}}}
	sink := newSink()

	start := time.Now()
	stats := run(t, src, sink, Options{MaxBatch: 1000, FlushEvery: 30 * time.Millisecond}, 1)

	if got := sink.sizes(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("want one batch of 2, got %v", got)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("a partial batch should flush after FlushEvery, took %v", time.Since(start))
	}
	if stats.Written != 2 || stats.Batches != 1 {
		t.Errorf("stats: want 2 written in 1 batch, got %+v", stats)
	}
}

func TestPollsOnlyForTheRoomLeftInTheBatch(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: recs(t, 0, 4)}, {recs: recs(t, 4, 6)}}}
	sink := newSink()

	run(t, src, sink, Options{MaxBatch: 10, FlushEvery: time.Hour}, 1)

	src.mu.Lock()
	defer src.mu.Unlock()
	if len(src.maxAsked) < 2 || src.maxAsked[0] != 10 || src.maxAsked[1] != 6 {
		t.Fatalf("want polls asking for 10 then 6, got %v", src.maxAsked)
	}
}

func TestCommitsOnlyAfterASuccessfulWrite(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: recs(t, 0, 2)}}}
	sink := newSink()
	sink.fail = []error{errors.New("db down"), errors.New("db down"), nil}

	stats := run(t, src, sink, Options{MaxBatch: 2, FlushEvery: time.Hour, RetryBackoff: time.Millisecond}, 1)

	if sink.calls != 3 {
		t.Errorf("want 3 write attempts (2 failures, 1 success), got %d", sink.calls)
	}
	if got := sink.sizes(); len(got) != 1 || got[0] != 2 {
		t.Errorf("the same batch should be retried, got batches %v", got)
	}
	if src.commits() != 1 {
		t.Errorf("want exactly 1 commit, after the successful write, got %d", src.commits())
	}
	if stats.WriteFail != 2 {
		t.Errorf("want 2 recorded write failures, got %d", stats.WriteFail)
	}
}

func TestDoesNotReadWhileAWriteKeepsFailing(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: recs(t, 0, 2)}, {recs: recs(t, 2, 2)}}}
	sink := newSink()
	sink.fail = []error{errors.New("db down"), errors.New("db down"), errors.New("db down"), errors.New("db down")}

	ctx, cancel := context.WithCancel(context.Background())
	p := New(src, sink, quiet, Options{MaxBatch: 2, FlushEvery: time.Hour, RetryBackoff: time.Hour})
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	time.Sleep(100 * time.Millisecond) // first write fails, then it backs off for an hour
	cancel()
	<-done

	src.mu.Lock()
	defer src.mu.Unlock()
	if len(src.script) != 1 {
		t.Errorf("the pipeline should not poll again while a batch is unwritten; %d scripted polls left, want 1", len(src.script))
	}
	if len(src.committed) != 0 {
		t.Errorf("nothing should be committed when the write never succeeded, got %d commits", len(src.committed))
	}
}

func TestStopsPromptlyWhenCancelledDuringRetry(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: recs(t, 0, 1)}}}
	sink := newSink()
	sink.fail = []error{errors.New("db down")}

	ctx, cancel := context.WithCancel(context.Background())
	p := New(src, sink, quiet, Options{MaxBatch: 1, FlushEvery: time.Hour, RetryBackoff: time.Hour})
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("want nil on shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run should stop at once when cancelled, even mid-backoff")
	}
	if src.commits() != 0 {
		t.Errorf("an unwritten batch must not be committed, got %d commits", src.commits())
	}
}

func TestDropsBadRecordsButCommitsThem(t *testing.T) {
	noID, _ := proto.Marshal(&fleetpulsev1.Position{EventTime: timestamppb.New(t0)})
	noTime, _ := proto.Marshal(&fleetpulsev1.Position{VehicleId: "v-no-time"})
	batch := []*kgo.Record{
		rec(t, 1),
		{Key: []byte("junk"), Value: []byte{0xff, 0xff, 0xff}},
		{Key: []byte("no-id"), Value: noID},
		{Key: []byte("no-time"), Value: noTime},
		rec(t, 2),
	}
	src := &fakeSource{script: []pollResult{{recs: batch}}}
	sink := newSink()

	stats := run(t, src, sink, Options{MaxBatch: 5, FlushEvery: time.Hour}, 1)

	if got := sink.sizes(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("only the 2 valid positions should be written, got %v", got)
	}
	if stats.Dropped != 3 {
		t.Errorf("want 3 dropped, got %d", stats.Dropped)
	}
	if src.commits() != 1 || len(src.committed[0]) != 5 {
		t.Errorf("all 5 records, bad ones included, should be committed together")
	}
}

func TestAWriteOfAllBadRecordsStillCommits(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: []*kgo.Record{{Key: []byte("junk"), Value: []byte{0xff}}}}}}
	sink := newSink()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := New(src, sink, quiet, Options{MaxBatch: 1, FlushEvery: time.Hour})
	go p.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for src.commits() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if src.commits() != 1 {
		t.Fatalf("a poison record must be committed so it doesn't block its partition, got %d commits", src.commits())
	}
}

func TestCommitFailureDoesNotStopThePipeline(t *testing.T) {
	src := &fakeSource{
		script:    []pollResult{{recs: recs(t, 0, 2)}, {recs: recs(t, 2, 2)}},
		commitErr: []error{errors.New("rebalance in progress")},
	}
	sink := newSink()

	run(t, src, sink, Options{MaxBatch: 2, FlushEvery: time.Hour}, 2)

	if got := sink.sizes(); len(got) != 2 {
		t.Fatalf("both batches should still be written, got %v", got)
	}
	if src.commits() != 2 {
		t.Errorf("want 2 commit attempts, got %d", src.commits())
	}
}

func TestFetchErrorWithRecordsStillProcessesThem(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: recs(t, 0, 2), err: errors.New("fetching positions/3: leader moved")}}}
	sink := newSink()

	run(t, src, sink, Options{MaxBatch: 2, FlushEvery: time.Hour}, 1)

	if got := sink.sizes(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("records returned alongside a fetch error should be written, got %v", got)
	}
}

func TestClientClosedStopsTheLoop(t *testing.T) {
	src := &fakeSource{script: []pollResult{{err: kgo.ErrClientClosed}}}
	p := New(src, newSink(), quiet, Options{})

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("want nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run should return when the consumer is closed")
	}
}

func TestDefaults(t *testing.T) {
	var o Options
	o.setDefaults()
	if o.MaxBatch != 1000 || o.FlushEvery != 250*time.Millisecond {
		t.Fatalf("want 1000 records / 250ms, got %d / %v", o.MaxBatch, o.FlushEvery)
	}
}

// ---- shutdown ----

func TestShutdownFlushesTheBufferedBatch(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: recs(t, 0, 2)}}}
	sink := newSink()

	ctx, cancel := context.WithCancel(context.Background())
	p := New(src, sink, quiet, Options{MaxBatch: 1000, FlushEvery: time.Hour})
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	time.Sleep(50 * time.Millisecond) // both records are now buffered, far from a full batch
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}

	if got := sink.sizes(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("the buffered batch should be written on shutdown, got %v", got)
	}
	if src.commits() != 1 {
		t.Errorf("the final batch should be committed, got %d commits", src.commits())
	}
	if p.Stats().Written != 2 {
		t.Errorf("want 2 written, got %d", p.Stats().Written)
	}
}

func TestShutdownGivesUpWhenTheFinalWriteKeepsFailing(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: recs(t, 0, 2)}}}
	sink := newSink()
	for range 1000 {
		sink.fail = append(sink.fail, errors.New("db down"))
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := New(src, sink, quiet, Options{
		MaxBatch: 1000, FlushEvery: time.Hour,
		RetryBackoff: time.Millisecond, ShutdownFlush: 80 * time.Millisecond,
	})
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown must be bounded by ShutdownFlush even when the database is down")
	}
	if src.commits() != 0 {
		t.Errorf("a batch that was never written must not be committed, got %d commits", src.commits())
	}
}

// slowSink blocks inside WriteBatch until released, like a write in flight.
type slowSink struct {
	started chan struct{}
	release chan struct{}

	mu          sync.Mutex
	ctxErrAtEnd error
	batchesDone int
}

func (s *slowSink) WriteBatch(ctx context.Context, _ []*fleetpulsev1.Position) error {
	s.started <- struct{}{}
	<-s.release
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctxErrAtEnd = ctx.Err()
	s.batchesDone++
	return nil
}

func TestShutdownLetsAWriteInFlightFinish(t *testing.T) {
	src := &fakeSource{script: []pollResult{{recs: recs(t, 0, 2)}}}
	sink := &slowSink{started: make(chan struct{}, 1), release: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	p := New(src, sink, quiet, Options{MaxBatch: 2, FlushEvery: time.Hour})
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	<-sink.started // the batch is full and its write is under way
	cancel()       // shutdown begins mid-write
	time.Sleep(30 * time.Millisecond)
	close(sink.release)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.ctxErrAtEnd != nil {
		t.Errorf("shutdown must not cancel a write already running, but its context ended with %v", sink.ctxErrAtEnd)
	}
	if src.commits() != 1 {
		t.Errorf("the finished write should be committed, got %d commits", src.commits())
	}
}

func TestShutdownWithNothingBufferedIsQuick(t *testing.T) {
	src := &fakeSource{}
	sink := newSink()
	ctx, cancel := context.WithCancel(context.Background())
	p := New(src, sink, quiet, Options{})
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("an idle pipeline should stop at once")
	}
	if len(sink.sizes()) != 0 || src.commits() != 0 {
		t.Errorf("nothing should be written or committed when nothing was buffered")
	}
}

// ---- publishing ----

// journal records the order of events across the sink, the publisher and the
// source's commit, so tests can check what happens before what.
type journal struct {
	mu     sync.Mutex
	events []string
}

func (j *journal) add(e string) { j.mu.Lock(); j.events = append(j.events, e); j.mu.Unlock() }
func (j *journal) list() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.events...)
}

type journalSink struct {
	j    *journal
	fail []error
}

func (s *journalSink) WriteBatch(_ context.Context, ps []*fleetpulsev1.Position) error {
	if len(s.fail) > 0 {
		err := s.fail[0]
		s.fail = s.fail[1:]
		if err != nil {
			s.j.add("write-failed")
			return err
		}
	}
	s.j.add(fmt.Sprintf("write:%d", len(ps)))
	return nil
}

type journalPublisher struct {
	j   *journal
	got [][]*fleetpulsev1.Position
}

func (p *journalPublisher) Publish(ps []*fleetpulsev1.Position) {
	p.j.add(fmt.Sprintf("publish:%d", len(ps)))
	p.got = append(p.got, append([]*fleetpulsev1.Position(nil), ps...))
}

type journalSource struct {
	*fakeSource
	j *journal
}

func (s *journalSource) Commit(ctx context.Context, r []*kgo.Record) error {
	s.j.add("commit")
	return s.fakeSource.Commit(ctx, r)
}

func runWith(t *testing.T, src Source, sink Sink, opts Options, until func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(src, sink, quiet, opts).Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for !until() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestPublishesAfterTheWriteAndBeforeTheCommit(t *testing.T) {
	j := &journal{}
	src := &journalSource{fakeSource: &fakeSource{script: []pollResult{{recs: recs(t, 0, 3)}}}, j: j}
	pub := &journalPublisher{j: j}

	runWith(t, src, &journalSink{j: j}, Options{MaxBatch: 3, FlushEvery: time.Hour, Publisher: pub},
		func() bool { return len(j.list()) >= 3 })

	want := []string{"write:3", "publish:3", "commit"}
	if got := j.list(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	if len(pub.got) != 1 || pub.got[0][0].GetVehicleId() != "v-00000" {
		t.Errorf("the publisher should get the batch's positions, got %v", pub.got)
	}
}

func TestNeverPublishesABatchThatFailedToWrite(t *testing.T) {
	j := &journal{}
	src := &journalSource{fakeSource: &fakeSource{script: []pollResult{{recs: recs(t, 0, 2)}}}, j: j}
	pub := &journalPublisher{j: j}
	sink := &journalSink{j: j, fail: []error{errors.New("db down"), errors.New("db down"), nil}}

	runWith(t, src, sink, Options{MaxBatch: 2, FlushEvery: time.Hour, RetryBackoff: time.Millisecond, Publisher: pub},
		func() bool { return len(j.list()) >= 5 })

	want := []string{"write-failed", "write-failed", "write:2", "publish:2", "commit"}
	if got := j.list(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("want %v, got %v (nothing may be published until the write succeeds, and only once)", want, got)
	}
}

func TestPublishesNothingWhileTheDatabaseStaysDown(t *testing.T) {
	j := &journal{}
	src := &journalSource{fakeSource: &fakeSource{script: []pollResult{{recs: recs(t, 0, 2)}}}, j: j}
	pub := &journalPublisher{j: j}
	sink := &journalSink{j: j}
	for range 1000 {
		sink.fail = append(sink.fail, errors.New("db down"))
	}

	runWith(t, src, sink, Options{MaxBatch: 2, FlushEvery: time.Hour, RetryBackoff: time.Millisecond, Publisher: pub},
		func() bool { return len(j.list()) >= 5 })

	for _, e := range j.list() {
		if strings.HasPrefix(e, "publish") || e == "commit" {
			t.Fatalf("with the database down nothing may be published or committed, saw %q in %v", e, j.list())
		}
	}
}

func TestDroppedRecordsAreNotPublished(t *testing.T) {
	j := &journal{}
	batch := []*kgo.Record{rec(t, 1), {Key: []byte("junk"), Value: []byte{0xff, 0xff}}, rec(t, 2)}
	src := &journalSource{fakeSource: &fakeSource{script: []pollResult{{recs: batch}}}, j: j}
	pub := &journalPublisher{j: j}

	runWith(t, src, &journalSink{j: j}, Options{MaxBatch: 3, FlushEvery: time.Hour, Publisher: pub},
		func() bool { return len(j.list()) >= 3 })

	if len(pub.got) != 1 || len(pub.got[0]) != 2 {
		t.Fatalf("only the 2 valid positions should be published, got %v", pub.got)
	}
}

func TestTheFinalShutdownBatchIsPublishedToo(t *testing.T) {
	j := &journal{}
	src := &journalSource{fakeSource: &fakeSource{script: []pollResult{{recs: recs(t, 0, 2)}}}, j: j}
	pub := &journalPublisher{j: j}

	// Far from a full batch and no timer: only the shutdown flush can write it.
	runWith(t, src, &journalSink{j: j}, Options{MaxBatch: 1000, FlushEvery: time.Hour, Publisher: pub},
		func() bool { time.Sleep(50 * time.Millisecond); return true })

	want := []string{"write:2", "publish:2", "commit"}
	if got := j.list(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestPipelineWorksWithoutAPublisher(t *testing.T) {
	j := &journal{}
	src := &journalSource{fakeSource: &fakeSource{script: []pollResult{{recs: recs(t, 0, 2)}}}, j: j}

	runWith(t, src, &journalSink{j: j}, Options{MaxBatch: 2, FlushEvery: time.Hour},
		func() bool { return len(j.list()) >= 2 })

	if got := j.list(); fmt.Sprint(got) != fmt.Sprint([]string{"write:2", "commit"}) {
		t.Fatalf("want a write then a commit, got %v", got)
	}
}
