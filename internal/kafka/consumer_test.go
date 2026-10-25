package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Integration test: needs `make up`. It skips when Redpanda isn't reachable.
// It checks the at-least-once contract: records whose offsets were committed
// are not delivered again, and records that were not committed are.
func TestConsumerCommitSemantics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	suffix := time.Now().UnixNano()
	topic := fmt.Sprintf("test-consumer-%d", suffix)
	const n = 5

	pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
	p, err := NewProducer(pingCtx, brokers, topic, kgo.AllowAutoTopicCreation())
	pingCancel()
	if err != nil {
		t.Skipf("Redpanda not reachable on %v (run `make up`): %v", brokers, err)
	}
	defer p.Close()

	for i := 0; i < n; i++ {
		p.Produce(ctx, fmt.Sprintf("v-%05d", i), []byte("x"), nil)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	t.Run("committed records are not redelivered", func(t *testing.T) {
		group := fmt.Sprintf("test-group-committed-%d", suffix)

		first := newTestConsumer(ctx, t, group, topic)
		recs := pollN(ctx, t, first, n)
		if err := first.Commit(ctx, recs); err != nil {
			t.Fatalf("commit: %v", err)
		}
		first.Close()

		second := newTestConsumer(ctx, t, group, topic)
		defer second.Close()
		if got := pollFor(ctx, second, 4*time.Second); len(got) != 0 {
			t.Fatalf("want no redelivery after commit, got %d records", len(got))
		}
	})

	t.Run("uncommitted records are redelivered", func(t *testing.T) {
		group := fmt.Sprintf("test-group-uncommitted-%d", suffix)

		first := newTestConsumer(ctx, t, group, topic)
		pollN(ctx, t, first, n)
		first.Close() // leaves the group without committing

		second := newTestConsumer(ctx, t, group, topic)
		defer second.Close()
		if got := pollN(ctx, t, second, n); len(got) != n {
			t.Fatalf("want %d records redelivered, got %d", n, len(got))
		}
	})
}

func newTestConsumer(ctx context.Context, t *testing.T, group, topic string) *Consumer {
	t.Helper()
	c, err := NewConsumer(ctx, brokers, group, topic)
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	return c
}

// pollN polls until it has n records, failing the test if ctx ends first.
func pollN(ctx context.Context, t *testing.T, c *Consumer, n int) []*kgo.Record {
	t.Helper()
	var all []*kgo.Record
	for len(all) < n {
		recs, err := c.Poll(ctx, n)
		if err != nil && ctx.Err() != nil {
			t.Fatalf("polled %d of %d records before giving up: %v", len(all), n, err)
		}
		all = append(all, recs...)
	}
	return all
}

// pollFor polls for d and returns whatever arrived in that time.
func pollFor(ctx context.Context, c *Consumer, d time.Duration) []*kgo.Record {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	var all []*kgo.Record
	for {
		recs, err := c.Poll(ctx, 100)
		all = append(all, recs...)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return all
		}
	}
}
