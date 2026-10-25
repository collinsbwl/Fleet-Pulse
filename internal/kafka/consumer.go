package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer reads records from one topic as a member of a consumer group.
// Offsets are committed by the caller, only after the records have been
// handled, so a crash replays records instead of losing them (at-least-once).
type Consumer struct {
	client *kgo.Client
}

// NewConsumer joins the group and checks the brokers are reachable. A new group
// starts from the oldest retained record, so nothing already in the topic is
// skipped. extra options are applied last and can override the defaults.
func NewConsumer(ctx context.Context, brokers []string, group, topic string, extra ...kgo.Opt) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	}
	opts = append(opts, extra...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("reaching kafka brokers %v: %w", brokers, err)
	}
	return &Consumer{client: client}, nil
}

// Poll waits for records and returns at most max of them.
//
// It returns ctx's error when ctx ends and kgo.ErrClientClosed after Close.
// Other fetch problems (a partition leader moving, a broker restarting) are
// usually temporary, so Poll may return records and a non-nil error together:
// log the error and keep going.
func (c *Consumer) Poll(ctx context.Context, max int) ([]*kgo.Record, error) {
	fetches := c.client.PollRecords(ctx, max)
	if fetches.IsClientClosed() {
		return nil, kgo.ErrClientClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var errs []error
	fetches.EachError(func(topic string, partition int32, err error) {
		errs = append(errs, fmt.Errorf("fetching %s/%d: %w", topic, partition, err))
	})
	return fetches.Records(), errors.Join(errs...)
}

// Commit records that everything up to and including recs has been handled.
func (c *Consumer) Commit(ctx context.Context, recs []*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	if err := c.client.CommitRecords(ctx, recs...); err != nil {
		return fmt.Errorf("committing offsets: %w", err)
	}
	return nil
}

// Close leaves the group, so its partitions move to another member quickly.
func (c *Consumer) Close() {
	c.client.Close()
}
