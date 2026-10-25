// Package kafka wraps the franz-go client for the two things Fleet Pulse does
// with Redpanda: produce records (simulator) and consume them (server).
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes records to one topic. It is safe for concurrent use.
type Producer struct {
	client *kgo.Client
}

// NewProducer connects to the brokers and checks they are reachable, so a wrong
// address fails at startup instead of on the first record. extra options are
// applied last and can override the defaults.
func NewProducer(ctx context.Context, brokers []string, topic string, extra ...kgo.Opt) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		// Wait a few ms so records produced close together share one request.
		kgo.ProducerLinger(5 * time.Millisecond),
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
	return &Producer{client: client}, nil
}

// Produce queues a record and returns immediately. done, if not nil, is called
// from another goroutine once the broker has acknowledged the record or the
// send has failed for good. Produce blocks only when the client's buffer is
// full, which slows the caller down instead of dropping records.
func (p *Producer) Produce(ctx context.Context, key string, value []byte, done func(error)) {
	rec := &kgo.Record{Key: []byte(key), Value: value}
	p.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		if done != nil {
			done(err)
		}
	})
}

// Flush waits until every queued record has been acknowledged or has failed.
func (p *Producer) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("flushing kafka producer: %w", err)
	}
	return nil
}

// Close releases the client. Call Flush first to avoid losing queued records.
func (p *Producer) Close() {
	p.client.Close()
}
