package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// brokers matches the host listener from deploy/docker-compose.yml.
var brokers = []string{"localhost:19092"}

// Integration test: needs `make up`. It skips when Redpanda isn't reachable.
func TestProducerRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A throwaway topic per run, created on first use. `make down` removes it.
	topic := fmt.Sprintf("test-producer-%d", time.Now().UnixNano())

	pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
	p, err := NewProducer(pingCtx, brokers, topic, kgo.AllowAutoTopicCreation())
	pingCancel()
	if err != nil {
		t.Skipf("Redpanda not reachable on %v (run `make up`): %v", brokers, err)
	}
	defer p.Close()

	acked := make(chan error, 1)
	p.Produce(ctx, "v-00042", []byte("hello"), func(err error) { acked <- err })
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	select {
	case err := <-acked:
		if err != nil {
			t.Fatalf("record not acknowledged: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for acknowledgement")
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	defer consumer.Close()

	fetches := consumer.PollRecords(ctx, 1)
	if err := fetches.Err(); err != nil {
		t.Fatalf("consuming: %v", err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if string(recs[0].Key) != "v-00042" || string(recs[0].Value) != "hello" {
		t.Fatalf("record mismatch: key=%q value=%q", recs[0].Key, recs[0].Value)
	}
}

func TestNewProducerFailsFastOnBadBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Nothing listens on port 1, so the ping must fail rather than hang.
	p, err := NewProducer(ctx, []string{"127.0.0.1:1"}, "positions")
	if err == nil {
		p.Close()
		t.Fatal("expected an error for an unreachable broker")
	}
}
