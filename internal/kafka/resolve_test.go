package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func fetchWith(recs ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      "positions",
		Partitions: []kgo.FetchPartition{{Partition: 0, Records: recs}},
	}}}}
}

// Regression: a poll can return records at the same instant its deadline
// passes. Those records were already handed over by the client, so dropping
// them lost about 1 message in 20,000 under load.
func TestResolveKeepsRecordsWhenContextExpiredAfterThePoll(t *testing.T) {
	want := []*kgo.Record{{Key: []byte("a")}, {Key: []byte("b")}}

	got, err := resolve(fetchWith(want...), context.DeadlineExceeded)

	if len(got) != 2 {
		t.Fatalf("want both records kept, got %d", len(got))
	}
	if err != nil {
		t.Fatalf("an expired context must not be reported when records arrived, got %v", err)
	}
}

func TestResolveReportsContextErrorWhenNothingArrived(t *testing.T) {
	got, err := resolve(kgo.NewErrFetch(context.DeadlineExceeded), context.DeadlineExceeded)

	if len(got) != 0 {
		t.Fatalf("want no records, got %d", len(got))
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the context error, got %v", err)
	}
}

func TestResolveReportsClientClosed(t *testing.T) {
	_, err := resolve(kgo.NewErrFetch(kgo.ErrClientClosed), nil)
	if !errors.Is(err, kgo.ErrClientClosed) {
		t.Fatalf("want ErrClientClosed, got %v", err)
	}
}

func TestResolveReturnsRecordsAlongsideFetchErrors(t *testing.T) {
	fetches := fetchWith(&kgo.Record{Key: []byte("a")})
	fetches = append(fetches, kgo.NewErrFetch(errors.New("leader moved"))...)

	got, err := resolve(fetches, nil)

	if len(got) != 1 {
		t.Fatalf("want the record kept, got %d", len(got))
	}
	if err == nil {
		t.Fatal("want the fetch error reported alongside the record")
	}
}

func TestResolveEmptyPollWithLiveContext(t *testing.T) {
	got, err := resolve(nil, nil)
	if len(got) != 0 || err != nil {
		t.Fatalf("want nothing and no error, got %d records, %v", len(got), err)
	}
}
