package ws

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newHub(opts Options) *Hub { return NewHub(quiet, opts) }

// waitFor polls cond until it is true, failing the test after timeout.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// drain reads c's queue in the background and counts what arrives.
func drain(c *Client) *atomic.Int64 {
	var n atomic.Int64
	go func() {
		for range c.Messages() {
			n.Add(1)
		}
	}()
	return &n
}

func TestBroadcastReachesEveryClientInOrder(t *testing.T) {
	h := newHub(Options{})
	clients := []*Client{NewClient(8), NewClient(8), NewClient(8)}
	for _, c := range clients {
		h.Register(c)
	}

	h.Broadcast([]byte("one"))
	h.Broadcast([]byte("two"))

	for i, c := range clients {
		for _, want := range []string{"one", "two"} {
			select {
			case got := <-c.Messages():
				if string(got) != want {
					t.Errorf("client %d: want %q, got %q", i, want, got)
				}
			case <-time.After(time.Second):
				t.Fatalf("client %d never received %q", i, want)
			}
		}
	}
	if h.Clients() != 3 {
		t.Errorf("want 3 clients, got %d", h.Clients())
	}
}

func TestSlowClientIsDroppedWithoutStallingTheOthers(t *testing.T) {
	h := newHub(Options{})
	slow := NewClient(2) // never reads
	fastA, fastB := NewClient(64), NewClient(64)
	for _, c := range []*Client{slow, fastA, fastB} {
		h.Register(c)
	}
	gotA, gotB := drain(fastA), drain(fastB)

	const total = 20
	start := time.Now()
	for i := 0; i < total; i++ {
		h.Broadcast([]byte(fmt.Sprintf("msg-%d", i)))
	}
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Errorf("Broadcast must never wait for a slow client, %d broadcasts took %v", total, took)
	}

	waitFor(t, "both fast clients to get every message", 2*time.Second, func() bool {
		return gotA.Load() == total && gotB.Load() == total
	})
	if h.Clients() != 2 {
		t.Errorf("the slow client should be gone: want 2 clients, got %d", h.Clients())
	}
	if h.Dropped() != 1 {
		t.Errorf("want 1 dropped client, got %d", h.Dropped())
	}

	// The slow client keeps what was already queued, then its queue closes.
	queued := 0
	for range slow.Messages() {
		queued++
	}
	if queued != 2 {
		t.Errorf("slow client should have kept its 2 queued messages, got %d", queued)
	}
	if slow.closeCode != websocket.StatusPolicyViolation {
		t.Errorf("slow client should be closed with a policy violation, got %v", slow.closeCode)
	}
}

func TestUnregisterStopsDeliveryAndIsIdempotent(t *testing.T) {
	h := newHub(Options{})
	c := NewClient(4)
	h.Register(c)

	h.Unregister(c)
	h.Unregister(c) // a second call must not panic on the closed queue
	h.Broadcast([]byte("after"))

	if _, open := <-c.Messages(); open {
		t.Error("an unregistered client's queue should be closed and empty")
	}
	if h.Clients() != 0 {
		t.Errorf("want 0 clients, got %d", h.Clients())
	}
	if h.Dropped() != 0 {
		t.Errorf("leaving on purpose is not a drop, got %d", h.Dropped())
	}
}

func TestBroadcastWithNoClientsIsHarmless(t *testing.T) {
	h := newHub(Options{})
	h.Broadcast([]byte("into the void"))
	if h.Clients() != 0 || h.Dropped() != 0 {
		t.Fatalf("want an empty hub, got %d clients, %d dropped", h.Clients(), h.Dropped())
	}
}

func TestCloseDropsEveryoneAndRefusesNewClients(t *testing.T) {
	h := newHub(Options{})
	a, b := NewClient(4), NewClient(4)
	h.Register(a)
	h.Register(b)

	if err := h.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for name, c := range map[string]*Client{"a": a, "b": b} {
		if _, open := <-c.Messages(); open {
			t.Errorf("client %s should be closed", name)
		}
		if c.closeCode != websocket.StatusGoingAway {
			t.Errorf("client %s: want going-away, got %v", name, c.closeCode)
		}
	}
	if h.Register(NewClient(1)) {
		t.Error("a closed hub must refuse new clients")
	}
	h.Broadcast([]byte("late")) // must not panic
}

func TestCloseGivesUpWaitingWhenContextEnds(t *testing.T) {
	h := newHub(Options{})
	h.handlers.Add(1) // a connection handler that never finishes
	defer h.handlers.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := h.Close(ctx); err == nil {
		t.Fatal("Close should report that it gave up waiting")
	}
}

// Run with -race: registering, dropping and broadcasting from many goroutines
// at once must neither race nor send on a closed queue.
func TestConcurrentRegisterUnregisterBroadcast(t *testing.T) {
	h := newHub(Options{})
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					h.Broadcast([]byte("x"))
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					c := NewClient(1) // tiny, so Broadcast drops it constantly
					h.Register(c)
					if i%2 == 0 {
						h.Unregister(c)
					}
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestDefaults(t *testing.T) {
	var o Options
	o.setDefaults()
	if o.SendBuffer != 64 || o.WriteTimeout != 5*time.Second || o.PingInterval != 30*time.Second {
		t.Fatalf("unexpected defaults: %+v", o)
	}
}
