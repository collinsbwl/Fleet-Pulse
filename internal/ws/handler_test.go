package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// serve starts a test server running the hub's handler.
func serve(t *testing.T, h *Hub) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http")
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func read(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, msg, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(msg)
}

func TestBrowserReceivesBroadcast(t *testing.T) {
	h := newHub(Options{})
	_, url := serve(t, h)
	conn := dial(t, url)
	waitFor(t, "the client to register", 2*time.Second, func() bool { return h.Clients() == 1 })

	h.Broadcast([]byte(`{"id":"v-00001"}`))

	if got := read(t, conn); got != `{"id":"v-00001"}` {
		t.Fatalf("want the broadcast message, got %q", got)
	}
}

func TestEveryConnectedBrowserGetsTheMessage(t *testing.T) {
	h := newHub(Options{})
	_, url := serve(t, h)
	conns := []*websocket.Conn{dial(t, url), dial(t, url), dial(t, url)}
	waitFor(t, "3 clients to register", 2*time.Second, func() bool { return h.Clients() == 3 })

	h.Broadcast([]byte("hello all"))

	for i, c := range conns {
		if got := read(t, c); got != "hello all" {
			t.Errorf("client %d: want %q, got %q", i, "hello all", got)
		}
	}
}

func TestDisconnectRemovesTheClient(t *testing.T) {
	h := newHub(Options{})
	_, url := serve(t, h)
	conn := dial(t, url)
	waitFor(t, "the client to register", 2*time.Second, func() bool { return h.Clients() == 1 })

	_ = conn.Close(websocket.StatusNormalClosure, "bye")

	waitFor(t, "the hub to notice the disconnect", 2*time.Second, func() bool { return h.Clients() == 0 })
	if h.Dropped() != 0 {
		t.Errorf("a normal disconnect is not a drop, got %d", h.Dropped())
	}
}

// A browser that stops reading (a frozen tab) must be cut off, and the browsers
// that do read must carry on receiving, with no stall.
func TestFrozenBrowserIsDroppedAndOthersKeepReceiving(t *testing.T) {
	h := newHub(Options{SendBuffer: 8})
	_, url := serve(t, h)

	frozen := dial(t, url) // never reads
	_ = frozen
	live := dial(t, url)
	live.SetReadLimit(1 << 20) // the client refuses messages over 32 KB by default; ours are 256 KB
	var received atomic.Int64
	go func() {
		for {
			if _, _, err := live.Read(context.Background()); err != nil {
				return
			}
			received.Add(1)
		}
	}()
	waitFor(t, "both clients to register", 2*time.Second, func() bool { return h.Clients() == 2 })

	// Large messages fill the frozen browser's socket buffers, then its queue.
	big := []byte(strings.Repeat("x", 256*1024))
	deadline := time.Now().Add(15 * time.Second)
	sent := 0
	for h.Dropped() == 0 && time.Now().Before(deadline) {
		h.Broadcast(big)
		sent++
		time.Sleep(time.Millisecond)
	}
	if h.Dropped() != 1 {
		t.Fatalf("the frozen browser should have been dropped after %d broadcasts, dropped=%d", sent, h.Dropped())
	}
	if h.Clients() != 1 {
		t.Errorf("want only the live client left, got %d", h.Clients())
	}

	before := received.Load()
	h.Broadcast([]byte("still here"))
	waitFor(t, "the live client to get the next message", 3*time.Second, func() bool { return received.Load() > before })
}

func TestCloseTellsBrowsersTheServerIsShuttingDown(t *testing.T) {
	h := newHub(Options{})
	_, url := serve(t, h)
	conn := dial(t, url)
	waitFor(t, "the client to register", 2*time.Second, func() bool { return h.Clients() == 1 })

	closed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _, err := conn.Read(ctx)
		closed <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		t.Fatalf("Close should wait for handlers to finish: %v", err)
	}

	select {
	case err := <-closed:
		if got := websocket.CloseStatus(err); got != websocket.StatusGoingAway {
			t.Fatalf("want close code going-away (1001), got %v (err: %v)", got, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the browser never saw the connection close")
	}
}

func TestPlainHTTPRequestIsNotUpgraded(t *testing.T) {
	h := newHub(Options{})
	srv, _ := serve(t, h)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 400 || resp.StatusCode >= 500 {
		t.Fatalf("a non-WebSocket request should be refused with a 4xx, got %d", resp.StatusCode)
	}
	if h.Clients() != 0 {
		t.Errorf("want 0 clients, got %d", h.Clients())
	}
}

func TestCrossOriginBrowserIsRefused(t *testing.T) {
	h := newHub(Options{})
	_, url := serve(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"https://evil.example"}},
	})
	if err == nil {
		t.Fatal("a handshake from another site's page must be refused")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %v (err: %v)", resp, err)
	}
	if h.Clients() != 0 {
		t.Errorf("want 0 clients, got %d", h.Clients())
	}
}

func TestPingsKeepAnAnsweringBrowserConnected(t *testing.T) {
	h := newHub(Options{PingInterval: 20 * time.Millisecond})
	_, url := serve(t, h)
	conn := dial(t, url)
	conn.CloseRead(context.Background()) // reading is what answers pings
	waitFor(t, "the client to register", 2*time.Second, func() bool { return h.Clients() == 1 })

	time.Sleep(300 * time.Millisecond) // about 15 pings

	if h.Clients() != 1 {
		t.Fatalf("a browser that answers pings must stay connected, got %d clients", h.Clients())
	}
}

func TestServeAfterCloseRefusesTheConnection(t *testing.T) {
	h := newHub(Options{})
	_, url := serve(t, h)
	if err := h.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	conn := dial(t, url) // the handshake still completes, then the server hangs up
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Fatalf("want going-away, got %v (err: %v)", websocket.CloseStatus(err), err)
	}
	if h.Clients() != 0 {
		t.Errorf("want 0 clients, got %d", h.Clients())
	}
}
