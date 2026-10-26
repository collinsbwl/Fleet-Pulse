// Package ws fans messages out to browsers over WebSockets.
//
// The Hub keeps a set of clients and hands each message to every one of them
// without ever waiting on any. A client that cannot keep up is dropped instead
// of slowing everyone else down.
package ws

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// Options tune the hub. Zero values take the defaults.
type Options struct {
	SendBuffer   int           // messages queued per client before it counts as too slow (default 64)
	WriteTimeout time.Duration // how long one write to a browser may take (default 5s)
	PingInterval time.Duration // how often idle browsers are pinged; a dead one is dropped (default 30s)
}

func (o *Options) setDefaults() {
	if o.SendBuffer <= 0 {
		o.SendBuffer = 64
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 5 * time.Second
	}
	if o.PingInterval <= 0 {
		o.PingInterval = 30 * time.Second
	}
}

// Client is one connected browser, as the hub sees it: a queue of messages.
type Client struct {
	send chan []byte

	// Set by the hub, under its lock, before send is closed. The client's writer
	// reads them only after it sees send closed, so no extra locking is needed.
	closeCode   websocket.StatusCode
	closeReason string
}

// NewClient makes a client whose queue holds up to buffer messages.
func NewClient(buffer int) *Client {
	return &Client{send: make(chan []byte, max(buffer, 1))}
}

// Messages is the client's queue. It is closed when the hub drops the client.
func (c *Client) Messages() <-chan []byte { return c.send }

// Hub tracks connected clients and broadcasts to them. It is safe for
// concurrent use.
type Hub struct {
	log  *slog.Logger
	opts Options

	mu      sync.RWMutex
	clients map[*Client]struct{}
	closed  bool

	handlers sync.WaitGroup // running connection handlers, so Close can wait for them
	dropped  atomic.Uint64
}

// NewHub returns an empty hub.
func NewHub(log *slog.Logger, opts Options) *Hub {
	opts.setDefaults()
	return &Hub{log: log, opts: opts, clients: make(map[*Client]struct{})}
}

// Register adds a client. It reports false, and adds nothing, once the hub is closed.
func (h *Hub) Register(c *Client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.clients[c] = struct{}{}
	return true
}

// Unregister removes a client and closes its queue. It is safe to call more than once.
func (h *Hub) Unregister(c *Client) {
	h.drop(c, websocket.StatusNormalClosure, "")
}

// Broadcast queues msg for every client and returns at once. A client whose
// queue is full is dropped, so one slow browser never holds up the others. The
// same slice goes to every client: callers must not change it afterwards.
func (h *Hub) Broadcast(msg []byte) {
	var slow []*Client

	h.mu.RLock()
	for c := range h.clients {
		select {
		case c.send <- msg:
		default:
			slow = append(slow, c)
		}
	}
	h.mu.RUnlock()

	for _, c := range slow {
		if h.drop(c, websocket.StatusPolicyViolation, "client too slow") {
			h.dropped.Add(1)
			h.log.Warn("dropped a slow websocket client", "buffer", h.opts.SendBuffer)
		}
	}
}

// Clients is the number of connected clients.
func (h *Hub) Clients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// Dropped counts clients removed for being too slow since the hub started.
func (h *Hub) Dropped() uint64 { return h.dropped.Load() }

// Close drops every client with a "going away" close frame, refuses new ones,
// and waits for the connection handlers to finish or ctx to end.
func (h *Hub) Close(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	for c := range h.clients {
		c.closeCode, c.closeReason = websocket.StatusGoingAway, "server shutting down"
		close(c.send)
		delete(h.clients, c)
	}
	h.mu.Unlock()

	done := make(chan struct{})
	go func() { h.handlers.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// drop removes c and closes its queue with the given close code. It reports
// whether c was still registered. The queue is closed under the lock, and
// Broadcast only sends to registered clients while holding the read lock, so a
// send can never reach a closed queue.
func (h *Hub) drop(c *Client, code websocket.StatusCode, reason string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; !ok {
		return false
	}
	delete(h.clients, c)
	c.closeCode, c.closeReason = code, reason
	close(c.send)
	return true
}

// Handler serves the WebSocket endpoint: each connection becomes a client.
//
// Browsers must connect from the same origin as the page (a cross-origin
// handshake is refused); tools with no Origin header, such as websocat, are
// accepted. Anything a browser sends is ignored, and a data frame closes the
// connection.
func (h *Hub) Handler() http.Handler {
	return http.HandlerFunc(h.serve)
}

func (h *Hub) serve(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		// Accept has already answered the HTTP request with the reason.
		h.log.Debug("websocket handshake refused", "error", err)
		return
	}

	c := NewClient(h.opts.SendBuffer)
	h.handlers.Add(1)
	defer h.handlers.Done()
	if !h.Register(c) {
		_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
		return
	}
	h.log.Debug("websocket client connected", "clients", h.Clients())

	h.run(r.Context(), conn, c)
	h.log.Debug("websocket client disconnected", "clients", h.Clients())
}

// run writes queued messages to the browser until the client is dropped, the
// browser goes away, or a write fails.
func (h *Hub) run(parent context.Context, conn *websocket.Conn, c *Client) {
	// CloseRead keeps reading so close frames and pongs are handled; its context
	// ends when the browser disconnects or sends data.
	ctx := conn.CloseRead(parent)

	ping := time.NewTicker(h.opts.PingInterval)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			h.Unregister(c)
			_ = conn.CloseNow()
			return

		case msg, ok := <-c.send:
			if !ok { // the hub dropped us; say why, then hang up
				_ = conn.Close(c.closeCode, c.closeReason)
				return
			}
			wctx, cancel := context.WithTimeout(ctx, h.opts.WriteTimeout)
			err := conn.Write(wctx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				h.Unregister(c)
				_ = conn.CloseNow()
				return
			}

		case <-ping.C:
			pctx, cancel := context.WithTimeout(ctx, h.opts.WriteTimeout)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				h.Unregister(c)
				_ = conn.CloseNow()
				return
			}
		}
	}
}
