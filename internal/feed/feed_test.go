package feed

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
)

var (
	quiet = slog.New(slog.NewTextHandler(io.Discard, nil))
	t0    = time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
)

type fakeHub struct {
	clients int
	sent    [][]byte
}

func (f *fakeHub) Broadcast(msg []byte) { f.sent = append(f.sent, msg) }
func (f *fakeHub) Clients() int         { return f.clients }

func pos(id string, at time.Time, lat float64) *fleetpulsev1.Position {
	return &fleetpulsev1.Position{
		VehicleId: id, EventTime: timestamppb.New(at),
		Lat: lat, Lon: -122.4, SpeedKph: 48.5, FuelPct: 73.25, EngineTempC: 91.5,
	}
}

func decode(t *testing.T, msg []byte) []map[string]any {
	t.Helper()
	var out []map[string]any
	if err := json.Unmarshal(msg, &out); err != nil {
		t.Fatalf("message is not a JSON array: %v\n%s", err, msg)
	}
	return out
}

func TestPublishSendsOneMessageWithOneEntryPerVehicle(t *testing.T) {
	hub := &fakeHub{clients: 1}
	p := NewPublisher(hub, quiet)

	p.Publish([]*fleetpulsev1.Position{
		pos("v-1", t0, 1),
		pos("v-2", t0, 2),
		pos("v-1", t0.Add(2*time.Second), 11), // newest for v-1
		pos("v-1", t0.Add(1*time.Second), 10), // older, arrives last
		pos("v-3", t0, 3),
	})

	if len(hub.sent) != 1 {
		t.Fatalf("want exactly one message per batch, got %d", len(hub.sent))
	}
	got := decode(t, hub.sent[0])
	if len(got) != 3 {
		t.Fatalf("want 3 vehicles (one each), got %d: %v", len(got), got)
	}
	wantOrder := []string{"v-1", "v-2", "v-3"}
	for i, id := range wantOrder {
		if got[i]["id"] != id {
			t.Errorf("entry %d: want %s, got %v (order of first appearance)", i, id, got[i]["id"])
		}
	}
	if got[0]["lat"] != 11.0 || got[0]["event_time"] != "2026-10-26T09:00:02Z" {
		t.Errorf("v-1 should carry its newest reading, got %v", got[0])
	}
}

func TestPublishUsesTheSameShapeAsTheVehiclesEndpoint(t *testing.T) {
	hub := &fakeHub{clients: 1}
	NewPublisher(hub, quiet).Publish([]*fleetpulsev1.Position{pos("v-00001", t0.Add(123456*time.Microsecond), 37.7749)})

	got := decode(t, hub.sent[0])[0]
	want := map[string]any{
		"id": "v-00001", "event_time": "2026-10-26T09:00:00.123456Z",
		"lat": 37.7749, "lon": -122.4,
		"speed_kph": 48.5, "fuel_pct": 73.25, "engine_temp_c": 91.5,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("field %q: want %v, got %v", k, v, got[k])
		}
	}
	if len(got) != len(want) {
		t.Errorf("want exactly %d fields, got %d: %v", len(want), len(got), got)
	}
}

func TestPublishEqualTimesKeepTheLaterReading(t *testing.T) {
	hub := &fakeHub{clients: 1}
	NewPublisher(hub, quiet).Publish([]*fleetpulsev1.Position{pos("v-1", t0, 1), pos("v-1", t0, 2)})

	if got := decode(t, hub.sent[0]); len(got) != 1 || got[0]["lat"] != 2.0 {
		t.Fatalf("on equal times the later reading in the batch should win, got %v", got)
	}
}

func TestPublishDoesNothingWithoutBrowsers(t *testing.T) {
	hub := &fakeHub{clients: 0}
	NewPublisher(hub, quiet).Publish([]*fleetpulsev1.Position{pos("v-1", t0, 1)})
	if len(hub.sent) != 0 {
		t.Fatalf("with nobody connected there is nothing to send, got %d messages", len(hub.sent))
	}
}

func TestPublishIgnoresAnEmptyBatch(t *testing.T) {
	hub := &fakeHub{clients: 2}
	p := NewPublisher(hub, quiet)
	p.Publish(nil)
	p.Publish([]*fleetpulsev1.Position{})
	if len(hub.sent) != 0 {
		t.Fatalf("an empty batch should send nothing, got %d messages", len(hub.sent))
	}
}

func TestPublishDoesNotKeepTheBatch(t *testing.T) {
	hub := &fakeHub{clients: 1}
	batch := []*fleetpulsev1.Position{pos("v-1", t0, 1)}
	NewPublisher(hub, quiet).Publish(batch)

	// The pipeline reuses its slice and positions after Publish returns.
	batch[0].Lat = 999
	batch[0] = nil

	if got := decode(t, hub.sent[0]); got[0]["lat"] != 1.0 {
		t.Fatalf("the message must be a snapshot, got %v", got[0])
	}
}
