package main

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
	"github.com/collinsbwl/Fleet-Pulse/internal/sim"
)

func TestToPosition(t *testing.T) {
	v := sim.NewFleet(1, sim.London, 1)[0]
	at := time.Date(2026, 10, 23, 9, 0, 0, 500, time.UTC)

	got := toPosition(v, at)

	if got.GetVehicleId() != v.ID {
		t.Errorf("vehicle id: want %s, got %s", v.ID, got.GetVehicleId())
	}
	if !got.GetEventTime().AsTime().Equal(at) {
		t.Errorf("event time: want %v, got %v", at, got.GetEventTime().AsTime())
	}
	if got.GetLat() != v.Lat || got.GetLon() != v.Lon {
		t.Errorf("position: want (%v, %v), got (%v, %v)", v.Lat, v.Lon, got.GetLat(), got.GetLon())
	}
	for name, pair := range map[string][2]float64{
		"speed": {v.SpeedKph, float64(got.GetSpeedKph())},
		"fuel":  {v.FuelPct, float64(got.GetFuelPct())},
		"temp":  {v.EngineTempC, float64(got.GetEngineTempC())},
	} {
		if math.Abs(pair[0]-pair[1]) > 1e-3 {
			t.Errorf("%s: want %v, got %v", name, pair[0], pair[1])
		}
	}
}

func TestToPositionSurvivesWire(t *testing.T) {
	v := sim.NewFleet(1, sim.London, 2)[0]
	want := toPosition(v, time.Now())

	b, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := &fleetpulsev1.Position{}
	if err := proto.Unmarshal(b, got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(want, got) {
		t.Fatalf("mismatch after round trip:\nwant %v\n got %v", want, got)
	}
}

func TestCountersFail(t *testing.T) {
	var c counters

	c.fail(context.Canceled)
	if c.failed.Load() != 0 {
		t.Fatalf("cancellation should not count as a failure, got %d", c.failed.Load())
	}

	c.fail(errors.New("broker unreachable"))
	if c.failed.Load() != 1 {
		t.Fatalf("want 1 failure, got %d", c.failed.Load())
	}
	if msg, _ := c.lastErr.Load().(string); msg != "broker unreachable" {
		t.Fatalf("last error: want %q, got %q", "broker unreachable", msg)
	}
}
