package fleetpulsev1_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
)

func TestPositionRoundTrip(t *testing.T) {
	want := &fleetpulsev1.Position{
		VehicleId:   "v-0042",
		EventTime:   timestamppb.New(time.Date(2026, 10, 22, 9, 30, 0, 123456789, time.UTC)),
		Lat:         51.507351,
		Lon:         -0.127758,
		SpeedKph:    48.5,
		FuelPct:     73.25,
		EngineTempC: 91.5,
	}

	b, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got := &fleetpulsev1.Position{}
	if err := proto.Unmarshal(b, got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !proto.Equal(want, got) {
		t.Fatalf("round trip mismatch:\nwant %v\n got %v", want, got)
	}
	if !got.GetEventTime().AsTime().Equal(want.GetEventTime().AsTime()) {
		t.Fatalf("event time lost precision: want %v, got %v", want.GetEventTime().AsTime(), got.GetEventTime().AsTime())
	}
	t.Logf("encoded size: %d bytes", len(b))
}
