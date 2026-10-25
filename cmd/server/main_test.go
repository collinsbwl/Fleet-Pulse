package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
)

func record(t *testing.T, partition int32, p *fleetpulsev1.Position) *kgo.Record {
	t.Helper()
	b, err := proto.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &kgo.Record{Key: []byte(p.GetVehicleId()), Value: b, Partition: partition}
}

func TestDecodeBatch(t *testing.T) {
	good := &fleetpulsev1.Position{
		VehicleId: "v-00001",
		EventTime: timestamppb.New(time.Date(2026, 10, 25, 9, 0, 0, 0, time.UTC)),
		Lat:       51.5, Lon: -0.12, SpeedKph: 40, FuelPct: 80, EngineTempC: 90,
	}
	recs := []*kgo.Record{
		record(t, 0, good),
		{Key: []byte("v-00002"), Value: []byte{0xff, 0xff, 0xff}, Partition: 1}, // not valid Protobuf
		record(t, 2, &fleetpulsev1.Position{VehicleId: "v-00003"}),
	}

	got, undecodable := decodeBatch(recs)

	if undecodable != 1 {
		t.Errorf("undecodable: want 1, got %d", undecodable)
	}
	if len(got) != 2 {
		t.Fatalf("decoded: want 2, got %d", len(got))
	}
	if !proto.Equal(got[0], good) {
		t.Errorf("first position changed in transit:\nwant %v\n got %v", good, got[0])
	}
	if got[1].GetVehicleId() != "v-00003" {
		t.Errorf("second position: want v-00003, got %s", got[1].GetVehicleId())
	}
}

func TestDecodeBatchEmpty(t *testing.T) {
	got, undecodable := decodeBatch(nil)
	if len(got) != 0 || undecodable != 0 {
		t.Fatalf("want nothing, got %d positions and %d undecodable", len(got), undecodable)
	}
}

func TestPartitionsOf(t *testing.T) {
	recs := []*kgo.Record{{Partition: 3}, {Partition: 0}, {Partition: 3}, {Partition: 5}, {Partition: 0}}
	want := []string{"0", "3", "5"}
	if got := partitionsOf(recs); !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}
