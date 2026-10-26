// Package feed turns saved batches of positions into WebSocket messages.
package feed

import (
	"encoding/json"
	"log/slog"

	fleetpulsev1 "github.com/collinsbwl/Fleet-Pulse/internal/pb/fleetpulse/v1"
	"github.com/collinsbwl/Fleet-Pulse/internal/wire"
)

// Hub is the part of ws.Hub the publisher uses.
type Hub interface {
	Broadcast(msg []byte)
	Clients() int
}

// Publisher sends each batch to every connected browser. *Publisher satisfies
// pipeline.Publisher.
type Publisher struct {
	hub Hub
	log *slog.Logger
}

// NewPublisher returns a publisher that broadcasts through hub.
func NewPublisher(hub Hub, log *slog.Logger) *Publisher {
	return &Publisher{hub: hub, log: log}
}

// Publish broadcasts one message for the batch: a JSON array holding the
// newest reading of each vehicle in it, in the same shape as GET /vehicles. A
// batch can hold several readings of one vehicle; browsers only need the last.
//
// It does nothing, not even encoding, while no browser is connected, never
// blocks (Hub.Broadcast returns at once), and does not keep positions after it
// returns, so the caller may reuse the slice.
func (p *Publisher) Publish(positions []*fleetpulsev1.Position) {
	if len(positions) == 0 || p.hub.Clients() == 0 {
		return
	}

	msg, err := json.Marshal(newest(positions))
	if err != nil {
		// Plain numbers and strings cannot fail to encode; if one ever does, the
		// feed skips this batch and the next one carries on.
		p.log.Error("encoding a batch for the websocket feed failed", "error", err)
		return
	}
	p.hub.Broadcast(msg)
}

// newest keeps one reading per vehicle, the one with the latest event time (the
// later one in the batch if the times are equal), in order of first appearance.
func newest(positions []*fleetpulsev1.Position) []wire.Vehicle {
	index := make(map[string]int, len(positions))
	out := make([]wire.Vehicle, 0, len(positions))
	for _, p := range positions {
		v := wire.Vehicle{
			ID:          p.GetVehicleId(),
			EventTime:   p.GetEventTime().AsTime().UTC(),
			Lat:         p.GetLat(),
			Lon:         p.GetLon(),
			SpeedKph:    p.GetSpeedKph(),
			FuelPct:     p.GetFuelPct(),
			EngineTempC: p.GetEngineTempC(),
		}
		if i, seen := index[v.ID]; seen {
			if !v.EventTime.Before(out[i].EventTime) {
				out[i] = v
			}
			continue
		}
		index[v.ID] = len(out)
		out = append(out, v)
	}
	return out
}
