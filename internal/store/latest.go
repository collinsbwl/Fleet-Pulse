package store

import (
	"context"
	"fmt"
	"time"
)

// Vehicle is one row of vehicles_latest: a vehicle's most recent reading.
type Vehicle struct {
	ID          string
	EventTime   time.Time // when the vehicle took the reading
	Lat, Lon    float64
	SpeedKph    float32
	FuelPct     float32
	EngineTempC float32
}

// Latest returns every vehicle's most recent reading, ordered by ID so the
// result is stable between calls. It returns an empty, non-nil slice when there
// are no vehicles. It is the map's first load, before WebSocket updates take over.
func (s *Store) Latest(ctx context.Context) ([]Vehicle, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT vehicle_id, event_ts, lat, lon, speed_kph, fuel_pct, engine_temp_c
		FROM vehicles_latest
		ORDER BY vehicle_id`)
	if err != nil {
		return nil, fmt.Errorf("querying vehicles_latest: %w", err)
	}
	defer rows.Close()

	vehicles := []Vehicle{}
	for rows.Next() {
		var v Vehicle
		if err := rows.Scan(&v.ID, &v.EventTime, &v.Lat, &v.Lon, &v.SpeedKph, &v.FuelPct, &v.EngineTempC); err != nil {
			return nil, fmt.Errorf("reading a vehicles_latest row: %w", err)
		}
		v.EventTime = v.EventTime.UTC()
		vehicles = append(vehicles, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading vehicles_latest: %w", err)
	}
	return vehicles, nil
}
