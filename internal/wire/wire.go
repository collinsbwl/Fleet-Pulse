// Package wire holds the JSON shape browsers receive. GET /vehicles and the
// WebSocket feed both send Vehicle, so the map can treat them the same way.
package wire

import "time"

// Vehicle is one vehicle's reading as sent to browsers.
type Vehicle struct {
	ID          string    `json:"id"`
	EventTime   time.Time `json:"event_time"` // RFC 3339, UTC
	Lat         float64   `json:"lat"`
	Lon         float64   `json:"lon"`
	SpeedKph    float32   `json:"speed_kph"`
	FuelPct     float32   `json:"fuel_pct"`
	EngineTempC float32   `json:"engine_temp_c"`
}
