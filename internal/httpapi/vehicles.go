package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/collinsbwl/Fleet-Pulse/internal/store"
)

const vehiclesTimeout = 5 * time.Second

// VehicleSource is where the latest reading of every vehicle comes from;
// *store.Store satisfies it.
type VehicleSource interface {
	Latest(ctx context.Context) ([]store.Vehicle, error)
}

// vehicleJSON is the wire shape of one vehicle. It is kept apart from
// store.Vehicle so renaming a database column never changes the API.
type vehicleJSON struct {
	ID          string    `json:"id"`
	EventTime   time.Time `json:"event_time"` // RFC 3339, UTC
	Lat         float64   `json:"lat"`
	Lon         float64   `json:"lon"`
	SpeedKph    float32   `json:"speed_kph"`
	FuelPct     float32   `json:"fuel_pct"`
	EngineTempC float32   `json:"engine_temp_c"`
}

// RegisterVehicles adds GET /vehicles to mux. It returns a JSON array with every
// vehicle's most recent reading, ordered by ID: the map's first load, before
// WebSocket updates take over. An empty fleet is [], never null.
func RegisterVehicles(mux *http.ServeMux, src VehicleSource, log *slog.Logger) {
	mux.HandleFunc("GET /vehicles", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), vehiclesTimeout)
		defer cancel()

		latest, err := src.Latest(ctx)
		if err != nil {
			// Log the cause; tell the client nothing that could leak internals.
			log.Error("loading vehicles failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not load vehicles"})
			return
		}

		out := make([]vehicleJSON, len(latest))
		for i, v := range latest {
			out[i] = vehicleJSON{
				ID: v.ID, EventTime: v.EventTime.UTC(), Lat: v.Lat, Lon: v.Lon,
				SpeedKph: v.SpeedKph, FuelPct: v.FuelPct, EngineTempC: v.EngineTempC,
			}
		}
		writeJSON(w, http.StatusOK, out)
	})
}
