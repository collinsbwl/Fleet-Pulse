package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/collinsbwl/Fleet-Pulse/internal/store"
	"github.com/collinsbwl/Fleet-Pulse/internal/wire"
)

const vehiclesTimeout = 5 * time.Second

// VehicleSource is where the latest reading of every vehicle comes from;
// *store.Store satisfies it.
type VehicleSource interface {
	Latest(ctx context.Context) ([]store.Vehicle, error)
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

		out := make([]wire.Vehicle, len(latest))
		for i, v := range latest {
			out[i] = wire.Vehicle{
				ID: v.ID, EventTime: v.EventTime.UTC(), Lat: v.Lat, Lon: v.Lon,
				SpeedKph: v.SpeedKph, FuelPct: v.FuelPct, EngineTempC: v.EngineTempC,
			}
		}
		writeJSON(w, http.StatusOK, out)
	})
}
