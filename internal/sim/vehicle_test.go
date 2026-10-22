package sim

import (
	"math"
	"testing"
	"time"
)

// snapshot captures the observable state of a vehicle for comparisons.
type snapshot struct {
	lat, lon, heading, speed, fuel, temp float64
}

func snap(v *Vehicle) snapshot {
	return snapshot{v.Lat, v.Lon, v.HeadingDeg, v.SpeedKph, v.FuelPct, v.EngineTempC}
}

func TestSameSeedSamePath(t *testing.T) {
	a := NewFleet(20, London, 42)
	b := NewFleet(20, London, 42)

	for step := 0; step < 1000; step++ {
		for i := range a {
			a[i].Step(time.Second)
			b[i].Step(time.Second)
			if snap(a[i]) != snap(b[i]) {
				t.Fatalf("vehicle %s diverged at step %d:\n a %+v\n b %+v", a[i].ID, step, snap(a[i]), snap(b[i]))
			}
		}
	}
}

func TestDifferentSeedDifferentPath(t *testing.T) {
	a := NewFleet(1, London, 1)[0]
	b := NewFleet(1, London, 2)[0]
	if snap(a) == snap(b) {
		t.Fatal("different seeds produced the same starting state")
	}
}

func TestFleetIDs(t *testing.T) {
	fleet := NewFleet(3, London, 7)
	want := []string{"v-00001", "v-00002", "v-00003"}
	for i, v := range fleet {
		if v.ID != want[i] {
			t.Errorf("vehicle %d: want ID %s, got %s", i, want[i], v.ID)
		}
	}
}

func TestStaysWithinLimits(t *testing.T) {
	// A tiny box and long steps force many bounces, including steps longer
	// than the box is wide.
	tiny := BBox{MinLat: 51.500, MinLon: -0.130, MaxLat: 51.505, MaxLon: -0.120}

	tests := []struct {
		name  string
		bbox  BBox
		dt    time.Duration
		steps int
	}{
		{"London, 1s steps", London, time.Second, 5000},
		{"tiny box, 1s steps", tiny, time.Second, 5000},
		{"tiny box, 5m steps", tiny, 5 * time.Minute, 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, v := range NewFleet(50, tt.bbox, 99) {
				for step := 0; step < tt.steps; step++ {
					v.Step(tt.dt)
					s := snap(v)
					switch {
					case !tt.bbox.Contains(v.Lat, v.Lon):
						t.Fatalf("%s left the box at step %d: %+v", v.ID, step, s)
					case v.SpeedKph < 0 || v.SpeedKph > MaxSpeedKph:
						t.Fatalf("%s speed out of range at step %d: %+v", v.ID, step, s)
					case v.FuelPct < 0 || v.FuelPct > 100:
						t.Fatalf("%s fuel out of range at step %d: %+v", v.ID, step, s)
					case v.EngineTempC < MinTempC || v.EngineTempC > MaxTempC:
						t.Fatalf("%s engine temp out of range at step %d: %+v", v.ID, step, s)
					case v.HeadingDeg < 0 || v.HeadingDeg >= 360:
						t.Fatalf("%s heading out of range at step %d: %+v", v.ID, step, s)
					}
				}
			}
		})
	}
}

func TestFuelDrainsAndRefuels(t *testing.T) {
	v := NewFleet(1, London, 5)[0]
	v.FuelPct = 6
	v.SpeedKph = 100

	refuelled := false
	for step := 0; step < 3600 && !refuelled; step++ {
		before := v.FuelPct
		v.Step(time.Second)
		if v.FuelPct > before {
			refuelled = true
			if v.FuelPct != 100 {
				t.Fatalf("refuel should fill to 100, got %.2f", v.FuelPct)
			}
		}
	}
	if !refuelled {
		t.Fatal("vehicle never refuelled within an hour of driving")
	}
}

func TestMovesAtPlausibleDistance(t *testing.T) {
	v := NewFleet(1, London, 3)[0]
	lat0, lon0 := v.Lat, v.Lon

	for step := 0; step < 60; step++ {
		lat, lon := v.Lat, v.Lon
		v.Step(time.Second)
		km := distanceKm(lat, lon, v.Lat, v.Lon)
		maxKm := MaxSpeedKph / 3600 * 1.01 // one second at top speed, plus rounding
		if km > maxKm {
			t.Fatalf("step %d moved %.4f km, more than %.4f km possible in 1s", step, km, maxKm)
		}
	}
	if v.Lat == lat0 && v.Lon == lon0 {
		t.Fatal("vehicle did not move in 60 seconds")
	}
}

func TestStepZeroIsNoOp(t *testing.T) {
	v := NewFleet(1, London, 11)[0]
	before := snap(v)
	v.Step(0)
	v.Step(-time.Second)
	if snap(v) != before {
		t.Fatalf("Step with dt <= 0 changed state:\n before %+v\n after  %+v", before, snap(v))
	}
}

// distanceKm is an equirectangular approximation, accurate at city scale.
func distanceKm(lat1, lon1, lat2, lon2 float64) float64 {
	dLat := (lat2 - lat1) * kmPerDegLat
	dLon := (lon2 - lon1) * kmPerDegLat * math.Cos((lat1+lat2)/2*math.Pi/180)
	return math.Hypot(dLat, dLon)
}
