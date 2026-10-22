// Package sim models simulated vehicles. It is pure logic: no I/O, no
// goroutines, no clock. Callers decide when to call Step and with what dt.
package sim

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// BBox is a latitude/longitude rectangle that vehicles stay inside.
type BBox struct {
	MinLat, MinLon, MaxLat, MaxLon float64
}

// London is the default area vehicles drive in.
var London = BBox{MinLat: 51.28, MinLon: -0.51, MaxLat: 51.69, MaxLon: 0.33}

// Contains reports whether the point is inside the box, edges included.
func (b BBox) Contains(lat, lon float64) bool {
	return lat >= b.MinLat && lat <= b.MaxLat && lon >= b.MinLon && lon <= b.MaxLon
}

// Model limits and rates.
const (
	MaxSpeedKph = 120.0
	MinTempC    = 60.0
	MaxTempC    = 120.0

	kmPerDegLat     = 111.32 // kilometres per degree of latitude
	fuelPctPerKm    = 0.05   // about 2.5 %/h at 50 km/h
	refuelBelowPct  = 5.0    // a vehicle this low "stops at a station" and refills
	idleTempC       = 80.0   // engine temperature at standstill
	tempPerKph      = 0.15   // extra degrees at speed (120 km/h → 98 °C)
	tempSettleSecs  = 30.0   // time for temperature to move most of the way to target
	headingJitter   = 15.0   // degrees of random turning per sqrt(second)
	speedJitterKph  = 4.0    // km/h of random speed change per sqrt(second)
	tempJitterC     = 0.2    // sensor noise per reading
	startMinSpeed   = 20.0
	startSpeedRange = 40.0
)

// Vehicle is one simulated vehicle. It is not safe for concurrent use; give
// each goroutine its own Vehicle.
type Vehicle struct {
	ID          string
	Lat, Lon    float64
	HeadingDeg  float64 // 0 = north, 90 = east
	SpeedKph    float64
	FuelPct     float64
	EngineTempC float64

	bbox BBox
	rng  *rand.Rand
}

// NewVehicle places a vehicle at a random point in bbox with a random heading,
// speed and fuel level. The same rng seed always gives the same vehicle.
func NewVehicle(id string, bbox BBox, rng *rand.Rand) *Vehicle {
	return &Vehicle{
		ID:          id,
		Lat:         bbox.MinLat + rng.Float64()*(bbox.MaxLat-bbox.MinLat),
		Lon:         bbox.MinLon + rng.Float64()*(bbox.MaxLon-bbox.MinLon),
		HeadingDeg:  rng.Float64() * 360,
		SpeedKph:    startMinSpeed + rng.Float64()*startSpeedRange,
		FuelPct:     40 + rng.Float64()*60,
		EngineTempC: idleTempC + rng.Float64()*10,
		bbox:        bbox,
		rng:         rng,
	}
}

// NewFleet creates n vehicles with IDs v-00001, v-00002, ... Each vehicle gets
// its own random stream derived from seed, so the fleet is reproducible and
// vehicles can later be stepped from separate goroutines.
func NewFleet(n int, bbox BBox, seed uint64) []*Vehicle {
	fleet := make([]*Vehicle, n)
	for i := range fleet {
		rng := rand.New(rand.NewPCG(seed, uint64(i)))
		fleet[i] = NewVehicle(fmt.Sprintf("v-%05d", i+1), bbox, rng)
	}
	return fleet
}

// Step advances the vehicle by dt: it turns a little, changes speed a little,
// moves, burns fuel and lets the engine temperature drift. Vehicles bounce off
// the edges of their bounding box. A dt of zero or less does nothing.
func (v *Vehicle) Step(dt time.Duration) {
	secs := dt.Seconds()
	if secs <= 0 {
		return
	}
	scale := math.Sqrt(secs) // random-walk noise grows with the square root of time

	v.HeadingDeg = normDeg(v.HeadingDeg + v.rng.NormFloat64()*headingJitter*scale)
	v.SpeedKph = clamp(v.SpeedKph+v.rng.NormFloat64()*speedJitterKph*scale, 0, MaxSpeedKph)

	km := v.SpeedKph * secs / 3600
	v.move(km)

	v.FuelPct = math.Max(0, v.FuelPct-km*fuelPctPerKm)
	if v.FuelPct < refuelBelowPct {
		v.FuelPct = 100
	}

	target := idleTempC + v.SpeedKph*tempPerKph
	settle := math.Min(1, secs/tempSettleSecs)
	v.EngineTempC += (target-v.EngineTempC)*settle + v.rng.NormFloat64()*tempJitterC
	v.EngineTempC = clamp(v.EngineTempC, MinTempC, MaxTempC)
}

// move travels km along the current heading, reflecting off the box edges.
func (v *Vehicle) move(km float64) {
	rad := v.HeadingDeg * math.Pi / 180
	lat := v.Lat + km*math.Cos(rad)/kmPerDegLat
	lon := v.Lon + km*math.Sin(rad)/(kmPerDegLat*math.Cos(v.Lat*math.Pi/180))

	b := v.bbox
	if lat < b.MinLat || lat > b.MaxLat {
		v.HeadingDeg = normDeg(180 - v.HeadingDeg) // flip north/south
		lat = reflect(lat, b.MinLat, b.MaxLat)
	}
	if lon < b.MinLon || lon > b.MaxLon {
		v.HeadingDeg = normDeg(-v.HeadingDeg) // flip east/west
		lon = reflect(lon, b.MinLon, b.MaxLon)
	}
	v.Lat, v.Lon = lat, lon
}

// reflect mirrors x back inside [lo, hi] and clamps it in case a single step
// was longer than the box is wide.
func reflect(x, lo, hi float64) float64 {
	if x < lo {
		x = lo + (lo - x)
	} else if x > hi {
		x = hi - (x - hi)
	}
	return clamp(x, lo, hi)
}

func clamp(x, lo, hi float64) float64 {
	return math.Max(lo, math.Min(hi, x))
}

// normDeg maps any angle to [0, 360).
func normDeg(d float64) float64 {
	d = math.Mod(d, 360)
	if d < 0 {
		d += 360
	}
	return d
}
