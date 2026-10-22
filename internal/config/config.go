// Package config loads runtime settings from environment variables.
//
// Every setting has a default that works for Go processes run on the host
// against `make up`. Containers override them through the Compose file.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds settings for both binaries; each uses the fields it needs.
type Config struct {
	LogLevel slog.Level // LOG_LEVEL: debug, info, warn, error

	KafkaBrokers []string // KAFKA_BROKERS: comma-separated host:port list
	KafkaTopic   string   // KAFKA_TOPIC
	KafkaGroup   string   // KAFKA_GROUP: consumer group for the server

	PostgresDSN string // POSTGRES_DSN

	HTTPAddr string // HTTP_ADDR: server listen address

	SimVehicles int           // SIM_VEHICLES: number of simulated vehicles
	SimInterval time.Duration // SIM_INTERVAL: time between readings per vehicle
}

// Defaults returns the configuration used when no env vars are set.
func Defaults() Config {
	return Config{
		LogLevel:     slog.LevelInfo,
		KafkaBrokers: []string{"localhost:19092"},
		KafkaTopic:   "positions",
		KafkaGroup:   "fleet-server",
		PostgresDSN:  "postgres://fleet:fleet@localhost:5432/fleet?sslmode=disable",
		HTTPAddr:     ":8080",
		SimVehicles:  1000,
		SimInterval:  time.Second,
	}
}

// Load reads the configuration from the process environment.
func Load() (Config, error) {
	return LoadFrom(os.LookupEnv)
}

// LoadFrom reads the configuration using lookup, so tests can supply a fake
// environment. It reports every invalid variable at once, not just the first.
func LoadFrom(lookup func(string) (string, bool)) (Config, error) {
	c := Defaults()
	var errs []error

	get := func(key string) (string, bool) {
		v, ok := lookup(key)
		v = strings.TrimSpace(v)
		return v, ok && v != ""
	}

	if v, ok := get("LOG_LEVEL"); ok {
		if err := c.LogLevel.UnmarshalText([]byte(v)); err != nil {
			errs = append(errs, fmt.Errorf("LOG_LEVEL=%q: want debug, info, warn or error", v))
		}
	}
	if v, ok := get("KAFKA_BROKERS"); ok {
		brokers := splitList(v)
		if len(brokers) == 0 {
			errs = append(errs, fmt.Errorf("KAFKA_BROKERS=%q: no brokers listed", v))
		} else {
			c.KafkaBrokers = brokers
		}
	}
	if v, ok := get("KAFKA_TOPIC"); ok {
		c.KafkaTopic = v
	}
	if v, ok := get("KAFKA_GROUP"); ok {
		c.KafkaGroup = v
	}
	if v, ok := get("POSTGRES_DSN"); ok {
		c.PostgresDSN = v
	}
	if v, ok := get("HTTP_ADDR"); ok {
		c.HTTPAddr = v
	}
	if v, ok := get("SIM_VEHICLES"); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			errs = append(errs, fmt.Errorf("SIM_VEHICLES=%q: want a positive integer", v))
		} else {
			c.SimVehicles = n
		}
	}
	if v, ok := get("SIM_INTERVAL"); ok {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("SIM_INTERVAL=%q: want a positive duration such as 1s or 500ms", v))
		} else {
			c.SimInterval = d
		}
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return c, nil
}

// splitList splits a comma-separated value, dropping blanks and whitespace.
func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
