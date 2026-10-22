package config

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"
)

// env returns a lookup function backed by a map, standing in for os.LookupEnv.
func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	got, err := LoadFrom(env(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := Defaults(); !reflect.DeepEqual(got, want) {
		t.Fatalf("defaults mismatch:\nwant %+v\n got %+v", want, got)
	}
}

func TestLoadOverrides(t *testing.T) {
	got, err := LoadFrom(env(map[string]string{
		"LOG_LEVEL":     "debug",
		"KAFKA_BROKERS": " redpanda:9092 , other:9092 ,",
		"KAFKA_TOPIC":   "positions-test",
		"KAFKA_GROUP":   "group-test",
		"POSTGRES_DSN":  "postgres://u:p@db:5432/x",
		"HTTP_ADDR":     ":9999",
		"SIM_VEHICLES":  "50000",
		"SIM_INTERVAL":  "250ms",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := Config{
		LogLevel:     slog.LevelDebug,
		KafkaBrokers: []string{"redpanda:9092", "other:9092"},
		KafkaTopic:   "positions-test",
		KafkaGroup:   "group-test",
		PostgresDSN:  "postgres://u:p@db:5432/x",
		HTTPAddr:     ":9999",
		SimVehicles:  50000,
		SimInterval:  250 * time.Millisecond,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("overrides mismatch:\nwant %+v\n got %+v", want, got)
	}
}

func TestLoadBlankValuesKeepDefaults(t *testing.T) {
	got, err := LoadFrom(env(map[string]string{
		"KAFKA_TOPIC":  "",
		"SIM_VEHICLES": "   ",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	d := Defaults()
	if got.KafkaTopic != d.KafkaTopic || got.SimVehicles != d.SimVehicles {
		t.Fatalf("blank values should keep defaults, got topic=%q vehicles=%d", got.KafkaTopic, got.SimVehicles)
	}
}

func TestLoadInvalid(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr []string // substrings that must all appear in the error
	}{
		{"bad log level", map[string]string{"LOG_LEVEL": "loud"}, []string{"LOG_LEVEL"}},
		{"empty broker list", map[string]string{"KAFKA_BROKERS": " , ,"}, []string{"KAFKA_BROKERS"}},
		{"vehicles not a number", map[string]string{"SIM_VEHICLES": "lots"}, []string{"SIM_VEHICLES"}},
		{"vehicles zero", map[string]string{"SIM_VEHICLES": "0"}, []string{"SIM_VEHICLES"}},
		{"interval not a duration", map[string]string{"SIM_INTERVAL": "1"}, []string{"SIM_INTERVAL"}},
		{"interval negative", map[string]string{"SIM_INTERVAL": "-1s"}, []string{"SIM_INTERVAL"}},
		{
			"reports every problem at once",
			map[string]string{"LOG_LEVEL": "loud", "SIM_VEHICLES": "-5", "SIM_INTERVAL": "soon"},
			[]string{"LOG_LEVEL", "SIM_VEHICLES", "SIM_INTERVAL"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadFrom(env(tt.env))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			for _, s := range tt.wantErr {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("error %q should mention %s", err, s)
				}
			}
		})
	}
}
