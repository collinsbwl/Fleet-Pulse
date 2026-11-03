package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func TestStatic(t *testing.T) {
	files := fstest.MapFS{"map.html": {Data: []byte("<h1>map</h1>")}}
	mux := http.NewServeMux()
	RegisterStatic(mux, files, "pk.abc")

	tests := []struct {
		name, method, path string
		wantStatus         int
		wantBody           string
	}{
		{"page at root", "GET", "/", 200, "<h1>map</h1>"},
		{"config has token", "GET", "/config", 200, `{"mapbox_token":"pk.abc"}`},
		{"unknown path is not the page", "GET", "/nope", 404, ""},
		{"POST to root rejected", "POST", "/", 405, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if !strings.Contains(rec.Body.String(), tt.wantBody) {
				t.Fatalf("body = %q, want it to contain %q", rec.Body.String(), tt.wantBody)
			}
		})
	}
}
