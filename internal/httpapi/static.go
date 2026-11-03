package httpapi

import (
	"encoding/json"
	"io/fs"
	"net/http"
)

// RegisterStatic serves the map page at / and the browser settings at
// GET /config. The Mapbox token comes from configuration, so it never appears
// in the repo or the page source. Only a public pk. token belongs here.
func RegisterStatic(mux *http.ServeMux, files fs.FS, mapboxToken string) {
	page := http.FileServerFS(files)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/map.html"
		w.Header().Set("Cache-Control", "no-cache")
		page.ServeHTTP(w, r)
	})

	body, _ := json.Marshal(map[string]string{"mapbox_token": mapboxToken})
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(body)
	})
}
