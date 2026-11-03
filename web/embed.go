// Package web holds the static map page, compiled into the server binary.
package web

import "embed"

// Files contains map.html.
//
//go:embed map.html
var Files embed.FS
