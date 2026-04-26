package main

import (
	"log/slog"
	"os"
)

// logger is the shared structured logger used across all components.
// It is initialised to the default text handler at package init so that
// tests do not need to call initLogger() explicitly.
// Call initLogger() in main() to respect the LOG_FORMAT environment variable.
var logger = slog.Default()

// initLogger creates the package-level logger.
// Set LOG_FORMAT=json for JSON-lines output (e.g. in production / log aggregators).
// The default is human-readable text, suitable for local development.
func initLogger() {
	format := os.Getenv("LOG_FORMAT")
	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		handler = slog.NewTextHandler(os.Stdout, nil)
	}
	logger = slog.New(handler)
}
