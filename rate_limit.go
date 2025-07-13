package main

import (
	"net/http"
	"strconv"
	"time"
)

func updateRateLimit(h http.Header) {
	rateMu.Lock()
	defer rateMu.Unlock()

	if limit := h.Get("Ratelimit-Limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			rateLimit = l
		}
	}
	if remaining := h.Get("Ratelimit-Remaining"); remaining != "" {
		if r, err := strconv.Atoi(remaining); err == nil {
			rateRemaining = r
		}
	}
	if reset := h.Get("Ratelimit-Reset"); reset != "" {
		if ts, err := strconv.ParseInt(reset, 10, 64); err == nil {
			rateReset = time.Unix(ts, 0)
		}
	}
}
