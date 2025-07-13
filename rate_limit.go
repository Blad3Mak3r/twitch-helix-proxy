package main

import (
	"net/http"
	"strconv"
	"time"
)

func updateRateLimit(h http.Header) {
	state.RateLimit.Mutex.Lock()
	defer state.RateLimit.Mutex.Unlock()

	if limit := h.Get("Ratelimit-Limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			state.RateLimit.RateLimit = l
		}
	}
	if remaining := h.Get("Ratelimit-Remaining"); remaining != "" {
		if r, err := strconv.Atoi(remaining); err == nil {
			state.RateLimit.RateRemaining = r
		}
	}
	if reset := h.Get("Ratelimit-Reset"); reset != "" {
		if ts, err := strconv.ParseInt(reset, 10, 64); err == nil {
			state.RateLimit.RateReset = time.Unix(ts, 0)
		}
	}
}
