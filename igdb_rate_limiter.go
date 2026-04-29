package main

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type IgdbRateLimiter struct {
	mu              sync.Mutex
	tokensRemaining int
	lastRefill      time.Time
	bucketCapacity  int
	refillRate      float64
}

func NewIgdbRateLimiter() *IgdbRateLimiter {
	now := time.Now()
	return &IgdbRateLimiter{
		tokensRemaining: 8,
		lastRefill:      now,
		bucketCapacity:  8,
		refillRate:      4.0,
	}
}

func (rl *IgdbRateLimiter) UpdateFromHeaders(h http.Header) {
	remaining := h.Get("X-RateLimit-Remaining")
	limit := h.Get("X-RateLimit-Limit")
	reset := h.Get("X-RateLimit-Reset")

	if remaining == "" || reset == "" {
		return
	}

	rem, err := strconv.Atoi(remaining)
	if err != nil {
		logger.Warn("invalid IGDB X-RateLimit-Remaining value", "value", remaining)
		return
	}

	resetUnix, err := strconv.ParseInt(reset, 10, 64)
	if err != nil {
		logger.Warn("invalid IGDB X-RateLimit-Reset timestamp", "value", reset)
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	newResetTime := time.Unix(resetUnix, 0)

	if newResetTime.Before(now.Add(-5 * time.Second)) {
		logger.Warn("received stale IGDB X-RateLimit-Reset, ignoring",
			"reset_unix", resetUnix,
			"seconds_ago", now.Sub(newResetTime).Seconds())
		return
	}

	if limit != "" {
		if lim, err := strconv.Atoi(limit); err == nil && lim > 0 {
			rl.bucketCapacity = lim
			rl.refillRate = float64(lim)
		}
	}

	rl.tokensRemaining = rem
	rl.lastRefill = now

	logger.Debug("IGDB rate limit updated",
		"tokens_remaining", rem,
		"reset_in_seconds", time.Until(newResetTime).Round(time.Second).Seconds())
}

func (rl *IgdbRateLimiter) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rl.mu.Lock()

		now := time.Now()
		elapsed := now.Sub(rl.lastRefill).Seconds()
		tokensToAdd := elapsed * rl.refillRate

		if tokensToAdd >= 1 {
			rl.tokensRemaining = min(rl.bucketCapacity, rl.tokensRemaining+int(tokensToAdd))
			rl.lastRefill = now
		}

		if rl.tokensRemaining > 0 {
			rl.tokensRemaining--
			rl.mu.Unlock()
			return nil
		}

		waitDuration := time.Duration(1 / rl.refillRate * float64(time.Second))
		if waitDuration <= 0 {
			waitDuration = 250 * time.Millisecond
		}

		rl.mu.Unlock()

		logger.Warn("IGDB rate limit exhausted, waiting for refill",
			"wait_seconds", waitDuration.Seconds())

		timer := time.NewTimer(waitDuration)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func (rl *IgdbRateLimiter) Status() LimiterStatus {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	elapsed := time.Since(rl.lastRefill).Seconds()
	tokensToAdd := elapsed * rl.refillRate
	currentTokens := rl.tokensRemaining + int(tokensToAdd)
	if currentTokens > rl.bucketCapacity {
		currentTokens = rl.bucketCapacity
	}

	refillIn := time.Duration(1 / rl.refillRate * float64(time.Second))
	return LimiterStatus{
		Name:            "igdb",
		TokensRemaining: currentTokens,
		ResetIn:         refillIn,
	}
}