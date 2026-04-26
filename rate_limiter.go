package main

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// TwitchRateLimiter implements Twitch's token bucket algorithm.
//
// Design: Twitch headers are the source of truth for tokensRemaining.
// Between header updates we use local estimation (refillRate) to optimistically
// allow requests. We use a plain Mutex (not RWMutex) because every Acquire call
// that succeeds must mutate state — an RLock→Lock upgrade is always a TOCTOU
// race and provides no real throughput benefit for this workload.
//
// TwitchRateLimiter implements the Limiter interface.
type TwitchRateLimiter struct {
	mu              sync.Mutex
	tokensRemaining int
	resetTime       time.Time
	bucketCapacity  int
	minBuffer       int
	refillRate      float64 // tokens per second
	lastUpdate      time.Time
}

// NewTwitchRateLimiter creates a rate limiter seeded with Twitch's default
// global bucket of 800 requests per minute.
func NewTwitchRateLimiter() *TwitchRateLimiter {
	now := time.Now()
	return &TwitchRateLimiter{
		tokensRemaining: 800,
		resetTime:       now.Add(time.Minute),
		bucketCapacity:  800,
		minBuffer:       50,
		refillRate:      800.0 / 60.0, // ~13.33 tokens/second
		lastUpdate:      now,
	}
}

// UpdateFromHeaders implements Limiter. It updates rate limiter state from the
// Twitch response headers Ratelimit-Remaining, Ratelimit-Limit, and Ratelimit-Reset.
func (rl *TwitchRateLimiter) UpdateFromHeaders(h http.Header) {
	remaining := h.Get("Ratelimit-Remaining")
	limit := h.Get("Ratelimit-Limit")
	reset := h.Get("Ratelimit-Reset")

	if remaining == "" || reset == "" {
		return
	}

	rem, err := strconv.Atoi(remaining)
	if err != nil {
		logger.Warn("invalid Ratelimit-Remaining value", "value", remaining)
		return
	}

	if rem < 0 {
		logger.Warn("negative Ratelimit-Remaining value", "value", rem)
		return
	}

	resetUnix, err := strconv.ParseInt(reset, 10, 64)
	if err != nil {
		logger.Warn("invalid Ratelimit-Reset timestamp", "value", reset)
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	newResetTime := time.Unix(resetUnix, 0)

	// Reject timestamps that are too far in the past (more than 5 seconds).
	// This protects against stale/out-of-order responses.
	if newResetTime.Before(now.Add(-5 * time.Second)) {
		logger.Warn("received stale Ratelimit-Reset, ignoring",
			"reset_unix", resetUnix,
			"seconds_ago", now.Sub(newResetTime).Seconds())
		return
	}

	if limit != "" {
		if lim, err := strconv.Atoi(limit); err == nil && lim > 0 {
			rl.bucketCapacity = lim
			rl.refillRate = float64(lim) / 60.0
		}
	}

	oldRemaining := rl.tokensRemaining
	rl.tokensRemaining = rem
	rl.resetTime = newResetTime
	rl.lastUpdate = now

	if oldRemaining != rem {
		logger.Debug("global rate limit updated",
			"tokens_before", oldRemaining,
			"tokens_after", rem,
			"reset_in_seconds", time.Until(newResetTime).Round(time.Second).Seconds())
	}
}

// Acquire implements Limiter. It blocks until a token is available or ctx is cancelled.
//
// Uses a single Mutex to avoid the TOCTOU race of RLock→Lock upgrades.
// Local token estimation between Twitch header updates keeps the limiter
// responsive without waiting for the next API response.
func (rl *TwitchRateLimiter) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rl.mu.Lock()

		now := time.Now()

		// Auto-reset when the Twitch bucket window has elapsed.
		// This is a fallback — normally UpdateFromHeaders drives the state.
		if now.After(rl.resetTime) {
			rl.tokensRemaining = rl.bucketCapacity
			rl.resetTime = now.Add(time.Minute)
			rl.lastUpdate = now
			logger.Debug("global bucket auto-reset", "tokens", rl.bucketCapacity)
		}

		// Optimistic local estimation: add tokens accrued since last Twitch update.
		elapsed := now.Sub(rl.lastUpdate).Seconds()
		estimatedTokens := rl.tokensRemaining + int(elapsed*rl.refillRate)
		if estimatedTokens > rl.bucketCapacity {
			estimatedTokens = rl.bucketCapacity
		}

		if estimatedTokens > rl.minBuffer {
			// Consume one token from the local estimate and record the time.
			// Twitch headers will correct any drift on the next response.
			rl.tokensRemaining = estimatedTokens - 1
			rl.lastUpdate = now
			rl.mu.Unlock()
			return nil
		}

		// Not enough tokens — capture wait info and release the lock before sleeping.
		waitUntil := rl.resetTime
		tokensLeft := rl.tokensRemaining
		rl.mu.Unlock()

		waitDuration := time.Until(waitUntil)
		if waitDuration <= 0 {
			// resetTime has passed; loop back to auto-reset on next iteration.
			continue
		}

		logger.Warn("global rate limit exhausted, waiting for reset",
			"tokens_remaining", tokensLeft,
			"wait_seconds", waitDuration.Seconds(),
			"reset_at", waitUntil.Format("15:04:05"))

		// Wait with context cancellation support — no bare time.Sleep.
		timer := time.NewTimer(waitDuration)
		select {
		case <-timer.C:
			// continue to re-evaluate
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// Status implements Limiter. It returns current rate limit state for the /status endpoint.
func (rl *TwitchRateLimiter) Status() LimiterStatus {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return LimiterStatus{
		Name:            "global",
		TokensRemaining: rl.tokensRemaining,
		ResetIn:         time.Until(rl.resetTime),
	}
}

// GetStatus returns the current token count and time until reset.
// Kept for backwards compatibility with existing tests.
func (rl *TwitchRateLimiter) GetStatus() (remaining int, resetIn time.Duration) {
	s := rl.Status()
	return s.TokensRemaining, s.ResetIn
}
