package main

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"
)

// TwitchRateLimiter implements Twitch's token bucket algorithm
type TwitchRateLimiter struct {
	mu              sync.RWMutex
	tokensRemaining int
	resetTime       time.Time
	bucketCapacity  int
	minBuffer       int
	refillRate      float64 // tokens per second
	lastUpdate      time.Time
}

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

// UpdateFromHeaders updates the rate limiter state from Twitch API response headers
func (rl *TwitchRateLimiter) UpdateFromHeaders(remaining, limit, reset string) {
	if remaining == "" || reset == "" {
		return
	}

	rem, err := strconv.Atoi(remaining)
	if err != nil {
		log.Printf("⚠️ Invalid remaining value: %s", remaining)
		return
	}

	// Validate that remaining is non-negative
	if rem < 0 {
		log.Printf("⚠️ Negative remaining value: %d", rem)
		return
	}

	// Parse reset timestamp
	resetUnix, err := strconv.ParseInt(reset, 10, 64)
	if err != nil {
		log.Printf("⚠️ Invalid reset timestamp: %s", reset)
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	newResetTime := time.Unix(resetUnix, 0)

	// Reject timestamps that are too far in the past (more than 5 seconds)
	if newResetTime.Before(now.Add(-5 * time.Second)) {
		log.Printf("⚠️ Received past reset time, ignoring: %d (%.1f seconds ago)",
			resetUnix, now.Sub(newResetTime).Seconds())
		return
	}

	// Update bucket capacity if limit is provided
	if limit != "" {
		if lim, err := strconv.Atoi(limit); err == nil && lim > 0 {
			rl.bucketCapacity = lim
			rl.refillRate = float64(lim) / 60.0
		}
	}

	// Always update with the latest information from Twitch
	oldRemaining := rl.tokensRemaining
	rl.tokensRemaining = rem
	rl.resetTime = newResetTime
	rl.lastUpdate = now

	// Log the update
	resetIn := time.Until(newResetTime).Round(time.Second)
	if oldRemaining != rem {
		log.Printf("� Rate limit updated: %d → %d tokens (reset in %.0f seconds)",
			oldRemaining, rem, resetIn.Seconds())
	}
} // Acquire uses RWMutex to allow concurrent reads
func (rl *TwitchRateLimiter) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rl.mu.RLock()

		// Check if bucket has reset
		now := time.Now()
		if now.After(rl.resetTime) {
			rl.mu.RUnlock()

			// Upgrade to write lock to reset
			rl.mu.Lock()
			// Double-check after acquiring write lock
			if time.Now().After(rl.resetTime) {
				rl.tokensRemaining = rl.bucketCapacity
				rl.resetTime = time.Now().Add(time.Minute)
				rl.lastUpdate = time.Now()
				log.Printf("🔄 Bucket auto-reset: %d tokens", rl.bucketCapacity)
			}
			rl.mu.Unlock()
			continue
		}

		// Simulate continuous refill based on time elapsed
		// This is an optimistic local estimation between Twitch updates
		elapsed := now.Sub(rl.lastUpdate).Seconds()
		estimatedRefill := int(elapsed * rl.refillRate)
		estimatedTokens := rl.tokensRemaining + estimatedRefill
		if estimatedTokens > rl.bucketCapacity {
			estimatedTokens = rl.bucketCapacity
		}

		// If estimated tokens are sufficient, allow
		if estimatedTokens > rl.minBuffer {
			rl.mu.RUnlock()

			// Decrement with write lock
			rl.mu.Lock()
			// Recalculate with write lock held
			elapsed = time.Since(rl.lastUpdate).Seconds()
			estimatedRefill = int(elapsed * rl.refillRate)
			estimatedTokens = rl.tokensRemaining + estimatedRefill
			if estimatedTokens > rl.bucketCapacity {
				estimatedTokens = rl.bucketCapacity
			}

			if estimatedTokens > rl.minBuffer {
				// Don't update tokensRemaining here - let Twitch headers be the source of truth
				// Just record that we made a request
				rl.mu.Unlock()
				return nil
			}
			rl.mu.Unlock()
			continue
		}

		// No tokens, calculate wait time
		waitUntil := rl.resetTime
		tokensLeft := rl.tokensRemaining
		rl.mu.RUnlock()

		waitDuration := time.Until(waitUntil)
		if waitDuration < 0 {
			continue // Bucket should reset, retry
		}

		log.Printf("⏸️  Rate limit: %d tokens remaining (estimated), waiting %.1fs until reset (%s)",
			tokensLeft, waitDuration.Seconds(), waitUntil.Format("15:04:05"))

		// Wait with cancellation
		timer := time.NewTimer(waitDuration)
		select {
		case <-timer.C:
			continue
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// GetStatus returns current state
func (rl *TwitchRateLimiter) GetStatus() (remaining int, resetIn time.Duration) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.tokensRemaining, time.Until(rl.resetTime)
}
