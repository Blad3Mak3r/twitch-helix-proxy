package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

// TwitchRateLimiter with continuous refill token bucket algorithm
type TwitchRateLimiter struct {
	mu              sync.RWMutex
	tokensRemaining int
	resetTime       time.Time
	bucketCapacity  int
	minBuffer       int
	refillRate      float64 // tokens per second (800/60 = 13.33)

	// Tracking to detect stale responses
	bucketID        string // Unique identifier for current bucket (reset timestamp)
	lowestRemaining int    // Lowest value seen in current bucket
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
		bucketID:        fmt.Sprintf("%d", now.Unix()),
		lowestRemaining: 800,
		lastUpdate:      now,
	}
}

// UpdateFromHeaders updates only if data is more recent
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

	// Ratelimit-Reset is a Unix epoch timestamp
	resetUnix, err := strconv.ParseInt(reset, 10, 64)
	if err != nil {
		log.Printf("⚠️ Invalid reset timestamp: %s", reset)
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Verificar que el timestamp de reset sea razonable
	now := time.Now()
	if resetUnix < now.Unix() {
		log.Printf("⚠️ Received past reset time, ignoring: %d", resetUnix)
		return
	}

	// Calculate reset time from Unix timestamp
	newResetTime := time.Unix(resetUnix, 0)
	resetDuration := time.Until(newResetTime)

	// Si el tiempo hasta el reset es mayor a 2 minutos, probablemente hay un error
	if resetDuration > 2*time.Minute {
		log.Printf("⚠️ Suspicious reset duration (%.1f minutes), using 1 minute", resetDuration.Minutes())
		newResetTime = now.Add(time.Minute)
	}

	// Generate bucket ID from reset timestamp
	newBucketID := fmt.Sprintf("%d", resetUnix)

	// Update last update time
	rl.lastUpdate = time.Now()

	// Case 1: New bucket detected (different reset timestamp)
	if newBucketID != rl.bucketID {
		// Check if this is actually a newer bucket
		if newResetTime.After(rl.resetTime) {
			resetIn := time.Until(newResetTime).Round(time.Second)
			log.Printf("🔄 New bucket detected: reset in %.0f seconds (%s)",
				resetIn.Seconds(), newResetTime.Format("15:04:05"))
			rl.resetTime = newResetTime
			rl.bucketID = newBucketID
			rl.tokensRemaining = rem
			rl.lowestRemaining = rem

			if limit != "" {
				if lim, err := strconv.Atoi(limit); err == nil && lim > 0 {
					rl.bucketCapacity = lim
					// Update refill rate based on bucket capacity
					rl.refillRate = float64(lim) / 60.0
				} else if err != nil {
					log.Printf("⚠️ Invalid limit value: %s", limit)
				}
			}

			log.Printf("📊 Bucket reset: %d/%d tokens available (refill: %.2f/s)",
				rem, rl.bucketCapacity, rl.refillRate)
			return
		}

		// Old bucket response, ignore
		log.Printf("⏪ Old bucket response ignored (reset in %.0f seconds)",
			time.Until(newResetTime).Seconds())
	}

	// Case 2: Same bucket, more recent response (lower remaining)
	if rem < rl.lowestRemaining {
		log.Printf("🔽 Valid update: tokens %d → %d (same bucket)",
			rl.tokensRemaining, rem)
		rl.tokensRemaining = rem
		rl.lowestRemaining = rem
		return
	}

	// Case 3: Stale response (higher remaining than minimum seen)
	if rem > rl.lowestRemaining {
		log.Printf("⏪ Stale response ignored: remaining=%d (current=%d)",
			rem, rl.lowestRemaining)
		return
	}
}

// Acquire uses RWMutex to allow concurrent reads
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
				rl.bucketID = fmt.Sprintf("%d", rl.resetTime.Unix())
				rl.lowestRemaining = rl.bucketCapacity
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
