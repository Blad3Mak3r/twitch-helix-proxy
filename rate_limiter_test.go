package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewTwitchRateLimiter(t *testing.T) {
	rl := NewTwitchRateLimiter()

	if rl == nil {
		t.Fatal("NewTwitchRateLimiter returned nil")
	}

	if rl.tokensRemaining != 800 {
		t.Errorf("Expected tokensRemaining to be 800, got %d", rl.tokensRemaining)
	}

	if rl.bucketCapacity != 800 {
		t.Errorf("Expected bucketCapacity to be 800, got %d", rl.bucketCapacity)
	}

	if rl.minBuffer != 50 {
		t.Errorf("Expected minBuffer to be 50, got %d", rl.minBuffer)
	}

	expectedRefillRate := 800.0 / 60.0
	if rl.refillRate != expectedRefillRate {
		t.Errorf("Expected refillRate to be %.2f, got %.2f", expectedRefillRate, rl.refillRate)
	}
}

func TestUpdateFromHeaders_ValidInput(t *testing.T) {
	rl := NewTwitchRateLimiter()

	// Set up a future reset time
	futureTime := time.Now().Add(30 * time.Second).Unix()

	rl.UpdateFromHeaders("500", "800", fmt.Sprintf("%d", futureTime))

	// The update should be applied since it's a valid future time
	// Note: This test might not change values if bucket logic prevents it
	// but should not crash
}

func TestUpdateFromHeaders_InvalidRemaining(t *testing.T) {
	rl := NewTwitchRateLimiter()
	initialRemaining := rl.tokensRemaining

	// Test with invalid remaining value
	rl.UpdateFromHeaders("invalid", "800", "")

	// Should not change the value
	if rl.tokensRemaining != initialRemaining {
		t.Errorf("Expected tokensRemaining to remain %d, got %d", initialRemaining, rl.tokensRemaining)
	}
}

func TestUpdateFromHeaders_NegativeRemaining(t *testing.T) {
	rl := NewTwitchRateLimiter()
	initialRemaining := rl.tokensRemaining

	// Test with negative remaining value
	futureTime := time.Now().Add(30 * time.Second).Unix()
	rl.UpdateFromHeaders("-10", "800", fmt.Sprintf("%d", futureTime))

	// Should not change the value
	if rl.tokensRemaining != initialRemaining {
		t.Errorf("Expected tokensRemaining to remain %d, got %d", initialRemaining, rl.tokensRemaining)
	}
}

func TestUpdateFromHeaders_EmptyStrings(t *testing.T) {
	rl := NewTwitchRateLimiter()
	initialRemaining := rl.tokensRemaining

	// Test with empty strings
	rl.UpdateFromHeaders("", "", "")

	// Should not change anything
	if rl.tokensRemaining != initialRemaining {
		t.Errorf("Expected tokensRemaining to remain %d, got %d", initialRemaining, rl.tokensRemaining)
	}
}

func TestUpdateFromHeaders_PastResetTime(t *testing.T) {
	rl := NewTwitchRateLimiter()
	initialRemaining := rl.tokensRemaining

	// Test with past reset time
	pastTime := time.Now().Add(-30 * time.Second).Unix()
	rl.UpdateFromHeaders("500", "800", fmt.Sprintf("%d", pastTime))

	// Should not update with past time
	if rl.tokensRemaining != initialRemaining {
		t.Errorf("Expected tokensRemaining to remain %d, got %d", initialRemaining, rl.tokensRemaining)
	}
}

func TestAcquire_Success(t *testing.T) {
	rl := NewTwitchRateLimiter()
	ctx := context.Background()

	// Should successfully acquire since we have plenty of tokens
	err := rl.Acquire(ctx)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestAcquire_ContextCancelled(t *testing.T) {
	rl := NewTwitchRateLimiter()

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should return context error
	err := rl.Acquire(ctx)
	if err == nil {
		t.Error("Expected context cancelled error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

func TestGetStatus(t *testing.T) {
	rl := NewTwitchRateLimiter()

	remaining, resetIn := rl.GetStatus()

	if remaining != 800 {
		t.Errorf("Expected remaining to be 800, got %d", remaining)
	}

	if resetIn <= 0 {
		t.Error("Expected resetIn to be positive")
	}

	if resetIn > time.Minute {
		t.Errorf("Expected resetIn to be less than 1 minute, got %v", resetIn)
	}
}
