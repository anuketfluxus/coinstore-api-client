package coinstore

import (
	"testing"
	"time"
)

func TestBackoffGrowsToItsMaximum(t *testing.T) {
	b := backoff{minDelay: time.Second, maxDelay: 30 * time.Second}

	wantCaps := []time.Duration{1, 2, 4, 8, 16, 30, 30, 30}
	for attempt, wantCap := range wantCaps {
		wantCap *= time.Second
		for range 100 {
			trial := backoff{minDelay: b.minDelay, maxDelay: b.maxDelay, attempts: attempt}
			got := trial.next()
			if got < wantCap/2 || got > wantCap {
				t.Fatalf("attempt %d: delay %s outside [%s, %s]", attempt, got, wantCap/2, wantCap)
			}
		}
	}
}

func TestBackoffCountsAttemptsAndResets(t *testing.T) {
	b := backoff{minDelay: time.Millisecond, maxDelay: time.Hour}
	for range 3 {
		b.next()
	}
	if b.attempts != 3 {
		t.Fatalf("attempts = %d after three delays, want 3", b.attempts)
	}

	b.reset()

	if got := b.next(); got > time.Millisecond {
		t.Errorf("first delay after reset = %s, want at most 1ms", got)
	}
}

func TestBackoffDoesNotOverflow(t *testing.T) {
	b := backoff{minDelay: time.Second, maxDelay: time.Duration(1<<63 - 1), attempts: 200}
	if got := b.next(); got <= 0 {
		t.Errorf("delay after 200 attempts = %s, want a positive delay", got)
	}
}
