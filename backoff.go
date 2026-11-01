package coinstore

import (
	"math/rand/v2"
	"time"
)

type backoff struct {
	minDelay time.Duration
	maxDelay time.Duration
	attempts int
}

func (b *backoff) next() time.Duration {
	delay := b.minDelay
	for range b.attempts {
		if delay >= b.maxDelay/2 {
			delay = b.maxDelay
			break
		}
		delay *= 2
	}
	delay = min(delay, b.maxDelay)
	b.attempts++

	half := delay / 2
	return delay - half + rand.N(half+1)
}

func (b *backoff) reset() {
	b.attempts = 0
}
