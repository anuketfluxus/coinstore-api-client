package coinstore

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
)

type requestKey struct {
	topic string

	pair string
}

func channelRequestKey(channel string) requestKey {
	pair, topic, _ := strings.Cut(channel, "@")
	return requestKey{topic: topic, pair: pair}
}

var subscriptionListKey = requestKey{topic: listRequestKey}

func (k requestKey) matchKey() string {
	if k.pair == "" {
		return k.topic
	}
	return k.pair + "@" + k.topic
}

func (k requestKey) conflicts(other requestKey, names *pairNames) bool {
	return k.topic == other.topic && names.mayBeSame(k.pair, other.pair)
}

var errRequestAborted = errors.New("coinstore: stream: request aborted")

type requestScheduler struct {
	names *pairNames

	mu          sync.Mutex
	outstanding []requestKey
	waiting     []*requestTicket
}

type requestTicket struct {
	key      requestKey
	admitted chan struct{}
}

func (rs *requestScheduler) enter(ctx context.Context, abort <-chan struct{}, key requestKey) (leave func(), err error) {
	ticket := &requestTicket{key: key, admitted: make(chan struct{})}
	rs.mu.Lock()
	rs.waiting = append(rs.waiting, ticket)
	rs.admit()
	rs.mu.Unlock()

	select {
	case <-ticket.admitted:
	case <-abort:
	case <-ctx.Done():
	}

	switch {
	case ctx.Err() != nil:
		err = ctx.Err()
	case isClosed(abort):
		err = errRequestAborted
	default:
		return sync.OnceFunc(func() { rs.leave(key) }), nil
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()
	if isClosed(ticket.admitted) {

		rs.removeOutstanding(key)
	} else {
		rs.waiting = slices.DeleteFunc(rs.waiting, func(t *requestTicket) bool { return t == ticket })
	}
	rs.admit()
	return nil, err
}

func (rs *requestScheduler) leave(key requestKey) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.removeOutstanding(key)
	rs.admit()
}

func (rs *requestScheduler) reconsider() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.admit()
}

func (rs *requestScheduler) removeOutstanding(key requestKey) {
	if i := slices.Index(rs.outstanding, key); i >= 0 {
		rs.outstanding = slices.Delete(rs.outstanding, i, i+1)
	}
}

func (rs *requestScheduler) admit() {
	stillWaiting := rs.waiting[:0]
	for _, ticket := range rs.waiting {
		conflicting := func(other requestKey) bool { return ticket.key.conflicts(other, rs.names) }
		blocked := slices.ContainsFunc(rs.outstanding, conflicting) ||
			slices.ContainsFunc(stillWaiting, func(ahead *requestTicket) bool { return conflicting(ahead.key) })
		if blocked {
			stillWaiting = append(stillWaiting, ticket)
			continue
		}
		rs.outstanding = append(rs.outstanding, ticket.key)
		close(ticket.admitted)
	}
	clear(rs.waiting[len(stillWaiting):])
	rs.waiting = stillWaiting
}
