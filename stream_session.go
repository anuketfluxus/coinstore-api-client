package coinstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var errClosedByClient = errors.New("connection closed by the client")

var errSessionEnded = errors.New("session ended")

type requestKind int

const (
	klineRequest requestKind = iota
	listRequest
)

type pendingRequest struct {
	kind requestKind
	id   uint64

	key string

	fits func(json.RawMessage) bool

	result chan requestResult

	failed  chan struct{}
	failure error
}

func (pending *pendingRequest) fail(err error) {
	if !isClosed(pending.failed) {
		pending.failure = err
		close(pending.failed)
	}
}

type resolution int

const (
	unmatched resolution = iota

	resolved

	misfit

	ambiguous

	unconfirmed
)

type requestResult struct {
	payload json.RawMessage
	err     error
}

type streamSession struct {
	conn *websocket.Conn

	writeMu sync.Mutex

	closing     chan struct{}
	closingOnce sync.Once

	done             chan struct{}
	terminateOnce    sync.Once
	terminationCause error

	lastSequence int64

	pendingMu sync.Mutex
	pending   []*pendingRequest

	refusal error
}

func newStreamSession(conn *websocket.Conn) *streamSession {
	return &streamSession{conn: conn, closing: make(chan struct{}), done: make(chan struct{})}
}

func (ss *streamSession) send(command any) error {
	payload, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("encode command: %w", err)
	}
	if err := ss.writeText(payload); err != nil {
		err = fmt.Errorf("send command: %w", err)
		ss.terminate(err)
		return err
	}
	return nil
}

func (ss *streamSession) writeText(payload []byte) error {
	ss.writeMu.Lock()
	defer ss.writeMu.Unlock()
	if err := ss.conn.SetWriteDeadline(time.Now().Add(streamWriteTimeout)); err != nil {
		return err
	}
	return ss.conn.WriteMessage(websocket.TextMessage, payload)
}

func (ss *streamSession) terminate(cause error) {
	ss.terminateOnce.Do(func() {
		ss.terminationCause = cause
		ss.refuseRequests(ErrStreamDisconnected)
		close(ss.done)
		ss.conn.Close()
	})
}

func (ss *streamSession) cause() error {
	<-ss.done
	return ss.terminationCause
}

func (ss *streamSession) refuseRequests(err error) {
	ss.closingOnce.Do(func() {
		ss.pendingMu.Lock()
		defer ss.pendingMu.Unlock()
		ss.refusal = err

		close(ss.closing)
		for _, pending := range ss.pending {
			pending.result <- requestResult{err: err}
		}
		ss.pending = nil
	})
}

func (ss *streamSession) acceptsRequests() bool {
	return !isClosed(ss.closing)
}

func (ss *streamSession) refusalError() error {
	ss.pendingMu.Lock()
	defer ss.pendingMu.Unlock()
	return ss.refusal
}

func (ss *streamSession) closeGracefully() {
	ss.refuseRequests(ErrStreamStopped)
	message := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
	if err := ss.conn.WriteControl(websocket.CloseMessage, message, time.Now().Add(streamWriteTimeout)); err == nil {
		timer := time.NewTimer(streamCloseTimeout)
		defer timer.Stop()
		select {
		case <-ss.done:
		case <-timer.C:
		}
	}
	ss.terminate(errClosedByClient)
}

func (ss *streamSession) extendReadDeadline(timeout time.Duration) error {
	return ss.conn.SetReadDeadline(time.Now().Add(timeout))
}

func (ss *streamSession) answerPing(appData string, idleTimeout time.Duration) error {
	if err := ss.extendReadDeadline(idleTimeout); err != nil {
		return err
	}
	err := ss.conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(streamWriteTimeout))
	var netErr net.Error
	if err == nil || errors.Is(err, websocket.ErrCloseSent) || (errors.As(err, &netErr) && netErr.Timeout()) {

		return nil
	}
	return err
}

func (ss *streamSession) sendKeepalives(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ss.done:
			return
		case <-ticker.C:
			err := ss.conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(streamWriteTimeout))
			if errors.Is(err, websocket.ErrCloseSent) {
				return
			}
			if err != nil {
				ss.terminate(fmt.Errorf("send keepalive: %w", err))
				return
			}
		}
	}
}

func (ss *streamSession) trackSequence(sequence int64) error {
	if sequence <= 0 {
		return nil
	}
	previous := ss.lastSequence
	ss.lastSequence = sequence
	if previous != 0 && sequence != previous+1 {
		return &SequenceGapError{Expected: previous + 1, Received: sequence}
	}
	return nil
}

func (ss *streamSession) addPending(kind requestKind, id uint64, key string, fits func(json.RawMessage) bool) (*pendingRequest, error) {
	ss.pendingMu.Lock()
	defer ss.pendingMu.Unlock()
	if ss.refusal != nil {
		return nil, ss.refusal
	}
	pending := &pendingRequest{
		kind:   kind,
		id:     id,
		key:    key,
		fits:   fits,
		result: make(chan requestResult, 1),
		failed: make(chan struct{}),
	}
	ss.pending = append(ss.pending, pending)
	return pending, nil
}

func (ss *streamSession) removePending(pending *pendingRequest) {
	ss.pendingMu.Lock()
	defer ss.pendingMu.Unlock()
	ss.pending = slices.DeleteFunc(ss.pending, func(other *pendingRequest) bool {
		return other == pending
	})
}

func (ss *streamSession) resolve(kind requestKind, id uint64, match replyMatch, payload json.RawMessage) (resolution, []string) {
	ss.pendingMu.Lock()
	defer ss.pendingMu.Unlock()
	if id != 0 {
		i := slices.IndexFunc(ss.pending, func(pending *pendingRequest) bool {
			return pending.kind == kind && pending.id == id
		})
		if i < 0 {
			return unmatched, nil
		}
		ss.deliverLocked(ss.pending[i], payload)
		return resolved, nil
	}

	var candidates []*pendingRequest
	named, sawMisfit, sawFailed := 0, false, false
	for _, pending := range ss.pending {
		if pending.kind != kind {
			continue
		}
		isNamed := slices.Contains(match.keys, pending.key)
		if !isNamed && !match.cannotRuleOut(pending.key) {
			continue
		}
		if pending.fits != nil && !pending.fits(payload) {
			sawMisfit = sawMisfit || isNamed
			continue
		}
		candidates = append(candidates, pending)
		if isNamed {
			named++
		}
		sawFailed = sawFailed || isClosed(pending.failed)
	}

	switch {
	case named == 0 && sawMisfit:
		return misfit, nil
	case len(candidates) == 0:
		return unmatched, nil
	case named == 0:

		failAll(candidates, ErrUnconfirmedReply)
		return unconfirmed, keysOf(candidates)
	case len(candidates) == 1 && !sawFailed:
		ss.deliverLocked(candidates[0], payload)
		return resolved, nil
	default:
		failAll(candidates, ErrAmbiguousReply)
		return ambiguous, keysOf(candidates)
	}
}

func failAll(requests []*pendingRequest, err error) {
	for _, request := range requests {
		request.fail(err)
	}
}

type replyMatch struct {
	keys []string

	topic string

	namesID, namesSymbol bool

	placed func(pair string) bool
}

func newReplyMatch(keys []string, placed func(pair string) bool) replyMatch {
	match := replyMatch{keys: keys, placed: placed}
	for _, key := range keys {
		pair, topic, found := strings.Cut(key, "@")
		if !found {
			continue
		}
		match.topic = topic
		if namesPairByID(pair) {
			match.namesID = true
		} else {
			match.namesSymbol = true
		}
	}
	return match
}

func (m replyMatch) cannotRuleOut(key string) bool {
	pair, topic, found := strings.Cut(key, "@")
	if !found || topic != m.topic || m.placed(pair) {
		return false
	}
	if namesPairByID(pair) {
		return !m.namesID
	}
	return !m.namesSymbol
}

func keysOf(requests []*pendingRequest) []string {
	keys := make([]string, len(requests))
	for i, request := range requests {
		keys[i] = request.key
	}
	return keys
}

func (ss *streamSession) deliverLocked(pending *pendingRequest, payload json.RawMessage) {
	ss.pending = slices.DeleteFunc(ss.pending, func(other *pendingRequest) bool { return other == pending })
	pending.result <- requestResult{payload: payload}
}
