package coinstore

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const testWait = 5 * time.Second

type fakeStreamServer struct {
	server *httptest.Server
	url    string
	conns  chan *fakeStreamConn

	rejections atomic.Int32

	mu       sync.Mutex
	accepted []*fakeStreamConn
}

func newFakeStreamServer(t *testing.T) *fakeStreamServer {
	t.Helper()
	srv := &fakeStreamServer{conns: make(chan *fakeStreamConn, 16)}
	upgrader := websocket.Upgrader{}
	srv.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if srv.rejections.Add(-1) >= 0 {
			http.Error(w, "connection refused by test", http.StatusForbidden)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fc := newFakeStreamConn(conn)
		srv.mu.Lock()
		srv.accepted = append(srv.accepted, fc)
		srv.mu.Unlock()
		srv.conns <- fc
	}))
	srv.url = "ws" + strings.TrimPrefix(srv.server.URL, "http")
	t.Cleanup(func() {
		srv.server.Close()
		srv.mu.Lock()
		defer srv.mu.Unlock()
		for _, fc := range srv.accepted {
			fc.conn.Close()
		}
	})
	return srv
}

func (srv *fakeStreamServer) rejectNext(n int32) {
	srv.rejections.Store(n)
}

func (srv *fakeStreamServer) accept(t *testing.T) *fakeStreamConn {
	t.Helper()
	select {
	case fc := <-srv.conns:
		return fc
	case <-time.After(testWait):
		t.Fatal("timed out waiting for the client to connect")
		return nil
	}
}

type fakeStreamConn struct {
	conn     *websocket.Conn
	writeMu  sync.Mutex
	commands chan []byte
	pongs    chan string

	closed   chan struct{}
	closeErr error
}

func newFakeStreamConn(conn *websocket.Conn) *fakeStreamConn {
	fc := &fakeStreamConn{
		conn:     conn,
		commands: make(chan []byte, 64),
		pongs:    make(chan string, 64),
		closed:   make(chan struct{}),
	}
	conn.SetPongHandler(func(appData string) error {
		select {
		case fc.pongs <- appData:
		default:
		}
		return nil
	})
	go fc.readCommands()
	return fc
}

func (fc *fakeStreamConn) readCommands() {
	defer close(fc.closed)
	for {
		_, payload, err := fc.conn.ReadMessage()
		if err != nil {
			fc.closeErr = err
			return
		}
		fc.commands <- payload
	}
}

type testCommand struct {
	Op      string          `json:"op"`
	Channel []string        `json:"channel"`
	Param   json.RawMessage `json:"param"`
	ID      uint64          `json:"id"`
	raw     string
}

func (fc *fakeStreamConn) expectCommand(t *testing.T) testCommand {
	t.Helper()
	select {
	case payload := <-fc.commands:
		command := testCommand{raw: string(payload)}
		if err := json.Unmarshal(payload, &command); err != nil {
			t.Fatalf("client sent an undecodable command %s: %v", payload, err)
		}
		return command
	case <-time.After(testWait):
		t.Fatal("timed out waiting for a command from the client")
		return testCommand{}
	}
}

func (fc *fakeStreamConn) expectSubscribe(t *testing.T, channel string) testCommand {
	t.Helper()
	command := fc.expectCommand(t)
	if command.Op != opSubscribe || len(command.Channel) != 1 || command.Channel[0] != channel || command.Param != nil {
		t.Fatalf("client sent %s, want a SUB command for %s", command.raw, channel)
	}
	return command
}

func (fc *fakeStreamConn) expectNoCommand(t *testing.T) {
	t.Helper()
	select {
	case payload := <-fc.commands:
		t.Fatalf("client sent unexpected command %s", payload)
	case <-time.After(50 * time.Millisecond):
	}
}

func (fc *fakeStreamConn) send(t *testing.T, message string) {
	t.Helper()
	if err := fc.write(message); err != nil {
		t.Fatalf("send %s: %v", message, err)
	}
}

func (fc *fakeStreamConn) write(message string) error {
	fc.writeMu.Lock()
	defer fc.writeMu.Unlock()
	return fc.conn.WriteMessage(websocket.TextMessage, []byte(message))
}

func (fc *fakeStreamConn) awaitClose(t *testing.T) error {
	t.Helper()
	select {
	case <-fc.closed:
		return fc.closeErr
	case <-time.After(testWait):
		t.Fatal("timed out waiting for the connection to close")
		return nil
	}
}

func newTestStreamClient(t *testing.T, srv *fakeStreamServer, opts ...StreamOption) *StreamClient {
	t.Helper()
	defaults := []StreamOption{
		WithStreamURL(srv.url),
		WithStreamReconnectDelay(time.Millisecond, 10*time.Millisecond),
	}
	client, err := NewStreamClient(append(defaults, opts...)...)
	if err != nil {
		t.Fatalf("NewStreamClient returned error: %v", err)
	}
	return client
}

func runStream(t *testing.T, client *StreamClient) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Run returned %v, want context.Canceled", err)
			}
		case <-time.After(testWait):
			t.Error("Run did not return after its context was canceled")
		}
	})
}

type errorCollector struct {
	errs chan error
}

func newErrorCollector() *errorCollector {
	return &errorCollector{errs: make(chan error, 256)}
}

func (c *errorCollector) handle(err error) {
	select {
	case c.errs <- err:
	default:
	}
}

func (c *errorCollector) expect(t *testing.T, matches func(error) bool) error {
	t.Helper()
	deadline := time.After(testWait)
	for {
		select {
		case err := <-c.errs:
			if matches(err) {
				return err
			}
		case <-deadline:
			t.Fatal("timed out waiting for an expected stream error")
			return nil
		}
	}
}

func (c *errorCollector) expectNone(t *testing.T) {
	t.Helper()
	select {
	case err := <-c.errs:
		t.Errorf("unexpected stream error: %v", err)
	default:
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(testWait):
		var zero T
		t.Fatalf("timed out waiting for a %T", zero)
		return zero
	}
}

func expectNothing[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case value := <-ch:
		t.Fatalf("unexpectedly received %+v", value)
	case <-time.After(50 * time.Millisecond):
	}
}

func collect[T any]() (chan T, func(T)) {
	ch := make(chan T, 64)
	return ch, func(value T) { ch <- value }
}

func mustSubscribe(t *testing.T) func(*Subscription, error) *Subscription {
	t.Helper()
	return func(sub *Subscription, err error) *Subscription {
		t.Helper()
		if err != nil {
			t.Fatalf("subscribe returned error: %v", err)
		}
		return sub
	}
}
