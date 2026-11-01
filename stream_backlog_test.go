package coinstore

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type gate struct {
	ctx    context.Context
	opened chan struct{}
	open   func()
}

func newGate(t *testing.T) *gate {
	opened := make(chan struct{})
	return &gate{ctx: t.Context(), opened: opened, open: sync.OnceFunc(func() { close(opened) })}
}

func (g *gate) pass() {
	select {
	case <-g.opened:
	case <-g.ctx.Done():
	}
}

func tradeMessage(tradeID int) string {
	return `{"T":"trade","channel":"4@trade","tradeId":` + strconv.Itoa(tradeID) + `}`
}

func depthMessage(bid string) string {
	return `{"T":"depth","channel":"4@depth@5","level":5,"b":[["` + bid + `","1"]],"a":[]}`
}

func syncReads(t *testing.T, client *StreamClient, conn *fakeStreamConn) {
	t.Helper()
	listed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		_, err := client.ListSubscriptions(ctx)
		listed <- err
	}()
	if command := conn.expectCommand(t); command.Op != opList {
		t.Fatalf("client sent %s, want a LIST command", command.raw)
	}
	conn.send(t, `{"T":"resp","subs":[]}`)
	if err := receive(t, listed); err != nil {
		t.Fatalf("ListSubscriptions returned error: %v", err)
	}
}

func expectEvents(t *testing.T, events <-chan string, want ...string) {
	t.Helper()
	for _, w := range want {
		if got := receive(t, events); got != w {
			t.Fatalf("handled %q, want %q", got, w)
		}
	}
}

func TestStreamKeepsReadingWhileHandlersFallBehind(t *testing.T) {
	srv := newFakeStreamServer(t)
	events := make(chan string, 64)
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(func(err error) { events <- err.Error() }))
	client.backlogLimits = backlogLimits{updates: 3, errors: 4}
	held := newGate(t)
	started := make(chan struct{}, 1)
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, func(trade Trade) {
		if trade.TradeID == 1 {
			started <- struct{}{}
			held.pass()
		}
		events <- "trade " + strconv.FormatInt(trade.TradeID, 10)
	}))
	runStream(t, client)
	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")

	conn.send(t, tradeMessage(1))
	receive(t, started)
	for id := 2; id <= 7; id++ {
		conn.send(t, tradeMessage(id))
	}
	syncReads(t, client, conn)
	held.open()

	expectEvents(t, events, "trade 1", "trade 2", "trade 3", "trade 4",
		"coinstore: stream: handlers fell behind, so the client discarded 3 updates of 4@trade")
	conn.send(t, tradeMessage(8))
	expectEvents(t, events, "trade 8")
}

func TestStreamDeliversLatestStatesWhenHandlersFallBehind(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	events := make(chan string, 64)
	held := newGate(t)
	started := make(chan struct{}, 1)
	mustSubscribe(t)(client.SubscribeOrderBook(OrderBookStream{Symbol: "4", Depth: 5}, func(book OrderBook) {
		events <- "book " + book.Bids[0].Price.String()
		if book.Bids[0].Price.Equal(d("1")) {
			started <- struct{}{}
			held.pass()
		}
	}))
	mustSubscribe(t)(client.SubscribeAllTickers(func(tickers []Ticker) {
		var pairs []string
		for _, ticker := range tickers {
			pairs = append(pairs, ticker.Symbol+"="+ticker.Last.String())
		}
		events <- "tickers " + strings.Join(pairs, " ")
	}))
	runStream(t, client)
	conn := srv.accept(t)
	conn.expectSubscribe(t, "!@ticker")
	conn.expectSubscribe(t, "4@depth@5")

	conn.send(t, depthMessage("1"))
	receive(t, started)
	conn.send(t, depthMessage("2"))
	conn.send(t, `{"T":"ticker","data":[{"symbol":"AUSDT","close":"1"},{"symbol":"BUSDT","close":"1"}]}`)
	conn.send(t, depthMessage("3"))
	conn.send(t, `{"T":"ticker","data":[{"symbol":"BUSDT","close":"2"}]}`)
	syncReads(t, client, conn)
	held.open()

	expectEvents(t, events, "book 1", "book 3", "tickers AUSDT=1 BUSDT=2")
	expectNothing(t, events)
}

func TestStreamKeepsUpdatesOfEachConnection(t *testing.T) {
	srv := newFakeStreamServer(t)
	events := make(chan string, 64)
	client := newTestStreamClient(t, srv,
		WithStreamConnectHandler(func() { events <- "connect" }),
		WithStreamErrorHandler(func(error) { events <- "error" }),
	)
	held := newGate(t)
	started := make(chan struct{}, 1)
	mustSubscribe(t)(client.SubscribeOrderBook(OrderBookStream{Symbol: "4", Depth: 5}, func(book OrderBook) {
		events <- "book " + book.Bids[0].Price.String()
		if book.Bids[0].Price.Equal(d("1")) {
			started <- struct{}{}
			held.pass()
		}
	}))
	runStream(t, client)
	first := srv.accept(t)
	first.expectSubscribe(t, "4@depth@5")

	first.send(t, depthMessage("1"))
	receive(t, started)
	first.send(t, depthMessage("2"))
	syncReads(t, client, first)
	first.conn.Close()
	second := srv.accept(t)
	second.expectSubscribe(t, "4@depth@5")
	second.send(t, depthMessage("3"))
	second.send(t, depthMessage("4"))
	syncReads(t, client, second)
	held.open()

	expectEvents(t, events, "connect", "book 1", "book 2", "error", "connect", "book 4")
	expectNothing(t, events)
}

func TestRunStopsWhileHandlerRetriesRequests(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	client.backlogLimits = backlogLimits{updates: 2, errors: 2}
	held := newGate(t)
	started := make(chan struct{}, 1)
	finished := make(chan error, 1)
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, func(trade Trade) {
		if trade.TradeID != 1 {
			return
		}
		started <- struct{}{}
		held.pass()

		for {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := client.ListSubscriptions(ctx)
			cancel()
			if err == nil || errors.Is(err, ErrStreamStopped) {
				finished <- err
				return
			}
		}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")

	conn.send(t, tradeMessage(1))
	receive(t, started)
	for id := 2; id <= 10; id++ {
		conn.send(t, tradeMessage(id))
	}
	held.open()
	conn.expectCommand(t)
	cancel()

	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	if err := receive(t, finished); !errors.Is(err, ErrStreamStopped) {
		t.Errorf("the handler's request returned %v, want ErrStreamStopped", err)
	}
}

func TestStreamReconnectsWhileHandlersFallBehind(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(func(error) {}))
	client.backlogLimits = backlogLimits{updates: 2, errors: 2}
	held := newGate(t)
	started := make(chan struct{}, 1)
	listed := make(chan error, 1)
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, func(trade Trade) {
		if trade.TradeID != 1 {
			return
		}
		started <- struct{}{}
		held.pass()
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		_, err := client.ListSubscriptions(ctx)
		listed <- err
	}))
	runStream(t, client)
	first := srv.accept(t)
	first.expectSubscribe(t, "4@trade")

	first.send(t, tradeMessage(1))
	receive(t, started)
	for id := 2; id <= 10; id++ {
		first.send(t, tradeMessage(id))
	}
	first.conn.Close()
	second := srv.accept(t)
	second.expectSubscribe(t, "4@trade")
	held.open()

	if command := second.expectCommand(t); command.Op != opList {
		t.Fatalf("client sent %s, want the handler's LIST command", command.raw)
	}
	second.send(t, `{"T":"resp","subs":[]}`)
	if err := receive(t, listed); err != nil {
		t.Errorf("the handler's request returned %v", err)
	}
}

func TestStreamReportsDiscardedErrors(t *testing.T) {
	srv := newFakeStreamServer(t)
	events := make(chan string, 64)
	held := newGate(t)
	started := make(chan struct{}, 1)
	var calls int
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(func(err error) {
		events <- err.Error()
		if calls++; calls == 1 {
			started <- struct{}{}
			held.pass()
		}
	}))
	client.backlogLimits = backlogLimits{updates: 2, errors: 1}
	runStream(t, client)
	conn := srv.accept(t)

	conn.send(t, `not json 1`)
	receive(t, started)
	for i := 2; i <= 4; i++ {
		conn.send(t, `not json `+strconv.Itoa(i))
	}
	syncReads(t, client, conn)
	held.open()

	first := receive(t, events)
	second := receive(t, events)
	if !strings.Contains(first, "decode message") || !strings.Contains(second, "decode message") {
		t.Errorf("handled %q and %q, want two decoding errors", first, second)
	}
	expectEvents(t, events, "coinstore: stream: handlers fell behind, so the client discarded 2 errors")
}

func TestRunPanicsWhenHandlerPanics(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, func(Trade) { panic("boom") }))
	recovered := make(chan any, 1)
	go func() {
		defer func() { recovered <- recover() }()
		_ = client.Run(context.Background())
	}()
	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")

	conn.send(t, tradeMessage(1))

	value := receive(t, recovered)
	p, ok := value.(HandlerPanic)
	if !ok || p.Value != "boom" || !strings.Contains(string(p.Stack), "TestRunPanicsWhenHandlerPanics") {
		t.Fatalf("Run panicked with %v, want a HandlerPanic with the handler's value and stack", value)
	}
	conn.awaitClose(t)
	if _, err := client.ListSubscriptions(context.Background()); !errors.Is(err, ErrStreamStopped) {
		t.Errorf("ListSubscriptions after the panic returned %v, want ErrStreamStopped", err)
	}
	runStream(t, client)
	srv.accept(t)
}

func TestRunExitsWhenHandlerExits(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, func(Trade) { runtime.Goexit() }))
	exited := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		defer close(exited)
		_ = client.Run(context.Background())
		close(returned)
	}()
	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")

	conn.send(t, tradeMessage(1))

	receive(t, exited)
	select {
	case <-returned:
		t.Error("Run returned, want it to call runtime.Goexit")
	default:
	}
	conn.awaitClose(t)
}
