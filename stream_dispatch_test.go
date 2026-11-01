package coinstore

import (
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type backlogCall struct {
	kind     entryKind
	channel  string
	update   any
	err      error
	overflow BacklogOverflowError
}

func drain(b *handlerBacklog) []backlogCall {
	var calls []backlogCall
	for {
		entry, ok := b.pop()
		if !ok {
			return calls
		}
		call := backlogCall{kind: entry.kind, channel: entry.channel, update: entry.update, err: entry.err}
		if entry.overflow != nil {
			call.overflow = *entry.overflow
		}
		calls = append(calls, call)
	}
}

func updateCall(channel string, update any) backlogCall {
	return backlogCall{kind: updateEntry, channel: channel, update: update}
}

func bookWithBid(price string) OrderBook {
	return OrderBook{Bids: []PriceLevel{{Price: d(price), Quantity: d("1")}}}
}

func klineAt(start int64, closePrice string) Kline {
	return Kline{StartTime: time.Unix(start, 0).UTC(), Close: d(closePrice)}
}

func TestBacklogReplacesUpdatesHoldingCompleteStates(t *testing.T) {
	b := newHandlerBacklog(backlogLimits{updates: 100, errors: 10}, true)

	b.pushUpdate("4@depth@5", bookWithBid("1"))
	b.pushUpdate("4@trade", Trade{TradeID: 1})
	b.pushUpdate("4@depth@5", bookWithBid("2"))
	b.pushUpdate("4@ticker", Ticker{Last: d("1")})
	b.pushUpdate("4@trade", Trade{TradeID: 2})
	b.pushUpdate("4@kline@min_1", klineAt(60, "1"))
	b.pushUpdate("4@kline@min_1", klineAt(60, "2"))
	b.pushUpdate("4@kline@min_1", klineAt(120, "3"))
	b.pushUpdate("4@ticker", Ticker{Last: d("2")})
	b.pushUpdate("4@depth@5", bookWithBid("3"))
	b.pushUpdate("5@depth@5", bookWithBid("4"))

	assertEqual(t, drain(&b), []backlogCall{

		updateCall("4@depth@5", bookWithBid("3")),
		updateCall("4@trade", Trade{TradeID: 1}),
		updateCall("4@ticker", Ticker{Last: d("2")}),
		updateCall("4@trade", Trade{TradeID: 2}),

		updateCall("4@kline@min_1", klineAt(60, "2")),
		updateCall("4@kline@min_1", klineAt(120, "3")),
		updateCall("5@depth@5", bookWithBid("4")),
	})
}

func TestBacklogNeverReplacesUpdatesOfOtherTypes(t *testing.T) {
	b := newHandlerBacklog(backlogLimits{updates: 100, errors: 10}, true)

	b.pushUpdate("4@depth@5", bookWithBid("1"))
	b.pushUpdate("4@depth@5", Ticker{Last: d("1")})
	b.pushUpdate("!@ticker", []Ticker{{Symbol: "AUSDT"}})
	b.pushUpdate("!@ticker", Ticker{Symbol: "BUSDT"})

	if calls := drain(&b); len(calls) != 4 {
		t.Errorf("backlog held %d calls, want 4: %+v", len(calls), calls)
	}
}

func TestBacklogMergesBatchesOfTickers(t *testing.T) {
	b := newHandlerBacklog(backlogLimits{updates: 100, errors: 10}, true)

	b.pushUpdate("!@ticker", []Ticker{{Symbol: "AUSDT", Last: d("1")}, {Symbol: "BUSDT", Last: d("1")}})
	b.pushUpdate("!@ticker", []Ticker{{Symbol: "busdt", Last: d("2")}, {SymbolID: 7, Last: d("2")}, {Last: d("2")}})
	b.pushUpdate("!@ticker", []Ticker{{SymbolID: 7, Last: d("3")}, {Last: d("3")}})

	assertEqual(t, drain(&b), []backlogCall{updateCall("!@ticker", []Ticker{
		{Symbol: "AUSDT", Last: d("1")},
		{Symbol: "busdt", Last: d("2")},
		{SymbolID: 7, Last: d("3")},

		{Last: d("2")},
		{Last: d("3")},
	})})
}

func TestBacklogMergesTickersOfAPairByEitherName(t *testing.T) {
	b := newHandlerBacklog(backlogLimits{updates: 100, errors: 10}, true)

	b.pushUpdate("!@ticker", []Ticker{{Symbol: "BTCUSDT", SymbolID: 4, Last: d("1")}})
	b.pushUpdate("!@ticker", []Ticker{{SymbolID: 4, Last: d("2")}})
	b.pushUpdate("!@ticker", []Ticker{{Symbol: "btcusdt", Last: d("3")}})
	b.pushUpdate("!@ticker", []Ticker{{SymbolID: 4, Last: d("4")}})

	assertEqual(t, drain(&b), []backlogCall{updateCall("!@ticker", []Ticker{
		{SymbolID: 4, Last: d("4")},
	})})
}

func TestBacklogKeepsUpdatesOfEarlierConnections(t *testing.T) {
	b := newHandlerBacklog(backlogLimits{updates: 100, errors: 10}, true)
	connected := false

	b.pushUpdate("4@depth@5", bookWithBid("1"))
	b.beginConnection(func() { connected = true })
	b.pushUpdate("4@depth@5", bookWithBid("2"))
	b.pushUpdate("4@depth@5", bookWithBid("3"))
	b.beginConnection(nil)
	b.pushUpdate("4@depth@5", bookWithBid("4"))

	entries := []*backlogEntry{}
	for entry, ok := b.pop(); ok; entry, ok = b.pop() {
		entries = append(entries, entry)
	}
	if len(entries) != 4 || entries[1].kind != connectEntry {
		t.Fatalf("backlog held %+v, want an update, the connect handler, and two updates", entries)
	}
	entries[1].call()
	if !connected {
		t.Error("the queued call is not the connect handler")
	}
	for i, want := range map[int]OrderBook{0: bookWithBid("1"), 2: bookWithBid("3"), 3: bookWithBid("4")} {
		assertEqual(t, entries[i].update, any(want))
	}
}

func TestBacklogReportsDiscardedCallsWhereTheyWouldHaveRun(t *testing.T) {
	b := newHandlerBacklog(backlogLimits{updates: 2, errors: 1}, true)
	errFirst, errSecond := errors.New("first"), errors.New("second")

	b.pushUpdate("4@trade", Trade{TradeID: 1})
	b.pushUpdate("4@trade", Trade{TradeID: 2})
	b.pushUpdate("4@trade", Trade{TradeID: 3})
	b.pushUpdate("5@trade", Trade{TradeID: 4})
	b.pushUpdate("4@trade", Trade{TradeID: 5})
	b.pushError(errFirst)
	b.pushError(errSecond)
	b.pushUpdate("4@depth@5", bookWithBid("1"))
	first, _ := b.pop()
	b.pushUpdate("4@trade", Trade{TradeID: 6})

	assertEqual(t, first.update, any(Trade{TradeID: 1}))
	assertEqual(t, drain(&b), []backlogCall{
		updateCall("4@trade", Trade{TradeID: 2}),
		{kind: overflowEntry, overflow: BacklogOverflowError{DroppedUpdates: map[string]int{"4@trade": 2, "5@trade": 1}}},
		{kind: errorEntry, err: errFirst},
		{kind: overflowEntry, overflow: BacklogOverflowError{DroppedUpdates: map[string]int{"4@depth@5": 1}, DroppedErrors: 1}},
		updateCall("4@trade", Trade{TradeID: 6}),
	})
}

func TestBacklogReplacesUpdatesBeyondTheLimit(t *testing.T) {
	b := newHandlerBacklog(backlogLimits{updates: 1, errors: 1}, true)

	b.pushUpdate("4@depth@5", bookWithBid("1"))
	b.pushUpdate("4@depth@5", bookWithBid("2"))

	assertEqual(t, drain(&b), []backlogCall{updateCall("4@depth@5", bookWithBid("2"))})
}

func TestBacklogWithoutErrorHandlerDiscardsSilently(t *testing.T) {
	b := newHandlerBacklog(backlogLimits{updates: 1, errors: 1}, false)

	b.pushUpdate("4@trade", Trade{TradeID: 1})
	b.pushUpdate("4@trade", Trade{TradeID: 2})

	assertEqual(t, drain(&b), []backlogCall{updateCall("4@trade", Trade{TradeID: 1})})
}

func TestBacklogDiscard(t *testing.T) {
	b := newHandlerBacklog(backlogLimits{updates: 1, errors: 1}, true)
	b.pushUpdate("4@depth@5", bookWithBid("1"))
	b.pushError(errors.New("lost"))

	b.discard()
	b.pushUpdate("4@depth@5", bookWithBid("2"))
	b.pushError(errors.New("kept"))

	calls := drain(&b)
	if len(calls) != 2 || calls[0].kind != updateEntry || calls[1].kind != errorEntry || calls[1].err.Error() != "kept" {
		t.Errorf("backlog held %+v after discard, want only the later update and error", calls)
	}
}

func TestDispatcherRunsNoQueuedCallAfterHalting(t *testing.T) {
	for trial := range 100 {
		var started, startedAfterHalt atomic.Int64
		var halted atomic.Bool
		count := func() {
			started.Add(1)
			if halted.Load() {
				startedAfterHalt.Add(1)
			}
		}
		d := startDispatcher(backlogLimits{updates: 50, errors: 5},
			func(string, any) { count() }, func(error) { count() }, func() {})
		stop := make(chan struct{})
		produced := make(chan struct{})
		go func() {
			defer close(produced)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				switch i % 5 {
				case 0:
					d.reportError(errors.New("lost"))
				case 1:
					d.beginConnection(count)
				default:
					d.dispatchUpdate(strconv.Itoa(i%3)+"@trade", Trade{TradeID: int64(i)})
				}
			}
		}()
		time.Sleep(time.Duration(trial*20) * time.Microsecond)

		d.halt()
		halted.Store(true)
		d.wait()
		close(stop)
		<-produced

		if n := startedAfterHalt.Load(); n > 1 {
			t.Fatalf("trial %d: %d calls started after halt returned", trial, n)
		}
	}
}

func TestBacklogOverflowErrorMessage(t *testing.T) {
	err := &BacklogOverflowError{DroppedUpdates: map[string]int{"5@trade": 1, "4@trade": 120}, DroppedErrors: 2}
	want := "coinstore: stream: handlers fell behind, so the client discarded 120 updates of 4@trade, 1 update of 5@trade, 2 errors"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestHandlerPanicString(t *testing.T) {
	p := HandlerPanic{Value: "boom", Stack: []byte("goroutine 7 [running]:")}
	if got := p.String(); !strings.Contains(got, "handler panicked: boom") || !strings.Contains(got, "goroutine 7") {
		t.Errorf("String() = %q, want the panic value and the stack", got)
	}
}
