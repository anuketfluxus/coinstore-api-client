package coinstore

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestStreamDeliversTrades(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, onTrade))
	runStream(t, client)

	conn := srv.accept(t)
	command := conn.expectSubscribe(t, "4@trade")
	if command.ID == 0 {
		t.Errorf("SUB command %s has no ID", command.raw)
	}

	conn.send(t, `{"S":4,"T":"trade","channel":"4@trade","time":1604040975,"price":"9811.7494086",`+
		`"takerSide":"SELL","tradeId":26461,"volume":"7.505","symbol":"BTCUSDT","instrumentId":4}`)

	assertEqual(t, receive(t, trades), Trade{
		Symbol:    "BTCUSDT",
		SymbolID:  4,
		TradeID:   26461,
		Price:     d("9811.7494086"),
		Quantity:  d("7.505"),
		TakerSide: SideSell,
		Time:      time.Unix(1604040975, 0).UTC(),
	})
}

func TestStreamTradeBackfill(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "28", Backfill: 2}, onTrade))
	runStream(t, client)

	conn := srv.accept(t)
	command := conn.expectCommand(t)
	if command.Op != opSubscribe || string(command.Param) != `{"size":2}` {
		t.Fatalf("client sent %s, want a SUB command with param {\"size\":2}", command.raw)
	}

	conn.send(t, `{"S":15,"T":"trade","data":[{"channel":"28@trade","price":"384.32","tradeId":21704,"seq":10157,`+
		`"ts":1612685697087,"takerSide":"BULL","time":1612685697,"volume":"2","symbol":"BTCJJ","instrumentId":28},`+
		`{"channel":"28@trade","price":"644.96","tradeId":21705,"seq":10158,"ts":1612685698157,"takerSide":"SELL",`+
		`"time":1612685698,"volume":"1.1932988","symbol":"BTCJJ","instrumentId":28}]}`)

	first, second := receive(t, trades), receive(t, trades)
	if first.TradeID != 21704 || second.TradeID != 21705 {
		t.Errorf("received trades %d and %d, want 21704 and 21705", first.TradeID, second.TradeID)
	}
	assertEqual(t, second, Trade{
		Symbol:    "BTCJJ",
		SymbolID:  28,
		TradeID:   21705,
		Sequence:  10158,
		Price:     d("644.96"),
		Quantity:  d("1.1932988"),
		TakerSide: SideSell,
		Time:      time.UnixMilli(1612685698157).UTC(),
	})
}

func TestStreamDeliversKlines(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	klines, onKline := collect[Kline]()
	mustSubscribe(t)(client.SubscribeKlines(KlineStream{Symbol: "88066", Interval: KlineInterval1Minute}, onKline))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "88066@kline@min_1")
	conn.send(t, `{"T":"kline","channel":"88066@kline@min_1","instrumentId":88066,"startTime":1603732500,`+
		`"endTime":1603732559,"symbol":"USDTBTC","interval":"min_1","firstTradeId":100,"lastTradeId":200,`+
		`"open":"0.0010","close":"0.0020","high":"0.0025","low":"0.0015","volume":"1000","amount":"1.0000"}`)

	assertEqual(t, receive(t, klines), Kline{
		Symbol:       "USDTBTC",
		SymbolID:     88066,
		Interval:     KlineInterval1Minute,
		StartTime:    time.Unix(1603732500, 0).UTC(),
		EndTime:      time.Unix(1603732559, 0).UTC(),
		Open:         d("0.0010"),
		High:         d("0.0025"),
		Low:          d("0.0015"),
		Close:        d("0.0020"),
		Volume:       d("1000"),
		QuoteVolume:  d("1.0000"),
		FirstTradeID: 100,
		LastTradeID:  200,
	})
}

func TestStreamDeliversTicker(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	tickers, onTicker := collect[Ticker]()
	mustSubscribe(t)(client.SubscribeTicker(TickerStream{Symbol: "88066"}, onTicker))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "88066@ticker")
	conn.send(t, `{"T":"ticker","channel":"88066@ticker","instrumentId":88066,"symbol":"USDTBTC","open":"0.0010",`+
		`"high":"0.0025","low":"0.0010","close":"0.0025","volume":"10000","amount":"18"}`)

	assertEqual(t, receive(t, tickers), Ticker{
		Symbol:      "USDTBTC",
		SymbolID:    88066,
		Open:        d("0.0010"),
		High:        d("0.0025"),
		Low:         d("0.0010"),
		Last:        d("0.0025"),
		Volume:      d("10000"),
		QuoteVolume: d("18"),
	})
}

func TestStreamDeliversAllTickers(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	batches, onTickers := collect[[]Ticker]()
	mustSubscribe(t)(client.SubscribeAllTickers(onTickers))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "!@ticker")

	conn.send(t, `{"data":[{"channel":"80002@ticker","open":"9996.24","high":"10048.6601","low":"9768.90178",`+
		`"close":"9833.12538","volume":"3362.405","amount":"33212664.57170620732","symbol":"BTCUSD",`+
		`"instrumentId":80002},{"channel":"80001@ticker","open":"10001.57","high":"10053.9441",`+
		`"low":"9816.23078","close":"9832.91038","volume":"3138.93361771","amount":"31040843.0757471861414",`+
		`"symbol":"ETHUSD","instrumentId":80001}]}`)

	batch := receive(t, batches)
	if len(batch) != 2 {
		t.Fatalf("received %d tickers, want 2", len(batch))
	}
	assertEqual(t, batch[1], Ticker{
		Symbol:      "ETHUSD",
		SymbolID:    80001,
		Open:        d("10001.57"),
		High:        d("10053.9441"),
		Low:         d("9816.23078"),
		Last:        d("9832.91038"),
		Volume:      d("3138.93361771"),
		QuoteVolume: d("31040843.0757471861414"),
	})
}

func TestStreamDeliversOrderBook(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	books, onBook := collect[OrderBook]()
	mustSubscribe(t)(client.SubscribeOrderBook(OrderBookStream{Symbol: "4", Depth: 20}, onBook))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@depth@20")
	conn.send(t, `{"T":"depth","channel":"4@depth@20","level":20,"instrumentId":4,"symbol":"BTCUSDT",`+
		`"b":[["0.0024","10"]],"a":[["0.0026","100"]]}`)

	assertEqual(t, receive(t, books), OrderBook{
		Symbol:   "BTCUSDT",
		SymbolID: 4,
		Bids:     []PriceLevel{{Price: d("0.0024"), Quantity: d("10")}},
		Asks:     []PriceLevel{{Price: d("0.0026"), Quantity: d("100")}},
	})
}

func TestStreamRoutesUpdatesByEitherPairName(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	trades, onTrade := collect[Trade]()
	tickers, onTicker := collect[Ticker]()
	books, onBook := collect[OrderBook]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "BTCUSDT"}, onTrade))
	mustSubscribe(t)(client.SubscribeTicker(TickerStream{Symbol: "4"}, onTicker))
	mustSubscribe(t)(client.SubscribeOrderBook(OrderBookStream{Symbol: "4", Depth: 5}, onBook))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@depth@5")
	conn.expectSubscribe(t, "4@ticker")
	conn.expectSubscribe(t, "btcusdt@trade")

	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1,"symbol":"BTCUSDT","instrumentId":4}`)

	conn.send(t, `{"T":"ticker","channel":"btcusdt@ticker","symbol":"BTCUSDT","instrumentId":4,"close":"1"}`)

	conn.send(t, `{"T":"depth","level":5,"symbol":"BTCUSDT","instrumentId":4,"b":[],"a":[]}`)

	if trade := receive(t, trades); trade.TradeID != 1 {
		t.Errorf("received trade %d, want 1", trade.TradeID)
	}
	if ticker := receive(t, tickers); !ticker.Last.Equal(d("1")) {
		t.Errorf("received ticker with last price %s, want 1", ticker.Last)
	}
	if book := receive(t, books); book.SymbolID != 4 {
		t.Errorf("received order book of pair %d, want 4", book.SymbolID)
	}
}

func TestStreamNamesPairIDsWithoutLeadingZeros(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "004"}, onTrade))
	runStream(t, client)
	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1,"symbol":"BTCUSDT","instrumentId":4}`)
	if trade := receive(t, trades); trade.TradeID != 1 {
		t.Errorf("received trade %d, want 1", trade.TradeID)
	}

	padded := requestDailyKlines(client, "04", 20000)
	expectRequestFor(t, conn, "4@kline@day_1")
	plain := requestDailyKlines(client, "4", 50000)
	conn.expectNoCommand(t)
	conn.send(t, klineReply("4@kline@day_1", 19999))
	expectKlinesFrom(t, padded, 19999)
	expectRequestFor(t, conn, "4@kline@day_1")
	conn.send(t, klineReply("4@kline@day_1", 49999))
	expectKlinesFrom(t, plain, 49999)
}

func TestStreamLearnsPairNamesFromChannels(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)

	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1,"symbol":"BTCUSDT"}`)
	conn.send(t, `{"data":[{"channel":"80002@ticker","close":"1","symbol":"BTCUSD"},{"channel":"80001@ticker","close":"1","symbol":"ETHUSD"}]}`)
	syncReads(t, client, conn)

	for _, names := range [][2]string{{"4", "btcusdt"}, {"80002", "btcusd"}, {"80001", "ethusd"}} {
		if !knownSame(&client.pairs, names[0], names[1]) {
			t.Errorf("the client did not learn that %s is %s", names[0], names[1])
		}
	}
}

func TestStreamRoutesUpdatesUnderLearnedNames(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	books, onBook := collect[OrderBook]()
	mustSubscribe(t)(client.SubscribeOrderBook(OrderBookStream{Symbol: "btcusdt", Depth: 20}, onBook))
	runStream(t, client)
	conn := srv.accept(t)
	conn.expectSubscribe(t, "btcusdt@depth@20")
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1,"symbol":"BTCUSDT","instrumentId":4}`)

	for bid, update := range map[string]string{
		"1": `{"T":"depth","channel":"4@depth@20","b":[["1","1"]],"a":[]}`,
		"2": `{"level":5,"instrumentId":4,"b":[["2","1"]],"a":[]}`,
	} {
		conn.send(t, update)
		if book := receive(t, books); len(book.Bids) != 1 || !book.Bids[0].Price.Equal(d(bid)) {
			t.Errorf("received order book %+v, want a bid at %s", book, bid)
		}
	}
	errs.expectNone(t)
}

func TestStreamFansOutAndUnsubscribes(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	firstTrades, onFirst := collect[Trade]()
	secondTrades, onSecond := collect[Trade]()
	tickers, onTicker := collect[Ticker]()
	first := mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, onFirst))
	second := mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, onSecond))
	mustSubscribe(t)(client.SubscribeTicker(TickerStream{Symbol: "4"}, onTicker))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@ticker")
	conn.expectSubscribe(t, "4@trade")
	conn.expectNoCommand(t)
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1}`)
	receive(t, firstTrades)
	receive(t, secondTrades)

	first.Unsubscribe()
	conn.expectNoCommand(t)
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":2}`)
	if trade := receive(t, secondTrades); trade.TradeID != 2 {
		t.Errorf("remaining subscription received trade %d, want 2", trade.TradeID)
	}
	expectNothing(t, firstTrades)

	second.Unsubscribe()
	second.Unsubscribe()
	command := conn.expectCommand(t)
	if command.Op != opUnsubscribe || len(command.Channel) != 1 || command.Channel[0] != "4@trade" {
		t.Fatalf("client sent %s, want an UNSUB command for 4@trade", command.raw)
	}
	conn.expectNoCommand(t)
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":3}`)
	conn.send(t, `{"T":"ticker","channel":"4@ticker","close":"1"}`)
	receive(t, tickers)
	expectNothing(t, secondTrades)
}

func TestStreamSubscribesOnOpenConnection(t *testing.T) {
	srv := newFakeStreamServer(t)
	connected := make(chan struct{}, 4)
	client := newTestStreamClient(t, srv, WithStreamConnectHandler(func() { connected <- struct{}{} }))
	runStream(t, client)

	conn := srv.accept(t)
	receive(t, connected)
	conn.expectNoCommand(t)

	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "btcusdt", Backfill: 10}, onTrade))
	command := conn.expectCommand(t)
	if command.Op != opSubscribe || command.Channel[0] != "btcusdt@trade" || string(command.Param) != `{"size":10}` {
		t.Fatalf("client sent %s, want a SUB command for btcusdt@trade with a backfill of 10", command.raw)
	}
	conn.send(t, `{"T":"trade","channel":"btcusdt@trade","tradeId":7}`)
	if trade := receive(t, trades); trade.TradeID != 7 {
		t.Errorf("received trade %d, want 7", trade.TradeID)
	}
}

func TestStreamUnsubscribeFromHandler(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	var sub *Subscription
	calls := make(chan int64, 8)
	sub = mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, func(trade Trade) {
		calls <- trade.TradeID
		sub.Unsubscribe()
	}))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")
	conn.send(t, `{"T":"trade","data":[{"channel":"4@trade","tradeId":1},{"channel":"4@trade","tradeId":2}]}`)

	if id := receive(t, calls); id != 1 {
		t.Errorf("handler received trade %d, want 1", id)
	}
	if command := conn.expectCommand(t); command.Op != opUnsubscribe {
		t.Fatalf("client sent %s, want an UNSUB command", command.raw)
	}
	expectNothing(t, calls)
}

func TestStreamRestoresSubscriptionsAfterReconnect(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	var connections atomic.Int32
	client := newTestStreamClient(t, srv,
		WithStreamErrorHandler(errs.handle),
		WithStreamConnectHandler(func() { connections.Add(1) }),
	)
	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4", Backfill: 5}, onTrade))
	mustSubscribe(t)(client.SubscribeKlines(KlineStream{Symbol: "4", Interval: KlineInterval1Hour}, func(Kline) {}))
	runStream(t, client)

	first := srv.accept(t)
	first.expectSubscribe(t, "4@kline@hour_1")
	if command := first.expectCommand(t); command.Channel[0] != "4@trade" || string(command.Param) != `{"size":5}` {
		t.Fatalf("client sent %s, want a SUB command for 4@trade with a backfill of 5", command.raw)
	}
	first.conn.Close()

	errs.expect(t, func(err error) bool { return strings.Contains(err.Error(), "connection lost") })
	second := srv.accept(t)
	second.expectSubscribe(t, "4@kline@hour_1")
	if command := second.expectCommand(t); command.Channel[0] != "4@trade" || string(command.Param) != `{"size":5}` {
		t.Fatalf("client sent %s, want a SUB command for 4@trade with a backfill of 5", command.raw)
	}
	second.send(t, `{"T":"trade","channel":"4@trade","tradeId":9}`)
	if trade := receive(t, trades); trade.TradeID != 9 {
		t.Errorf("received trade %d, want 9", trade.TradeID)
	}
	if n := connections.Load(); n != 2 {
		t.Errorf("connect handler ran %d times, want 2", n)
	}
}

func TestStreamRetriesFailedConnections(t *testing.T) {
	srv := newFakeStreamServer(t)
	srv.rejectNext(2)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	runStream(t, client)

	for range 2 {
		errs.expect(t, func(err error) bool {
			return strings.Contains(err.Error(), "connect") && strings.Contains(err.Error(), "HTTP 403")
		})
	}
	srv.accept(t)
}

func TestStreamReconnectsIdleConnections(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv,
		WithStreamErrorHandler(errs.handle),
		WithStreamIdleTimeout(100*time.Millisecond),
		WithStreamKeepaliveInterval(time.Hour),
	)
	runStream(t, client)

	srv.accept(t)
	err := errs.expect(t, func(err error) bool { return strings.Contains(err.Error(), "connection lost") })
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("idle connection failed with %v, want a timeout", err)
	}
	srv.accept(t)
}

func TestStreamPingsKeepConnectionAlive(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv,
		WithStreamErrorHandler(errs.handle),
		WithStreamIdleTimeout(300*time.Millisecond),
		WithStreamKeepaliveInterval(time.Hour),
	)
	runStream(t, client)

	conn := srv.accept(t)
	for i := range 6 {
		appData := "ping-" + string(rune('a'+i))
		if err := conn.conn.WriteControl(websocket.PingMessage, []byte(appData), time.Now().Add(testWait)); err != nil {
			t.Fatalf("send ping: %v", err)
		}
		if pong := receive(t, conn.pongs); pong != appData {
			t.Errorf("client answered ping %q with pong %q", appData, pong)
		}
		time.Sleep(100 * time.Millisecond)
	}
	errs.expectNone(t)
}

func TestStreamSendsKeepalives(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv, WithStreamKeepaliveInterval(20*time.Millisecond))
	runStream(t, client)

	conn := srv.accept(t)
	for range 3 {
		if pong := receive(t, conn.pongs); pong != "" {
			t.Errorf("keepalive pong carries %q, want no data", pong)
		}
	}
}

func TestStreamClosesConnectionOnShutdown(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	conn := srv.accept(t)

	cancel()

	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	if err := conn.awaitClose(t); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Errorf("connection closed with %v, want a normal closure", err)
	}
}

func TestStreamRunRejectsConcurrentCalls(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	srv.accept(t)

	if err := client.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "already executing") {
		t.Errorf("second Run returned %v, want an error", err)
	}
}

func TestStreamReportsServerErrors(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	runStream(t, client)

	conn := srv.accept(t)
	conn.send(t, `{"T":"resp","C":200,"M":"established","sid":"3ae985ff-cadd-9c96-7a42-4ed29e77f83b"}`)
	conn.send(t, `{"T":"echo","C":200,"M":"command.received","echo":{"op":"SUB","channel":["x@trade"],"id":1}}`)
	conn.send(t, `{"T":"resp","C":4003,"M":"sub.channel.invalid"}`)

	err := errs.expect(t, func(error) bool { return true })
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("reported %v, want a *StreamError", err)
	}
	assertEqual(t, *streamErr, StreamError{Code: 4003, Message: "sub.channel.invalid"})
	errs.expectNone(t)
}

func TestStreamReportsSequenceGaps(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	runStream(t, client)

	conn := srv.accept(t)
	conn.send(t, `{"S":1,"T":"resp","C":200,"M":"established"}`)
	conn.send(t, `{"S":2,"T":"resp","C":200,"M":"sub.channel.success"}`)
	conn.send(t, `{"S":5,"T":"resp","C":200,"M":"sub.channel.success"}`)

	err := errs.expect(t, func(error) bool { return true })
	var gap *SequenceGapError
	if !errors.As(err, &gap) {
		t.Fatalf("reported %v, want a *SequenceGapError", err)
	}
	assertEqual(t, *gap, SequenceGapError{Expected: 3, Received: 5})
}

func TestStreamSequenceRestartsWithEachConnection(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	runStream(t, client)

	first := srv.accept(t)
	first.send(t, `{"S":1,"T":"resp","C":200,"M":"established"}`)
	first.send(t, `{"S":2,"T":"resp","C":200,"M":"sub.channel.success"}`)
	first.conn.Close()
	errs.expect(t, func(err error) bool { return strings.Contains(err.Error(), "connection lost") })

	second := srv.accept(t)
	second.send(t, `{"S":1,"T":"resp","C":200,"M":"established"}`)
	second.send(t, `{"S":2,"T":"resp","C":200,"M":"sub.channel.success"}`)

	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, onTrade))
	second.expectSubscribe(t, "4@trade")
	second.send(t, `{"S":3,"T":"trade","channel":"4@trade","tradeId":1}`)
	receive(t, trades)
	errs.expectNone(t)
}

func TestStreamReportsUndecodableMessages(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, onTrade))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")
	conn.send(t, `not json`)
	errs.expect(t, func(err error) bool { return strings.Contains(err.Error(), "decode message") })
	conn.send(t, `{"T":"trade","data":[{"channel":"4@trade","tradeId":1},{"channel":"4@trade","price":"abc"}]}`)
	errs.expect(t, func(err error) bool { return strings.Contains(err.Error(), "decode trade update") })
	conn.send(t, `{"T":"trade","data":"oops"}`)
	errs.expect(t, func(err error) bool { return strings.Contains(err.Error(), "decode trade update") })

	expectNothing(t, trades)
}

func TestStreamIgnoresUnknownMessages(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, onTrade))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")
	conn.send(t, `{"T":"account","channel":"account","balance":"1"}`)
	conn.send(t, `{"op":"pong","epochMillis":1604040975}`)
	conn.send(t, `{"T":"trade","channel":"5@trade","tradeId":1,"symbol":"ETHUSDT","instrumentId":5}`)
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":2}`)

	if trade := receive(t, trades); trade.TradeID != 2 {
		t.Errorf("received trade %d, want 2", trade.TradeID)
	}
	errs.expectNone(t)
}

func TestStreamRecognizesDocumentedUpdatesWithoutTypeOrChannel(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	tickers, onTicker := collect[Ticker]()
	books, onBook := collect[OrderBook]()
	trades, onTrade := collect[Trade]()
	klines, onKline := collect[Kline]()
	mustSubscribe(t)(client.SubscribeTicker(TickerStream{Symbol: "88066"}, onTicker))
	mustSubscribe(t)(client.SubscribeOrderBook(OrderBookStream{Symbol: "88066", Depth: 5}, onBook))
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "88066"}, onTrade))
	mustSubscribe(t)(client.SubscribeKlines(KlineStream{Symbol: "88066", Interval: KlineInterval1Minute}, onKline))
	runStream(t, client)
	conn := srv.accept(t)
	for range 4 {
		conn.expectCommand(t)
	}

	conn.send(t, `{"instrumentId":88066,"symbol":"USDTBTC","open":"0.0010","high":"0.0025","low":"0.0010",`+
		`"close":"0.0025","volume":"10000","amount":"18"}`)
	conn.send(t, `{"level":5,"instrumentId":88066,"symbol":"BTCUSDT","b":[["0.0024","10"]],"a":[["0.0026","100"]]}`)
	conn.send(t, `{"instrumentId":88066,"symbol":"USDTBTC","tradeId":12345,"takerSide":"BUY","price":"0.001",`+
		`"volume":"100","time":123456785,"ts":1612685313400,"seq":9935}`)
	conn.send(t, `{"instrumentId":88066,"startTime":1603732500,"endTime":1603732559,"symbol":"USDTBTC",`+
		`"interval":"1m","firstTradeId":100,"lastTradeId":200,"open":"0.0010","close":"0.0020","high":"0.0025",`+
		`"low":"0.0015","volume":"1000","amount":"1.0000"}`)

	if ticker := receive(t, tickers); !ticker.Last.Equal(d("0.0025")) {
		t.Errorf("received ticker %+v", ticker)
	}
	if book := receive(t, books); len(book.Bids) != 1 || len(book.Asks) != 1 {
		t.Errorf("received order book %+v", book)
	}
	if trade := receive(t, trades); trade.TradeID != 12345 {
		t.Errorf("received trade %+v", trade)
	}
	if kline := receive(t, klines); kline.Interval != KlineInterval1Minute || kline.StartTime.Unix() != 1603732500 {
		t.Errorf("received kline %+v", kline)
	}
	errs.expectNone(t)
}

func TestStreamRoutesUpdatesWithoutChannelByTheirPair(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	books, onBook := collect[OrderBook]()
	klines, onKline := collect[Kline]()
	mustSubscribe(t)(client.SubscribeOrderBook(OrderBookStream{Symbol: "4", Depth: 20}, onBook))
	mustSubscribe(t)(client.SubscribeKlines(KlineStream{Symbol: "4", Interval: KlineInterval1Hour}, onKline))
	runStream(t, client)
	conn := srv.accept(t)
	for range 2 {
		conn.expectCommand(t)
	}

	conn.send(t, `{"level":5,"instrumentId":4,"symbol":"BTCUSDT","b":[["0.0024","10"]],"a":[]}`)
	conn.send(t, `{"instrumentId":4,"symbol":"BTCUSDT","startTime":3600,"interval":"60m","close":"1"}`)

	if book := receive(t, books); len(book.Bids) != 1 {
		t.Errorf("received order book %+v", book)
	}
	if kline := receive(t, klines); kline.StartTime.Unix() != 3600 {
		t.Errorf("received kline %+v", kline)
	}
	errs.expectNone(t)
}

func TestStreamReportsUpdatesWithoutChannelThatItCannotRoute(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	books, onBook := collect[OrderBook]()
	mustSubscribe(t)(client.SubscribeOrderBook(OrderBookStream{Symbol: "4", Depth: 10}, onBook))
	mustSubscribe(t)(client.SubscribeOrderBook(OrderBookStream{Symbol: "4", Depth: 20}, onBook))
	runStream(t, client)
	conn := srv.accept(t)
	for range 2 {
		conn.expectCommand(t)
	}

	conn.send(t, `{"level":5,"instrumentId":4,"symbol":"BTCUSDT","b":[],"a":[]}`)
	err := errs.expect(t, func(error) bool { return true })
	want := "coinstore: stream: ignored a depth update without a channel, whose fields point to 4@depth@5, " +
		"which is not subscribed, while the pair has several depth subscriptions: 4@depth@10, 4@depth@20"
	if err.Error() != want {
		t.Errorf("reported %q, want %q", err, want)
	}
	conn.send(t, `{"tradeId":1,"price":"1"}`)
	err = errs.expect(t, func(error) bool { return true })
	if want := "coinstore: stream: ignored a trade update that names neither its channel nor its pair"; err.Error() != want {
		t.Errorf("reported %q, want %q", err, want)
	}
	expectNothing(t, books)
}

func TestStreamReportsUnrecognizedMessages(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	runStream(t, client)
	conn := srv.accept(t)

	conn.send(t, `{"op":"pong","epochMillis":1604040975}`)
	conn.send(t, `{"S":7,"foo":"bar"}`)

	err := errs.expect(t, func(error) bool { return true })
	if want := `coinstore: stream: ignored a message of unknown kind: {"S":7,"foo":"bar"}`; err.Error() != want {
		t.Errorf("reported %q, want %q", err, want)
	}
	errs.expectNone(t)
}

func TestStreamHandlesMessagesWithOtherOperations(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, onTrade))
	runStream(t, client)
	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")

	conn.send(t, `{"op":"trade","T":"trade","channel":"4@trade","tradeId":5}`)
	if trade := receive(t, trades); trade.TradeID != 5 {
		t.Errorf("received trade %d, want 5", trade.TradeID)
	}
	results := make(chan []Kline, 1)
	go func() {
		klines, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
		if err != nil {
			t.Errorf("RequestKlines returned error: %v", err)
		}
		results <- klines
	}()
	command := conn.expectCommand(t)
	conn.send(t, `{"op":"REQ","id":`+strconv.FormatUint(command.ID, 10)+`,"channel":"4@kline@day_1","item":[{"interval":"day_1","startTime":60}]}`)
	if klines := receive(t, results); len(klines) != 1 {
		t.Errorf("RequestKlines returned %+v, want the reply", klines)
	}
}

func TestKindByFields(t *testing.T) {
	tests := map[string]string{
		`{"tradeId":1,"price":"1"}`:              messageTrade,
		`{"startTime":60,"close":"1"}`:           messageKline,
		`{"interval":"1m","close":"1"}`:          messageKline,
		`{"level":5,"b":[]}`:                     messageDepth,
		`{"a":[["1","2"]]}`:                      messageDepth,
		`{"open":"1","close":"1","volume":"10"}`: messageTicker,
		`{"symbol":"BTCUSDT"}`:                   "",
		`[1,2]`:                                  "",
	}
	for item, want := range tests {
		if got := kindByFields(json.RawMessage(item)); got != want {
			t.Errorf("kindByFields(%s) = %q, want %q", item, got, want)
		}
	}
}

func TestRequestKlines(t *testing.T) {
	tests := []struct {
		name      string
		request   KlineHistoryRequest
		wantParam string
	}{
		{
			name:      "before an end time",
			request:   KlineHistoryRequest{Symbol: "88066", Interval: KlineInterval1Minute, End: time.Unix(1603766400, 0), Limit: 200},
			wantParam: `{"channel":"88066@kline@min_1","endTime":1603766400,"limit":200}`,
		},
		{
			name:      "most recent",
			request:   KlineHistoryRequest{Symbol: "88066", Interval: KlineInterval1Minute},
			wantParam: `{"channel":"88066@kline@min_1","endTime":null}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newFakeStreamServer(t)
			client := newTestStreamClient(t, srv)
			runStream(t, client)
			conn := srv.accept(t)

			type result struct {
				klines []Kline
				err    error
			}
			results := make(chan result, 1)
			go func() {
				klines, err := client.RequestKlines(context.Background(), tt.request)
				results <- result{klines, err}
			}()

			command := conn.expectCommand(t)
			if command.Op != opRequest || string(command.Param) != tt.wantParam || command.ID == 0 {
				t.Fatalf("client sent %s, want a REQ command with param %s", command.raw, tt.wantParam)
			}

			conn.send(t, `{"channel":"88066@kline@min_1","symbol":"USDTBTC","instrumentId":88066,"item":[`+
				`{"interval":"min_1","startTime":1603766280,"endTime":1603766339,"lastTradeId":4739656,`+
				`"firstTradeId":4739656,"close":"4.91","open":"4.91","high":"4.91","low":"4.91","volume":"8.488",`+
				`"amount":"41.67608"}]}`)

			got := receive(t, results)
			if got.err != nil {
				t.Fatalf("RequestKlines returned error: %v", got.err)
			}
			assertEqual(t, got.klines, []Kline{{
				Symbol:       "USDTBTC",
				SymbolID:     88066,
				Interval:     KlineInterval1Minute,
				StartTime:    time.Unix(1603766280, 0).UTC(),
				EndTime:      time.Unix(1603766339, 0).UTC(),
				Open:         d("4.91"),
				High:         d("4.91"),
				Low:          d("4.91"),
				Close:        d("4.91"),
				Volume:       d("8.488"),
				QuoteVolume:  d("41.67608"),
				FirstTradeID: 4739656,
				LastTradeID:  4739656,
			}})
		})
	}
}

func TestRequestKlinesMatchesResponsesToRequests(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)

	minute := make(chan []Kline, 1)
	hour := make(chan []Kline, 1)
	request := func(interval KlineInterval, results chan<- []Kline) {
		klines, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "btcusdt", Interval: interval})
		if err != nil {
			t.Errorf("RequestKlines(%s) returned error: %v", interval, err)
		}
		results <- klines
	}
	go request(KlineInterval1Minute, minute)
	conn.expectCommand(t)
	go request(KlineInterval1Hour, hour)
	hourCommand := conn.expectCommand(t)

	conn.send(t, `{"id":`+strconv.FormatUint(hourCommand.ID, 10)+`,"channel":"btcusdt@kline@hour_1","item":[{"interval":"hour_1"}]}`)
	conn.send(t, `{"channel":"4@kline@min_1","symbol":"BTCUSDT","instrumentId":4,"item":[{"interval":"min_1"}]}`)

	if klines := receive(t, hour); len(klines) != 1 || klines[0].Interval != KlineInterval1Hour {
		t.Errorf("hour request received %+v", klines)
	}
	if klines := receive(t, minute); len(klines) != 1 || klines[0].Interval != KlineInterval1Minute {
		t.Errorf("minute request received %+v", klines)
	}
}

func TestRequestKlinesFailsWhenDisconnected(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)
	errs := make(chan error, 1)
	go func() {
		_, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
		errs <- err
	}()
	conn.expectCommand(t)

	conn.conn.Close()

	if err := receive(t, errs); !errors.Is(err, ErrStreamDisconnected) {
		t.Errorf("RequestKlines returned %v, want ErrStreamDisconnected", err)
	}
}

func TestRequestKlinesHonorsContext(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)

	t.Run("while disconnected", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := client.RequestKlines(ctx, KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("RequestKlines returned %v, want context.DeadlineExceeded", err)
		}
	})

	t.Run("while awaiting the response", func(t *testing.T) {
		runStream(t, client)
		conn := srv.accept(t)
		ctx, cancel := context.WithCancel(context.Background())
		errs := make(chan error, 1)
		go func() {
			_, err := client.RequestKlines(ctx, KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
			errs <- err
		}()
		conn.expectCommand(t)

		cancel()

		if err := receive(t, errs); !errors.Is(err, context.Canceled) {
			t.Errorf("RequestKlines returned %v, want context.Canceled", err)
		}

		conn.send(t, `{"channel":"4@kline@day_1","item":[]}`)
	})
}

func TestListSubscriptions(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)
	type result struct {
		channels []ChannelInfo
		err      error
	}
	results := make(chan result, 1)
	go func() {
		channels, err := client.ListSubscriptions(context.Background())
		results <- result{channels, err}
	}()

	command := conn.expectCommand(t)
	if command.Op != opList || command.Channel == nil || len(command.Channel) != 0 || command.ID == 0 {
		t.Fatalf("client sent %s, want a LIST command with an empty channel list", command.raw)
	}

	conn.send(t, `{"S":12,"T":"resp","subs":[{"name":"80004@trade","type":{"name":"trade","auth":"PUB"},`+
		`"instrumentId":"80004","lifeStartTime":1604043594,"msgCount":212}],"sid":"b1beeb73-7c84-862f-3d03-619de1e1a5dc"}`)

	got := receive(t, results)
	if got.err != nil {
		t.Fatalf("ListSubscriptions returned error: %v", got.err)
	}
	assertEqual(t, got.channels, []ChannelInfo{{
		Channel:      "80004@trade",
		Type:         "trade",
		SubscribedAt: time.Unix(1604043594, 0).UTC(),
		MessageCount: 212,
	}})
}

func TestListSubscriptionsRejectsMalformedResponse(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)
	errs := make(chan error, 1)
	go func() {
		_, err := client.ListSubscriptions(context.Background())
		errs <- err
	}()
	conn.expectCommand(t)

	conn.send(t, `{"T":"resp","subs":"not a list"}`)

	if err := receive(t, errs); err == nil || !strings.Contains(err.Error(), "decode subscription list") {
		t.Errorf("ListSubscriptions returned %v, want a decoding error", err)
	}
}

func TestStreamAcceptsSingleUpdateInData(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	books, onBook := collect[OrderBook]()
	mustSubscribe(t)(client.SubscribeOrderBook(OrderBookStream{Symbol: "4", Depth: 5}, onBook))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@depth@5")
	conn.send(t, `{"T":"depth","channel":"4@depth@5","data":{"level":5,"symbol":"BTCUSDT","instrumentId":4,"b":[["1","2"]],"a":[]}}`)

	if book := receive(t, books); len(book.Bids) != 1 || book.Symbol != "BTCUSDT" {
		t.Errorf("received order book %+v", book)
	}
}

func TestStreamReportsMalformedTickerBatches(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	batches, onTickers := collect[[]Ticker]()
	mustSubscribe(t)(client.SubscribeAllTickers(onTickers))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "!@ticker")
	conn.send(t, `{"T":"ticker","data":[{"channel":"1@ticker","close":"1"},{"channel":"2@ticker","close":[]}]}`)

	errs.expect(t, func(err error) bool { return strings.Contains(err.Error(), "decode ticker update") })
	expectNothing(t, batches)
}

func TestStreamRestoresSubscriptionAddedWhileConnectionFails(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	first := srv.accept(t)

	first.conn.Close()
	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, onTrade))

	second := srv.accept(t)
	second.expectSubscribe(t, "4@trade")
	second.send(t, `{"T":"trade","channel":"4@trade","tradeId":1}`)
	receive(t, trades)
}

func TestStreamRunStopsWhileWaitingToReconnect(t *testing.T) {
	errs := newErrorCollector()
	client, err := NewStreamClient(
		WithStreamURL("ws://127.0.0.1:1/s/ws"),
		WithStreamReconnectDelay(time.Hour, time.Hour),
		WithStreamErrorHandler(errs.handle),
	)
	if err != nil {
		t.Fatalf("NewStreamClient returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()

	errs.expect(t, func(err error) bool { return strings.Contains(err.Error(), "connect") })
	cancel()

	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
}

func TestStreamSubscriptionValidation(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	calls := map[string]func() error{
		"trades without symbol": func() error {
			_, err := client.SubscribeTrades(TradeStream{Symbol: " "}, func(Trade) {})
			return err
		},
		"trades with reserved characters": func() error {
			_, err := client.SubscribeTrades(TradeStream{Symbol: "btc@usdt"}, func(Trade) {})
			return err
		},
		"ticker for the whole-market name": func() error {
			_, err := client.SubscribeTicker(TickerStream{Symbol: "!"}, func(Ticker) {})
			return err
		},
		"trades with negative backfill": func() error {
			_, err := client.SubscribeTrades(TradeStream{Symbol: "4", Backfill: -1}, func(Trade) {})
			return err
		},
		"trades without handler": func() error {
			_, err := client.SubscribeTrades(TradeStream{Symbol: "4"}, nil)
			return err
		},
		"klines with unknown interval": func() error {
			_, err := client.SubscribeKlines(KlineStream{Symbol: "4", Interval: "1m"}, func(Kline) {})
			return err
		},
		"order book without symbol": func() error {
			_, err := client.SubscribeOrderBook(OrderBookStream{Depth: 5}, func(OrderBook) {})
			return err
		},
		"kline history without symbol": func() error {
			_, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Interval: KlineInterval1Day})
			return err
		},
		"order book with unsupported depth": func() error {
			_, err := client.SubscribeOrderBook(OrderBookStream{Symbol: "4", Depth: 15}, func(OrderBook) {})
			return err
		},
		"all tickers without handler": func() error {
			_, err := client.SubscribeAllTickers(nil)
			return err
		},
		"kline history beyond the limit": func() error {
			_, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day, Limit: 201})
			return err
		},
		"kline history with negative limit": func() error {
			_, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day, Limit: -1})
			return err
		},
		"kline history without interval": func() error {
			_, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "4"})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestStreamAcceptsMonthlyKlines(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	sub := mustSubscribe(t)(client.SubscribeKlines(KlineStream{Symbol: "BTCUSDT", Interval: KlineInterval1Month}, func(Kline) {}))
	if sub.Channel() != "btcusdt@kline@mon_1" {
		t.Errorf("channel = %s, want btcusdt@kline@mon_1", sub.Channel())
	}
}

func TestNewStreamClientValidation(t *testing.T) {
	tests := map[string]StreamOption{
		"HTTP URL":                    WithStreamURL("https://ws.coinstore.com/s/ws"),
		"relative URL":                WithStreamURL("/s/ws"),
		"unparsable URL":              WithStreamURL("wss://ws.coinstore.com/%zz"),
		"zero idle timeout":           WithStreamIdleTimeout(0),
		"negative keepalive interval": WithStreamKeepaliveInterval(-time.Second),
		"zero reconnect delay":        WithStreamReconnectDelay(0, time.Second),
		"minimum above maximum delay": WithStreamReconnectDelay(time.Minute, time.Second),
	}
	for name, option := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewStreamClient(option); err == nil {
				t.Error("NewStreamClient accepted an invalid option")
			}
		})
	}
}

func TestNewStreamClientDefaults(t *testing.T) {
	client, err := NewStreamClient()
	if err != nil {
		t.Fatalf("NewStreamClient returned error: %v", err)
	}
	if client.url != DefaultStreamURL {
		t.Errorf("URL = %s, want %s", client.url, DefaultStreamURL)
	}
	if client.idleTimeout != 6*time.Minute+30*time.Second {
		t.Errorf("idle timeout = %s, want 6m30s", client.idleTimeout)
	}
	if client.keepaliveInterval != time.Minute {
		t.Errorf("keepalive interval = %s, want 1m", client.keepaliveInterval)
	}
	if client.minReconnectDelay != time.Second || client.maxReconnectDelay != 30*time.Second {
		t.Errorf("reconnect delays = %s to %s, want 1s to 30s", client.minReconnectDelay, client.maxReconnectDelay)
	}
}

func TestStreamErrorMessages(t *testing.T) {
	streamErr := &StreamError{Code: 4003, Message: "sub.channel.invalid"}
	if got, want := streamErr.Error(), "coinstore: stream: server error 4003: sub.channel.invalid"; got != want {
		t.Errorf("StreamError.Error() = %q, want %q", got, want)
	}
	gap := &SequenceGapError{Expected: 3, Received: 5}
	if got, want := gap.Error(), "coinstore: stream: message sequence gap: expected 3, received 5"; got != want {
		t.Errorf("SequenceGapError.Error() = %q, want %q", got, want)
	}
}

func TestRouteChannelNames(t *testing.T) {
	tests := []struct {
		name  string
		route route
		want  []string
	}{
		{"channel named by ID", newRoute("4@trade", messageTrade, "BTCUSDT", 4), []string{"4@trade", "btcusdt@trade"}},
		{"channel named by symbol", newRoute("btcusdt@ticker", messageTicker, "BTCUSDT", 4), []string{"btcusdt@ticker", "4@ticker"}},
		{"channel topic overrides the default", newRoute("4@depth@20", "depth@5", "", 4), []string{"4@depth@20"}},
		{"no channel", newRoute("", "kline@min_1", "BTCUSDT", 4), []string{"4@kline@min_1", "btcusdt@kline@min_1"}},
		{"whole market", route{channel: allTickersChannel}, []string{"!@ticker"}},
		{"nothing known", newRoute("", "trade", "", 0), []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertEqual(t, tt.route.channelNames(), tt.want)
		})
	}
}

func klineReply(channel string, startTimes ...int64) string {
	items := make([]string, len(startTimes))
	for i, start := range startTimes {
		items[i] = `{"interval":"day_1","startTime":` + strconv.FormatInt(start, 10) + `}`
	}
	return `{"channel":"` + channel + `","item":[` + strings.Join(items, ",") + `]}`
}

func TestHandlersCanMakeRequests(t *testing.T) {
	srv := newFakeStreamServer(t)
	results := make(chan error, 4)
	var client *StreamClient
	request := func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		_, err := client.RequestKlines(ctx, KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
		results <- err
	}
	client = newTestStreamClient(t, srv, WithStreamConnectHandler(request))
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, func(Trade) { request() }))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")

	if command := conn.expectCommand(t); command.Op != opRequest {
		t.Fatalf("client sent %s, want the connect handler's REQ command", command.raw)
	}
	conn.send(t, klineReply("4@kline@day_1", 100))
	if err := receive(t, results); err != nil {
		t.Fatalf("request from the connect handler failed: %v", err)
	}

	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1}`)
	if command := conn.expectCommand(t); command.Op != opRequest {
		t.Fatalf("client sent %s, want the trade handler's REQ command", command.raw)
	}
	conn.send(t, klineReply("4@kline@day_1", 100))
	if err := receive(t, results); err != nil {
		t.Errorf("request from the trade handler failed: %v", err)
	}
}

func TestStreamKeepsReadingWhileHandlerBlocks(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv, WithStreamKeepaliveInterval(time.Hour))
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, func(Trade) {
		started <- struct{}{}
		<-release
	}))
	runStream(t, client)
	t.Cleanup(func() { close(release) })

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1}`)
	receive(t, started)

	if err := conn.conn.WriteControl(websocket.PingMessage, []byte("still there?"), time.Now().Add(testWait)); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	if pong := receive(t, conn.pongs); pong != "still there?" {
		t.Errorf("client answered with pong %q", pong)
	}
}

func TestRunWaitsForHandlerAndDiscardsQueuedCalls(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	started := make(chan int64, 4)
	release := make(chan struct{})
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, func(trade Trade) {
		started <- trade.TradeID
		<-release
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1}`)
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":2}`)
	receive(t, started)
	cancel()
	conn.awaitClose(t)

	expectNothing(t, done)
	close(release)
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	expectNothing(t, started)
}

func TestRequestsFailWhenRunStops(t *testing.T) {
	t.Run("waiting for a connection", func(t *testing.T) {
		errs := make(chan error, 1)
		var client *StreamClient
		client, err := NewStreamClient(
			WithStreamURL("ws://127.0.0.1:1/s/ws"),
			WithStreamReconnectDelay(time.Hour, time.Hour),
			WithStreamErrorHandler(func(error) {
				_, err := client.ListSubscriptions(context.Background())
				errs <- err
			}),
		)
		if err != nil {
			t.Fatalf("NewStreamClient returned error: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- client.Run(ctx) }()
		time.Sleep(50 * time.Millisecond)

		cancel()

		if err := receive(t, done); !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
		if err := receive(t, errs); !errors.Is(err, ErrStreamStopped) {
			t.Errorf("ListSubscriptions returned %v, want ErrStreamStopped", err)
		}
	})

	t.Run("awaiting a reply", func(t *testing.T) {
		srv := newFakeStreamServer(t)
		errs := make(chan error, 1)
		var client *StreamClient
		client = newTestStreamClient(t, srv, WithStreamConnectHandler(func() {
			_, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
			errs <- err
		}))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- client.Run(ctx) }()
		conn := srv.accept(t)
		conn.expectCommand(t)

		cancel()

		if err := receive(t, done); !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
		if err := receive(t, errs); !errors.Is(err, ErrStreamStopped) {
			t.Errorf("RequestKlines returned %v, want ErrStreamStopped", err)
		}
	})

	t.Run("waiting to be sent", func(t *testing.T) {
		srv := newFakeStreamServer(t)
		client := newTestStreamClient(t, srv)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- client.Run(ctx) }()
		conn := srv.accept(t)
		outstanding := requestDailyKlines(client, "4", 10000)
		conn.expectCommand(t)
		waiting := requestDailyKlines(client, "4", 20000)
		conn.expectNoCommand(t)

		cancel()

		receive(t, done)
		for _, results := range []<-chan klineResult{outstanding, waiting} {
			if got := receive(t, results); !errors.Is(got.err, ErrStreamStopped) {
				t.Errorf("RequestKlines returned %v, want ErrStreamStopped", got.err)
			}
		}
	})
}

func TestRetriedRequestWaitsForNextConnection(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	first := srv.accept(t)
	errs := make(chan error, 1)
	go func() {
		_, err := client.ListSubscriptions(context.Background())
		errs <- err
	}()
	first.expectCommand(t)
	first.conn.Close()
	if err := receive(t, errs); !errors.Is(err, ErrStreamDisconnected) {
		t.Fatalf("ListSubscriptions returned %v, want ErrStreamDisconnected", err)
	}

	go func() {
		_, err := client.ListSubscriptions(context.Background())
		errs <- err
	}()

	second := srv.accept(t)
	if command := second.expectCommand(t); command.Op != opList {
		t.Fatalf("client sent %s, want the retried LIST command", command.raw)
	}
	second.send(t, `{"T":"resp","subs":[]}`)
	if err := receive(t, errs); err != nil {
		t.Errorf("retried ListSubscriptions returned %v", err)
	}
}

func TestRequestKlinesSendsOneRequestPerChannelAtATime(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)
	results := make(chan []Kline, 2)
	request := func(end int64) {
		klines, err := client.RequestKlines(context.Background(), KlineHistoryRequest{
			Symbol: "4", Interval: KlineInterval1Day, End: time.Unix(end, 0),
		})
		if err != nil {
			t.Errorf("RequestKlines returned error: %v", err)
		}
		results <- klines
	}

	go request(2000)
	conn.expectCommand(t)
	go request(1000)
	conn.expectNoCommand(t)

	conn.send(t, klineReply("4@kline@day_1", 1900))
	if klines := receive(t, results); klines[0].StartTime.Unix() != 1900 {
		t.Errorf("first request received klines from %d", klines[0].StartTime.Unix())
	}
	if command := conn.expectCommand(t); !strings.Contains(string(command.Param), `"endTime":1000`) {
		t.Fatalf("client sent %s, want the second request", command.raw)
	}
	conn.send(t, klineReply("4@kline@day_1", 900))
	if klines := receive(t, results); klines[0].StartTime.Unix() != 900 {
		t.Errorf("second request received klines from %d", klines[0].StartTime.Unix())
	}
}

func TestAbandonedRequestKeepsItsLateReply(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)

	ctx, cancel := context.WithCancel(context.Background())
	abandoned := make(chan error, 1)
	go func() {
		_, err := client.RequestKlines(ctx, KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day, End: time.Unix(2000, 0)})
		abandoned <- err
	}()
	conn.expectCommand(t)
	cancel()
	if err := receive(t, abandoned); !errors.Is(err, context.Canceled) {
		t.Fatalf("abandoned RequestKlines returned %v, want context.Canceled", err)
	}

	results := make(chan []Kline, 1)
	go func() {
		klines, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
		if err != nil {
			t.Errorf("RequestKlines returned error: %v", err)
		}
		results <- klines
	}()
	conn.expectNoCommand(t)

	conn.send(t, klineReply("4@kline@day_1", 1900))
	conn.expectCommand(t)
	conn.send(t, klineReply("4@kline@day_1", 5000))
	if klines := receive(t, results); klines[0].StartTime.Unix() != 5000 {
		t.Errorf("request received klines from %d, want its own reply", klines[0].StartTime.Unix())
	}
}

func TestAbandonedRequestReleasesItsChannelAfterTimeToLive(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	client.abandonedRequestTTL = 50 * time.Millisecond
	runStream(t, client)
	conn := srv.accept(t)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, _ = client.RequestKlines(ctx, KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
	}()
	conn.expectCommand(t)
	cancel()

	errs := make(chan error, 1)
	go func() {
		_, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
		errs <- err
	}()
	conn.expectCommand(t)
	conn.send(t, klineReply("4@kline@day_1", 100))
	if err := receive(t, errs); err != nil {
		t.Errorf("RequestKlines returned %v", err)
	}
}

func TestRequestKlinesDiscardsRepliesThatDoNotFit(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	runStream(t, client)
	conn := srv.accept(t)
	results := make(chan []Kline, 1)
	go func() {
		klines, err := client.RequestKlines(context.Background(), KlineHistoryRequest{
			Symbol: "4", Interval: KlineInterval1Day, End: time.Unix(1000, 0),
		})
		if err != nil {
			t.Errorf("RequestKlines returned error: %v", err)
		}
		results <- klines
	}()
	conn.expectCommand(t)

	conn.send(t, klineReply("4@kline@day_1", 900, 1940))
	errs.expect(t, func(err error) bool { return strings.Contains(err.Error(), "does not fit") })
	conn.send(t, klineReply("4@kline@day_1", 800, 900))

	if klines := receive(t, results); len(klines) != 2 || klines[1].StartTime.Unix() != 900 {
		t.Errorf("RequestKlines returned %+v, want the fitting reply", klines)
	}
}

func TestListSubscriptionsAcceptsNullList(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)
	results := make(chan []ChannelInfo, 1)
	go func() {
		channels, err := client.ListSubscriptions(context.Background())
		if err != nil {
			t.Errorf("ListSubscriptions returned error: %v", err)
		}
		results <- channels
	}()
	conn.expectCommand(t)

	conn.send(t, `{"T":"resp","C":200,"subs":null}`)

	if channels := receive(t, results); len(channels) != 0 {
		t.Errorf("ListSubscriptions returned %+v, want no channels", channels)
	}
}

func TestStreamNormalizesDocumentedKlineInterval(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	klines, onKline := collect[Kline]()
	mustSubscribe(t)(client.SubscribeKlines(KlineStream{Symbol: "88066", Interval: KlineInterval1Minute}, onKline))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "88066@kline@min_1")

	conn.send(t, `{"T":"kline","instrumentId":88066,"startTime":1603732500,"endTime":1603732559,`+
		`"symbol":"USDTBTC","interval":"1m","open":"0.0010","close":"0.0020"}`)

	if kline := receive(t, klines); kline.Interval != KlineInterval1Minute {
		t.Errorf("kline interval = %q, want %q", kline.Interval, KlineInterval1Minute)
	}
}

func TestStreamReportsUpdatesOfTheWrongType(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	trades, onTrade := collect[Trade]()
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, onTrade))
	runStream(t, client)

	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")
	conn.send(t, `{"T":"ticker","channel":"4@trade","close":"1"}`)

	errs.expect(t, func(err error) bool { return strings.Contains(err.Error(), "does not take") })
	expectNothing(t, trades)
}

func TestHandlerStartingRequestDuringShutdownDoesNotHang(t *testing.T) {
	srv := newFakeStreamServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	var client *StreamClient
	client = newTestStreamClient(t, srv, WithStreamConnectHandler(func() {

		cancel()
		time.Sleep(50 * time.Millisecond)
		_, err := client.ListSubscriptions(context.Background())
		errs <- err
	}))
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	srv.accept(t)

	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	if err := receive(t, errs); !errors.Is(err, ErrStreamStopped) {
		t.Errorf("ListSubscriptions returned %v, want ErrStreamStopped", err)
	}
}

func TestRunRemainsExclusiveWhileWaitingForHandler(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mustSubscribe(t)(client.SubscribeTrades(TradeStream{Symbol: "4"}, func(Trade) {
		started <- struct{}{}
		<-release
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	conn := srv.accept(t)
	conn.expectSubscribe(t, "4@trade")
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1}`)
	receive(t, started)
	cancel()
	conn.awaitClose(t)
	time.Sleep(50 * time.Millisecond)

	secondCtx, cancelSecond := context.WithTimeout(context.Background(), testWait)
	defer cancelSecond()
	if err := client.Run(secondCtx); err == nil || !strings.Contains(err.Error(), "already executing") {
		t.Errorf("Run while the previous Run waits for its handler returned %v, want an error", err)
	}

	close(release)
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	runStream(t, client)
	srv.accept(t)
}

func TestRequestsFailOnceRunsContextIsDone(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	srv.accept(t)

	cancel()

	if _, err := client.ListSubscriptions(context.Background()); !errors.Is(err, ErrStreamStopped) {
		t.Errorf("ListSubscriptions returned %v, want ErrStreamStopped", err)
	}
	receive(t, done)
}

func TestRequestsAvoidClosingConnections(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	conn, _, err := websocket.DefaultDialer.Dial(srv.url, nil)
	if err != nil {
		t.Fatalf("dial the fake server: %v", err)
	}
	server := srv.accept(t)
	sess := newStreamSession(conn)
	t.Cleanup(func() { sess.terminate(errClosedByClient) })
	if err := client.begin(make(chan struct{})); err != nil {
		t.Fatalf("begin returned error: %v", err)
	}
	client.attach(sess)
	sess.refuseRequests(ErrStreamStopped)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = client.ListSubscriptions(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ListSubscriptions returned %v, want it to wait for another connection", err)
	}
	server.expectNoCommand(t)
}

func TestRequestThatCannotBeSentFailsAsDisconnected(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	conn, _, err := websocket.DefaultDialer.Dial(srv.url, nil)
	if err != nil {
		t.Fatalf("dial the fake server: %v", err)
	}
	srv.accept(t)
	if err := client.begin(make(chan struct{})); err != nil {
		t.Fatalf("begin returned error: %v", err)
	}
	client.attach(newStreamSession(conn))
	conn.UnderlyingConn().Close()

	_, err = client.ListSubscriptions(context.Background())

	if !errors.Is(err, ErrStreamDisconnected) {
		t.Errorf("ListSubscriptions returned %v, want ErrStreamDisconnected", err)
	}
}

func TestRequestsBeforeFirstRunWaitForConnection(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	errs := make(chan error, 1)
	go func() {
		_, err := client.ListSubscriptions(context.Background())
		errs <- err
	}()
	time.Sleep(20 * time.Millisecond)

	runStream(t, client)
	conn := srv.accept(t)
	conn.expectCommand(t)
	conn.send(t, `{"T":"resp","subs":[]}`)

	if err := receive(t, errs); err != nil {
		t.Errorf("ListSubscriptions returned %v", err)
	}
}

func TestRequestsAfterRunStopsFail(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	srv.accept(t)
	cancel()
	receive(t, done)

	if _, err := client.ListSubscriptions(context.Background()); !errors.Is(err, ErrStreamStopped) {
		t.Errorf("ListSubscriptions returned %v, want ErrStreamStopped", err)
	}
}

func TestRequestWaitingForItsChannel(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)
	first := make(chan error, 1)
	go func() {
		_, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
		first <- err
	}()
	conn.expectCommand(t)

	t.Run("gives up when its context ends", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := client.RequestKlines(ctx, KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("RequestKlines returned %v, want context.DeadlineExceeded", err)
		}
	})

	t.Run("fails when the connection closes", func(t *testing.T) {
		second := make(chan error, 1)
		go func() {
			_, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day})
			second <- err
		}()
		conn.expectNoCommand(t)

		conn.conn.Close()

		for _, errs := range []chan error{first, second} {
			if err := receive(t, errs); !errors.Is(err, ErrStreamDisconnected) {
				t.Errorf("RequestKlines returned %v, want ErrStreamDisconnected", err)
			}
		}
	})
}
