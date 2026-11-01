package coinstore

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestTickers(t *testing.T) {

	const response = `{"data":[{"channel":"ticker","count":47081,"close":"0.331114","open":"0.330995",` +
		`"high":"0.333552","low":"0.329754","amount":"547485.28858491","volume":"1652536.69","bidSize":"637.41",` +
		`"askSize":"3","bid":"0.331021","ask":"0.331238","symbol":"TRXUSDT","instrumentId":2}],"code":0}`
	client, api := serveOnce(t, http.StatusOK, response)

	tickers, err := client.Tickers(context.Background())
	if err != nil {
		t.Fatalf("Tickers returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/v1/market/tickers", "", "")
	assertUnsigned(t, req)
	assertEqual(t, tickers, []MarketTicker{{
		Ticker: Ticker{
			Symbol:      "TRXUSDT",
			SymbolID:    2,
			Open:        d("0.330995"),
			High:        d("0.333552"),
			Low:         d("0.329754"),
			Last:        d("0.331114"),
			Volume:      d("1652536.69"),
			QuoteVolume: d("547485.28858491"),
		},
		TradeCount: 47081,
		BestBid:    PriceLevel{Price: d("0.331021"), Quantity: d("637.41")},
		BestAsk:    PriceLevel{Price: d("0.331238"), Quantity: d("3")},
	}})
}

func TestOrderBook(t *testing.T) {

	const response = `{"data":{"channel":"4@depth@5","level":5,"lastPrice":"81762.56",` +
		`"a":[["81762.59","0.0762",-1],["81762.8","0.0561",-1]],` +
		`"b":[["81762.55","1.693815",1],["81762.44","0.014292",1]],"symbol":"BTCUSDT","instrumentId":4},"code":0}`
	client, api := serveOnce(t, http.StatusOK, response)

	book, err := client.OrderBook(context.Background(), "BTCUSDT", 5)
	if err != nil {
		t.Fatalf("OrderBook returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/v1/market/depth/BTCUSDT", "depth=5", "")
	assertUnsigned(t, req)
	assertEqual(t, book, OrderBook{
		Symbol:   "BTCUSDT",
		SymbolID: 4,
		Bids: []PriceLevel{
			{Price: d("81762.55"), Quantity: d("1.693815")},
			{Price: d("81762.44"), Quantity: d("0.014292")},
		},
		Asks: []PriceLevel{
			{Price: d("81762.59"), Quantity: d("0.0762")},
			{Price: d("81762.8"), Quantity: d("0.0561")},
		},
	})
}

func TestOrderBookDefaultDepthAndEscaping(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0,"data":{"a":[],"b":[]}}`)

	if _, err := client.OrderBook(context.Background(), "odd/symbol", 0); err != nil {
		t.Fatalf("OrderBook returned error: %v", err)
	}

	assertRequest(t, api.onlyRequest(t), http.MethodGet, "/api/v1/market/depth/odd%2Fsymbol", "", "")
}

func TestOrderBookRejectsMalformedLevels(t *testing.T) {
	for _, level := range []string{`["1"]`, `"1,2"`, `["x","1"]`, `["1",{}]`} {
		t.Run(level, func(t *testing.T) {
			client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":{"a":[`+level+`],"b":[]}}`)
			if _, err := client.OrderBook(context.Background(), "BTCUSDT", 0); err == nil {
				t.Error("OrderBook accepted a malformed price level")
			}
		})
	}
}

func TestKlines(t *testing.T) {

	const response = `{"data":{"channel":"4@kline@min_1","item":[{"amount":"15602.59935349","endTime":1792759499,` +
		`"interval":"min_1","startTime":1792759440,"trade":null,"firstTradeId":268063445,"lastTradeId":268063546,` +
		`"volume":"0.19086","buyPrice":null,"sellPrice":null,"buyNum":null,"sellNum":null,"close":"81747.77",` +
		`"open":"81752.95","high":"81752.97","low":"81745.83"}],"symbol":"BTCUSDT","instrumentId":4},"code":0}`
	client, api := serveOnce(t, http.StatusOK, response)

	klines, err := client.Klines(context.Background(), "BTCUSDT", KlineInterval1Minute, 2)
	if err != nil {
		t.Fatalf("Klines returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/v1/market/kline/BTCUSDT", "period=1min&size=2", "")
	assertUnsigned(t, req)
	assertEqual(t, klines, []Kline{{
		Symbol:       "BTCUSDT",
		SymbolID:     4,
		Interval:     KlineInterval1Minute,
		StartTime:    time.Unix(1792759440, 0).UTC(),
		EndTime:      time.Unix(1792759499, 0).UTC(),
		Open:         d("81752.95"),
		High:         d("81752.97"),
		Low:          d("81745.83"),
		Close:        d("81747.77"),
		Volume:       d("0.19086"),
		QuoteVolume:  d("15602.59935349"),
		FirstTradeID: 268063445,
		LastTradeID:  268063546,
	}})
}

func TestKlinesTranslatesIntervals(t *testing.T) {
	tests := []struct {
		interval KlineInterval
		period   string
	}{
		{KlineInterval1Minute, "1min"},
		{KlineInterval5Minutes, "5min"},
		{KlineInterval15Minutes, "15min"},
		{KlineInterval30Minutes, "30min"},
		{KlineInterval1Hour, "60min"},
		{KlineInterval4Hours, "4hour"},
		{KlineInterval12Hours, "12hour"},
		{KlineInterval1Day, "1day"},
		{KlineInterval1Week, "1week"},
	}
	for _, tt := range tests {
		t.Run(string(tt.interval), func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, `{"code":0,"data":{"item":[]}}`)
			if _, err := client.Klines(context.Background(), "BTCUSDT", tt.interval, 0); err != nil {
				t.Fatalf("Klines returned error: %v", err)
			}
			if got := api.onlyRequest(t).RawQuery; got != "period="+tt.period {
				t.Errorf("query = %q, want period=%s", got, tt.period)
			}
		})
	}
}

func TestRecentTrades(t *testing.T) {

	const response = `{"data":[{"channel":"4@trade","seq":268063751,"ts":1792759595000,"time":1792759595,` +
		`"tradeId":268063751,"price":"81763.56","volume":"0.000501","takerSide":"SELL","symbol":"BTCUSDT",` +
		`"instrumentId":4}],"code":0}`
	client, api := serveOnce(t, http.StatusOK, response)

	trades, err := client.RecentTrades(context.Background(), "BTCUSDT", 2)
	if err != nil {
		t.Fatalf("RecentTrades returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/v1/market/trade/BTCUSDT", "size=2", "")
	assertUnsigned(t, req)
	assertEqual(t, trades, []Trade{{
		Symbol:    "BTCUSDT",
		SymbolID:  4,
		TradeID:   268063751,
		Sequence:  268063751,
		Price:     d("81763.56"),
		Quantity:  d("0.000501"),
		TakerSide: SideSell,
		Time:      time.UnixMilli(1792759595000).UTC(),
	}})
}

func TestMarketDataOrdering(t *testing.T) {
	t.Run("klines oldest first", func(t *testing.T) {
		client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":{"item":[`+
			`{"startTime":1792759500},{"startTime":1792759380},{"startTime":1792759440}]}}`)

		klines, err := client.Klines(context.Background(), "BTCUSDT", KlineInterval1Minute, 0)
		if err != nil {
			t.Fatalf("Klines returned error: %v", err)
		}
		for i, want := range []int64{1792759380, 1792759440, 1792759500} {
			if got := klines[i].StartTime.Unix(); got != want {
				t.Errorf("kline %d starts at %d, want %d", i, got, want)
			}
		}
	})

	t.Run("trades newest first", func(t *testing.T) {
		client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":[{"tradeId":1},{"tradeId":3},{"tradeId":2}]}`)

		trades, err := client.RecentTrades(context.Background(), "BTCUSDT", 0)
		if err != nil {
			t.Fatalf("RecentTrades returned error: %v", err)
		}
		for i, want := range []int64{3, 2, 1} {
			if got := trades[i].TradeID; got != want {
				t.Errorf("trade %d has ID %d, want %d", i, got, want)
			}
		}
	})
}

func TestParseKlineInterval(t *testing.T) {
	tests := map[string]KlineInterval{
		"min_1": KlineInterval1Minute, "1min": KlineInterval1Minute, "1m": KlineInterval1Minute,
		"min_5": KlineInterval5Minutes, "5min": KlineInterval5Minutes, "5m": KlineInterval5Minutes,
		"min_15": KlineInterval15Minutes, "15min": KlineInterval15Minutes, "15m": KlineInterval15Minutes,
		"min_30": KlineInterval30Minutes, "30min": KlineInterval30Minutes, "30m": KlineInterval30Minutes,
		"hour_1": KlineInterval1Hour, "60min": KlineInterval1Hour, "1h": KlineInterval1Hour,
		"hour_4": KlineInterval4Hours, "4hour": KlineInterval4Hours, "4h": KlineInterval4Hours,
		"hour_12": KlineInterval12Hours, "12hour": KlineInterval12Hours, "12h": KlineInterval12Hours,
		"day_1": KlineInterval1Day, "1day": KlineInterval1Day, "1d": KlineInterval1Day,
		"week_1": KlineInterval1Week, "1week": KlineInterval1Week, "1w": KlineInterval1Week,
		"mon_1": KlineInterval1Month, "1M": KlineInterval1Month,
		"2min": KlineInterval("2min"),
	}
	for spelling, want := range tests {
		if got := parseKlineInterval(spelling); got != want {
			t.Errorf("parseKlineInterval(%q) = %q, want %q", spelling, got, want)
		}
	}
}

func TestTradeFallsBackToSecondTimestamp(t *testing.T) {
	trade := tradeJSON{Time: 1612685697}.toTrade()
	assertEqual(t, trade.Time, time.Unix(1612685697, 0).UTC())
}

func TestLatestPrices(t *testing.T) {
	const response = `{"code":0,"message":"","data":[{"id":1,"symbol":"btcusdt","price":"400"},` +
		`{"id":4,"symbol":"autusdt","price":"1"},{"id":7,"symbol":"ETHUSDT","price":"2429.93"}]}`

	t.Run("all symbols", func(t *testing.T) {
		client, api := serveOnce(t, http.StatusOK, response)

		prices, err := client.LatestPrices(context.Background())
		if err != nil {
			t.Fatalf("LatestPrices returned error: %v", err)
		}

		req := api.onlyRequest(t)
		assertRequest(t, req, http.MethodGet, "/api/v1/ticker/price", "", "")
		assertUnsigned(t, req)
		assertEqual(t, prices, []SymbolPrice{
			{Symbol: "btcusdt", SymbolID: 1, Price: d("400")},
			{Symbol: "autusdt", SymbolID: 4, Price: d("1")},
			{Symbol: "ETHUSDT", SymbolID: 7, Price: d("2429.93")},
		})
	})

	t.Run("selected symbols match regardless of case", func(t *testing.T) {
		client, api := serveOnce(t, http.StatusOK, response)

		prices, err := client.LatestPrices(context.Background(), "BTCUSDT", "ethusdt", "XRPUSDT")
		if err != nil {
			t.Fatalf("LatestPrices returned error: %v", err)
		}

		assertRequest(t, api.onlyRequest(t), http.MethodGet, "/api/v1/ticker/price", "", "")
		assertEqual(t, prices, []SymbolPrice{
			{Symbol: "btcusdt", SymbolID: 1, Price: d("400")},
			{Symbol: "ETHUSDT", SymbolID: 7, Price: d("2429.93")},
		})
	})
}

func TestMarketDataValidation(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)
	ctx := context.Background()
	calls := map[string]func() error{
		"order book without symbol": func() error { _, err := client.OrderBook(ctx, " ", 5); return err },
		"order book negative depth": func() error { _, err := client.OrderBook(ctx, "BTCUSDT", -1); return err },
		"klines without symbol":     func() error { _, err := client.Klines(ctx, "", KlineInterval1Day, 0); return err },
		"klines monthly interval":   func() error { _, err := client.Klines(ctx, "BTCUSDT", KlineInterval1Month, 0); return err },
		"klines unknown interval":   func() error { _, err := client.Klines(ctx, "BTCUSDT", "2min", 0); return err },
		"klines negative limit":     func() error { _, err := client.Klines(ctx, "BTCUSDT", KlineInterval1Day, -1); return err },
		"trades without symbol":     func() error { _, err := client.RecentTrades(ctx, "", 0); return err },
		"trades negative limit":     func() error { _, err := client.RecentTrades(ctx, "BTCUSDT", -5); return err },
		"prices with empty symbol":  func() error { _, err := client.LatestPrices(ctx, "BTCUSDT", ""); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("error = %v, want ErrInvalidRequest", err)
			}
		})
	}
	if n := api.requestCount(); n != 0 {
		t.Errorf("invalid requests reached the server %d times", n)
	}
}
