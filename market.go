package coinstore

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/anuketfluxus/coinstore-api-client/internal/flexjson"
	"github.com/shopspring/decimal"
)

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type KlineInterval string

const (
	KlineInterval1Minute   KlineInterval = "min_1"
	KlineInterval5Minutes  KlineInterval = "min_5"
	KlineInterval15Minutes KlineInterval = "min_15"
	KlineInterval30Minutes KlineInterval = "min_30"
	KlineInterval1Hour     KlineInterval = "hour_1"
	KlineInterval4Hours    KlineInterval = "hour_4"
	KlineInterval12Hours   KlineInterval = "hour_12"
	KlineInterval1Day      KlineInterval = "day_1"
	KlineInterval1Week     KlineInterval = "week_1"
	KlineInterval1Month    KlineInterval = "mon_1"
)

func (i KlineInterval) restPeriod() (period string, ok bool) {
	switch i {
	case KlineInterval1Minute:
		return "1min", true
	case KlineInterval5Minutes:
		return "5min", true
	case KlineInterval15Minutes:
		return "15min", true
	case KlineInterval30Minutes:
		return "30min", true
	case KlineInterval1Hour:
		return "60min", true
	case KlineInterval4Hours:
		return "4hour", true
	case KlineInterval12Hours:
		return "12hour", true
	case KlineInterval1Day:
		return "1day", true
	case KlineInterval1Week:
		return "1week", true
	default:
		return "", false
	}
}

func (i KlineInterval) isStreamable() bool {
	_, ok := i.restPeriod()
	return ok || i == KlineInterval1Month
}

func parseKlineInterval(spelling string) KlineInterval {
	switch spelling {
	case "1m", "1min":
		return KlineInterval1Minute
	case "5m", "5min":
		return KlineInterval5Minutes
	case "15m", "15min":
		return KlineInterval15Minutes
	case "30m", "30min":
		return KlineInterval30Minutes
	case "1h", "60min":
		return KlineInterval1Hour
	case "4h", "4hour":
		return KlineInterval4Hours
	case "12h", "12hour":
		return KlineInterval12Hours
	case "1d", "1day":
		return KlineInterval1Day
	case "1w", "1week":
		return KlineInterval1Week
	case "1M":
		return KlineInterval1Month
	default:
		return KlineInterval(spelling)
	}
}

type Ticker struct {
	Symbol   string
	SymbolID int64

	Open decimal.Decimal
	High decimal.Decimal
	Low  decimal.Decimal
	Last decimal.Decimal

	Volume      decimal.Decimal
	QuoteVolume decimal.Decimal
}

type MarketTicker struct {
	Ticker
	TradeCount int64
	BestBid    PriceLevel
	BestAsk    PriceLevel
}

type PriceLevel struct {
	Price    decimal.Decimal
	Quantity decimal.Decimal
}

type OrderBook struct {
	Symbol   string
	SymbolID int64

	Bids []PriceLevel
	Asks []PriceLevel
}

type Kline struct {
	Symbol   string
	SymbolID int64
	Interval KlineInterval

	StartTime time.Time
	EndTime   time.Time
	Open      decimal.Decimal
	High      decimal.Decimal
	Low       decimal.Decimal
	Close     decimal.Decimal

	Volume       decimal.Decimal
	QuoteVolume  decimal.Decimal
	FirstTradeID int64
	LastTradeID  int64
}

type Trade struct {
	Symbol   string
	SymbolID int64
	TradeID  int64

	Sequence  int64
	Price     decimal.Decimal
	Quantity  decimal.Decimal
	TakerSide Side
	Time      time.Time
}

type SymbolPrice struct {
	Symbol   string
	SymbolID int64
	Price    decimal.Decimal
}

func (c *Client) Tickers(ctx context.Context) ([]MarketTicker, error) {
	var tickers []marketTickerJSON
	if err := c.do(ctx, c.get("/v1/market/tickers", nil, public), &tickers); err != nil {
		return nil, err
	}
	return convertAll(tickers, marketTickerJSON.toMarketTicker), nil
}

func (c *Client) OrderBook(ctx context.Context, symbol string, depth int) (OrderBook, error) {
	if err := validateSymbol(symbol); err != nil {
		return OrderBook{}, err
	}
	if err := validateNotNegative("depth", depth); err != nil {
		return OrderBook{}, err
	}
	query := url.Values{}
	if depth > 0 {
		query.Set("depth", strconv.Itoa(depth))
	}

	var book orderBookJSON
	if err := c.do(ctx, c.get(marketPath("depth", symbol), query, public), &book); err != nil {
		return OrderBook{}, err
	}
	return book.toOrderBook(), nil
}

func (c *Client) Klines(ctx context.Context, symbol string, interval KlineInterval, limit int) ([]Kline, error) {
	if err := validateSymbol(symbol); err != nil {
		return nil, err
	}
	period, ok := interval.restPeriod()
	if !ok {
		return nil, invalidRequest("kline interval %q is not available from the REST API", interval)
	}
	if err := validateNotNegative("limit", limit); err != nil {
		return nil, err
	}
	query := url.Values{"period": {period}}
	if limit > 0 {
		query.Set("size", strconv.Itoa(limit))
	}

	var series klineSeriesJSON
	if err := c.do(ctx, c.get(marketPath("kline", symbol), query, public), &series); err != nil {
		return nil, err
	}
	return series.toKlines(), nil
}

func (c *Client) RecentTrades(ctx context.Context, symbol string, limit int) ([]Trade, error) {
	if err := validateSymbol(symbol); err != nil {
		return nil, err
	}
	if err := validateNotNegative("limit", limit); err != nil {
		return nil, err
	}
	query := url.Values{}
	if limit > 0 {
		query.Set("size", strconv.Itoa(limit))
	}

	var tradesJSON []tradeJSON
	if err := c.do(ctx, c.get(marketPath("trade", symbol), query, public), &tradesJSON); err != nil {
		return nil, err
	}
	trades := convertAll(tradesJSON, tradeJSON.toTrade)
	slices.SortStableFunc(trades, func(a, b Trade) int {
		return cmp.Compare(b.TradeID, a.TradeID)
	})
	return trades, nil
}

func (c *Client) LatestPrices(ctx context.Context, symbols ...string) ([]SymbolPrice, error) {
	wanted := make(map[string]bool, len(symbols))
	for _, symbol := range symbols {
		if err := validateSymbol(symbol); err != nil {
			return nil, err
		}
		wanted[strings.ToUpper(symbol)] = true
	}

	var prices []symbolPriceJSON
	if err := c.do(ctx, c.get("/v1/ticker/price", nil, public), &prices); err != nil {
		return nil, err
	}
	result := make([]SymbolPrice, 0, len(prices))
	for _, price := range prices {
		if len(wanted) == 0 || wanted[strings.ToUpper(price.Symbol)] {
			result = append(result, price.toSymbolPrice())
		}
	}
	return result, nil
}

func marketPath(resource, symbol string) string {
	return "/v1/market/" + resource + "/" + url.PathEscape(symbol)
}

type tickerJSON struct {
	Symbol       string           `json:"symbol"`
	InstrumentID flexjson.Int64   `json:"instrumentId"`
	Open         flexjson.Decimal `json:"open"`
	High         flexjson.Decimal `json:"high"`
	Low          flexjson.Decimal `json:"low"`
	Close        flexjson.Decimal `json:"close"`
	Volume       flexjson.Decimal `json:"volume"`
	Amount       flexjson.Decimal `json:"amount"`
}

func (j tickerJSON) toTicker() Ticker {
	return Ticker{
		Symbol:      j.Symbol,
		SymbolID:    int64(j.InstrumentID),
		Open:        dec(j.Open),
		High:        dec(j.High),
		Low:         dec(j.Low),
		Last:        dec(j.Close),
		Volume:      dec(j.Volume),
		QuoteVolume: dec(j.Amount),
	}
}

type marketTickerJSON struct {
	tickerJSON
	Count   flexjson.Int64   `json:"count"`
	Bid     flexjson.Decimal `json:"bid"`
	BidSize flexjson.Decimal `json:"bidSize"`
	Ask     flexjson.Decimal `json:"ask"`
	AskSize flexjson.Decimal `json:"askSize"`
}

func (j marketTickerJSON) toMarketTicker() MarketTicker {
	return MarketTicker{
		Ticker:     j.toTicker(),
		TradeCount: int64(j.Count),
		BestBid:    PriceLevel{Price: dec(j.Bid), Quantity: dec(j.BidSize)},
		BestAsk:    PriceLevel{Price: dec(j.Ask), Quantity: dec(j.AskSize)},
	}
}

type orderBookJSON struct {
	Symbol       string           `json:"symbol"`
	InstrumentID flexjson.Int64   `json:"instrumentId"`
	Bids         []priceLevelJSON `json:"b"`
	Asks         []priceLevelJSON `json:"a"`
}

func (j orderBookJSON) toOrderBook() OrderBook {
	return OrderBook{
		Symbol:   j.Symbol,
		SymbolID: int64(j.InstrumentID),
		Bids:     convertAll(j.Bids, priceLevelJSON.toPriceLevel),
		Asks:     convertAll(j.Asks, priceLevelJSON.toPriceLevel),
	}
}

type priceLevelJSON struct {
	price    flexjson.Decimal
	quantity flexjson.Decimal
}

func (p *priceLevelJSON) UnmarshalJSON(data []byte) error {
	var elements []json.RawMessage
	if err := json.Unmarshal(data, &elements); err != nil {
		return fmt.Errorf("decode price level: %w", err)
	}
	if len(elements) < 2 {
		return fmt.Errorf("decode price level: want price and quantity, got %d elements", len(elements))
	}
	if err := p.price.UnmarshalJSON(elements[0]); err != nil {
		return fmt.Errorf("decode price level price: %w", err)
	}
	if err := p.quantity.UnmarshalJSON(elements[1]); err != nil {
		return fmt.Errorf("decode price level quantity: %w", err)
	}
	return nil
}

func (p priceLevelJSON) toPriceLevel() PriceLevel {
	return PriceLevel{Price: dec(p.price), Quantity: dec(p.quantity)}
}

type klineJSON struct {
	Symbol       string           `json:"symbol"`
	InstrumentID flexjson.Int64   `json:"instrumentId"`
	Interval     string           `json:"interval"`
	StartTime    flexjson.Int64   `json:"startTime"`
	EndTime      flexjson.Int64   `json:"endTime"`
	Open         flexjson.Decimal `json:"open"`
	High         flexjson.Decimal `json:"high"`
	Low          flexjson.Decimal `json:"low"`
	Close        flexjson.Decimal `json:"close"`
	Volume       flexjson.Decimal `json:"volume"`
	Amount       flexjson.Decimal `json:"amount"`
	FirstTradeID flexjson.Int64   `json:"firstTradeId"`
	LastTradeID  flexjson.Int64   `json:"lastTradeId"`
}

func (j klineJSON) toKline() Kline {
	return Kline{
		Symbol:       j.Symbol,
		SymbolID:     int64(j.InstrumentID),
		Interval:     parseKlineInterval(j.Interval),
		StartTime:    timeFromUnix(j.StartTime),
		EndTime:      timeFromUnix(j.EndTime),
		Open:         dec(j.Open),
		High:         dec(j.High),
		Low:          dec(j.Low),
		Close:        dec(j.Close),
		Volume:       dec(j.Volume),
		QuoteVolume:  dec(j.Amount),
		FirstTradeID: int64(j.FirstTradeID),
		LastTradeID:  int64(j.LastTradeID),
	}
}

type klineSeriesJSON struct {
	Symbol       string         `json:"symbol"`
	InstrumentID flexjson.Int64 `json:"instrumentId"`
	Items        []klineJSON    `json:"item"`
}

func (j klineSeriesJSON) toKlines() []Kline {
	klines := convertAll(j.Items, func(item klineJSON) Kline {
		kline := item.toKline()
		kline.Symbol = j.Symbol
		kline.SymbolID = int64(j.InstrumentID)
		return kline
	})
	slices.SortStableFunc(klines, func(a, b Kline) int {
		return a.StartTime.Compare(b.StartTime)
	})
	return klines
}

type tradeJSON struct {
	Symbol       string           `json:"symbol"`
	InstrumentID flexjson.Int64   `json:"instrumentId"`
	TradeID      flexjson.Int64   `json:"tradeId"`
	Seq          flexjson.Int64   `json:"seq"`
	Price        flexjson.Decimal `json:"price"`
	Volume       flexjson.Decimal `json:"volume"`
	TakerSide    string           `json:"takerSide"`

	Time flexjson.Int64 `json:"time"`
	TS   flexjson.Int64 `json:"ts"`
}

func (j tradeJSON) toTrade() Trade {
	tradeTime := timeFromUnixMilli(j.TS)
	if tradeTime.IsZero() {
		tradeTime = timeFromUnix(j.Time)
	}
	return Trade{
		Symbol:    j.Symbol,
		SymbolID:  int64(j.InstrumentID),
		TradeID:   int64(j.TradeID),
		Sequence:  int64(j.Seq),
		Price:     dec(j.Price),
		Quantity:  dec(j.Volume),
		TakerSide: Side(j.TakerSide),
		Time:      tradeTime,
	}
}

type symbolPriceJSON struct {
	ID     flexjson.Int64   `json:"id"`
	Symbol string           `json:"symbol"`
	Price  flexjson.Decimal `json:"price"`
}

func (j symbolPriceJSON) toSymbolPrice() SymbolPrice {
	return SymbolPrice{Symbol: j.Symbol, SymbolID: int64(j.ID), Price: dec(j.Price)}
}
