package coinstore

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestOpenOrders(t *testing.T) {

	const response = `{"data":[{"symbol":"LUFFYUSDT","baseCurrency":"LUFFY","quoteCurrency":"USDT",` +
		`"timestamp":1642402062196,"side":"BUY","timeInForce":"GTC","accountId":1138204,"ordPrice":9.1E-10,` +
		`"cumAmt":"0","cumQty":"0","leavesQty":"0","clOrdId":"b1b2ea5e00a84b888d419cb73f8eb203","ordAmt":"0",` +
		`"ordQty":"1","ordId":"1722183748419690","ordStatus":"SUBMITTED","ordType":"LIMIT"}],"code":0}`
	client, api := serveOnce(t, http.StatusOK, response)

	orders, err := client.OpenOrders(context.Background())
	if err != nil {
		t.Fatalf("OpenOrders returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/trade/order/active", "", "")
	assertSigned(t, req)
	assertEqual(t, orders, []Order{{
		OrderID:           1722183748419690,
		ClientOrderID:     "b1b2ea5e00a84b888d419cb73f8eb203",
		AccountID:         1138204,
		Symbol:            "LUFFYUSDT",
		BaseCurrency:      "LUFFY",
		QuoteCurrency:     "USDT",
		Side:              SideBuy,
		Type:              OrderTypeLimit,
		TimeInForce:       TimeInForceGTC,
		Status:            OrderStatusSubmitted,
		Price:             d("9.1E-10"),
		Quantity:          d("1"),
		QuoteAmount:       d("0"),
		FilledQuantity:    d("0"),
		FilledAmount:      d("0"),
		RemainingQuantity: d("0"),
		CreatedAt:         time.UnixMilli(1642402062196).UTC(),
	}})
}

const openOrdersV2Response = `{"data":[{"baseCurrency":"A123","quoteCurrency":"USDT","side":"BUY","cumQty":"0",` +
	`"ordId":1758065956225025,"clOrdId":"tLCGVA4g19zuEBwsITXi9g3U624Al0Bw","ordType":"LIMIT","ordQty":"10",` +
	`"cumAmt":"0","accountId":1134912,"timeInForce":"GTC","ordPrice":"100","leavesQty":"10","avgPrice":"0",` +
	`"ordStatus":"SUBMITTED","symbol":"A123USDT","timestamp":1676622290389}],"code":0}`

const orderInfoV2Response = `{"data":[{"baseCurrency":"A123","quoteCurrency":"USDT","side":"BUY","cumQty":"0",` +
	`"ordId":1758065956225025,"clOrdId":"tLCGVA4g19zuEBwsITXi9g3U624Al0Bw","ordType":"LIMIT","ordQty":"10",` +
	`"cumAmt":"0","accountId":1134912,"timeInForce":"GTC","ordPrice":"100","leavesQty":"10","avgPrice":"0",` +
	`"ordStatus":"SUBMITTED","symbol":"A123USDT","orderUpdateTime":1676622290389,"timestamp":1676622290389}],"code":0}`

var openOrderV2 = Order{
	OrderID:           1758065956225025,
	ClientOrderID:     "tLCGVA4g19zuEBwsITXi9g3U624Al0Bw",
	AccountID:         1134912,
	Symbol:            "A123USDT",
	BaseCurrency:      "A123",
	QuoteCurrency:     "USDT",
	Side:              SideBuy,
	Type:              OrderTypeLimit,
	TimeInForce:       TimeInForceGTC,
	Status:            OrderStatusSubmitted,
	Price:             d("100"),
	Quantity:          d("10"),
	FilledQuantity:    d("0"),
	FilledAmount:      d("0"),
	RemainingQuantity: d("10"),
	AveragePrice:      d("0"),
	CreatedAt:         time.UnixMilli(1676622290389).UTC(),
}

func TestOpenOrdersV2(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, openOrdersV2Response)

	orders, err := client.OpenOrdersV2(context.Background(), OpenOrdersFilter{
		Symbol:        "A123USDT",
		OrderID:       1758065956225025,
		ClientOrderID: "tLCGVA4g19zuEBwsITXi9g3U624Al0Bw",
	})
	if err != nil {
		t.Fatalf("OpenOrdersV2 returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/v2/trade/order/active",
		"clOrdId=tLCGVA4g19zuEBwsITXi9g3U624Al0Bw&ordId=1758065956225025&symbol=A123USDT", "")
	assertSigned(t, req)
	assertEqual(t, orders, []Order{openOrderV2})
}

func TestOpenOrdersV2WithoutFilter(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0,"data":[]}`)

	if _, err := client.OpenOrdersV2(context.Background(), OpenOrdersFilter{}); err != nil {
		t.Fatalf("OpenOrdersV2 returned error: %v", err)
	}

	assertRequest(t, api.onlyRequest(t), http.MethodGet, "/api/v2/trade/order/active", "", "")
}

func TestOrder(t *testing.T) {

	const response = `{"data":{"baseCurrency":"LUFFY","quoteCurrency":"USDT","symbol":"LUFFYUSDT",` +
		`"timestamp":1642574848089,"side":"BUY","accountId":1138204,"ordId":1722364585836586,` +
		`"clOrdId":"d0344830639d4f6d94be4110bf98c759","ordType":"LIMIT","ordState":"CANCELED",` +
		`"ordPrice":"0.00000000092","ordQty":"1","ordAmt":"0.00000000092","cumAmt":"0","cumQty":"0",` +
		`"leavesQty":"0","avgPrice":"0","feeCurrency":"LUFFY","timeInForce":"GTC"},"code":0}`
	client, api := serveOnce(t, http.StatusOK, response)

	order, err := client.Order(context.Background(), 1722364585836586)
	if err != nil {
		t.Fatalf("Order returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/trade/order/orderInfo", "ordId=1722364585836586", "")
	assertSigned(t, req)
	assertEqual(t, order, Order{
		OrderID:           1722364585836586,
		ClientOrderID:     "d0344830639d4f6d94be4110bf98c759",
		AccountID:         1138204,
		Symbol:            "LUFFYUSDT",
		BaseCurrency:      "LUFFY",
		QuoteCurrency:     "USDT",
		Side:              SideBuy,
		Type:              OrderTypeLimit,
		TimeInForce:       TimeInForceGTC,
		Status:            OrderStatusCanceled,
		Price:             d("0.00000000092"),
		Quantity:          d("1"),
		QuoteAmount:       d("0.00000000092"),
		FilledQuantity:    d("0"),
		FilledAmount:      d("0"),
		RemainingQuantity: d("0"),
		AveragePrice:      d("0"),
		FeeCurrency:       "LUFFY",
		CreatedAt:         time.UnixMilli(1642574848089).UTC(),
	})
}

func TestOrderV2(t *testing.T) {
	tests := []struct {
		name      string
		lookup    OrderLookup
		wantQuery string
	}{
		{"by order ID", OrderLookup{OrderID: 1758065956225025}, "ordId=1758065956225025"},
		{"by client order ID", OrderLookup{ClientOrderID: "tLCGVA4g19zuEBwsITXi9g3U624Al0Bw"}, "clOrdId=tLCGVA4g19zuEBwsITXi9g3U624Al0Bw"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, orderInfoV2Response)

			order, err := client.OrderV2(context.Background(), tt.lookup)
			if err != nil {
				t.Fatalf("OrderV2 returned error: %v", err)
			}

			req := api.onlyRequest(t)
			assertRequest(t, req, http.MethodGet, "/api/v2/trade/order/orderInfo", tt.wantQuery, "")
			assertSigned(t, req)
			want := openOrderV2
			want.UpdatedAt = time.UnixMilli(1676622290389).UTC()
			assertEqual(t, order, want)
		})
	}
}

func TestOrderV2DecodesASingleOrder(t *testing.T) {
	response := strings.Replace(strings.Replace(orderInfoV2Response, `"data":[{`, `"data":{`, 1), `}],"code"`, `},"code"`, 1)
	client, _ := serveOnce(t, http.StatusOK, response)

	order, err := client.OrderV2(context.Background(), OrderLookup{OrderID: 1758065956225025})
	if err != nil {
		t.Fatalf("OrderV2 returned error: %v", err)
	}
	want := openOrderV2
	want.UpdatedAt = time.UnixMilli(1676622290389).UTC()
	assertEqual(t, order, want)
}

func TestOrderNotFound(t *testing.T) {
	tests := map[string]string{
		"null data":             `{"code":0,"data":null}`,
		"empty object":          `{"code":0,"data":{}}`,
		"not-found status":      `{"code":0,"data":{"ordId":1,"ordState":"NOT_FOUND"}}`,
		"order-not-found error": `{"code":3103,"message":"order-not-found"}`,
	}
	for name, response := range tests {
		t.Run(name, func(t *testing.T) {
			client, _ := serveOnce(t, http.StatusOK, response)

			_, err := client.Order(context.Background(), 1)

			if !errors.Is(err, ErrOrderNotFound) {
				t.Errorf("Order error = %v, want ErrOrderNotFound", err)
			}
		})
	}
}

func TestOrderV2NotFound(t *testing.T) {
	tests := map[string]string{
		"empty list":                               `{"code":0,"data":[]}`,
		"null data":                                `{"code":0,"data":null}`,
		"not-found status":                         `{"code":0,"data":[{"ordId":1,"ordStatus":"NOT_FOUND"}]}`,
		"a single not-found order":                 `{"code":0,"data":{"ordId":1,"ordStatus":"NOT_FOUND"}}`,
		"an empty object":                          `{"code":0,"data":{}}`,
		"order-not-found error":                    `{"code":3103,"message":"order-not-found"}`,
		"order-not-found error with a string code": `{"code":"3103","message":"order-not-found","data":null}`,
	}
	for name, response := range tests {
		t.Run(name, func(t *testing.T) {
			client, _ := serveOnce(t, http.StatusOK, response)

			_, err := client.OrderV2(context.Background(), OrderLookup{OrderID: 1})

			if !errors.Is(err, ErrOrderNotFound) {
				t.Errorf("OrderV2 error = %v, want ErrOrderNotFound", err)
			}
		})
	}
}

func TestOrderV2RejectsAmbiguousResponse(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":[{"ordId":1},{"ordId":2}]}`)

	_, err := client.OrderV2(context.Background(), OrderLookup{ClientOrderID: "dup"})

	if err == nil || !strings.Contains(err.Error(), "matched 2 orders") {
		t.Errorf("OrderV2 error = %v, want an ambiguity error", err)
	}
}

func TestFills(t *testing.T) {

	const response = `{"data":[{"id":3984987,"remainingQty":0E-18,"matchRole":1,"feeCurrencyId":44,` +
		`"acturalFeeRate":0.002000000000000000,"role":1,"accountId":1138204,"instrumentId":15,` +
		`"baseCurrencyId":44,"quoteCurrencyId":30,"execQty":40.900000000000000000,"orderState":50,` +
		`"matchId":258338866,"orderId":1717384395096065,"side":1,"execAmt":8.887161000000000000,` +
		`"selfDealingQty":0E-18,"tradeId":11523732,"fee":0.081800000000000000,"matchTime":1637825389,` +
		`"seq":null}],"code":0}`
	client, api := serveOnce(t, http.StatusOK, response)

	fills, err := client.Fills(context.Background(), "tipusdt", FillsFilter{
		OrderID: 1717384395096065, Side: SideSell, Page: 2, PageSize: 50,
	})
	if err != nil {
		t.Fatalf("Fills returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/trade/match/accountMatches",
		"ordId=1717384395096065&pageNum=2&pageSize=50&side=-1&symbol=tipusdt", "")
	assertSigned(t, req)
	assertEqual(t, fills, []Fill{{
		ID:                3984987,
		TradeID:           11523732,
		MatchID:           258338866,
		OrderID:           1717384395096065,
		AccountID:         1138204,
		SymbolID:          15,
		BaseCurrencyID:    44,
		QuoteCurrencyID:   30,
		Side:              SideBuy,
		Role:              MatchRoleTaker,
		Quantity:          d("40.900000000000000000"),
		QuoteAmount:       d("8.887161000000000000"),
		Fee:               d("0.081800000000000000"),
		FeeCurrencyID:     44,
		FeeRate:           d("0.002000000000000000"),
		RemainingQuantity: d("0E-18"),
		SelfTradeQuantity: d("0E-18"),
		OrderState:        50,
		Time:              time.Unix(1637825389, 0).UTC(),
	}})
}

func TestFillCodes(t *testing.T) {
	tests := []struct {
		side, matchRole, role string
		wantSide              Side
		wantRole              MatchRole
	}{
		{`-1`, `-1`, ``, SideSell, MatchRoleMaker},
		{`"BUY"`, ``, `"TAKER"`, SideBuy, MatchRoleTaker},
		{`"SELL"`, `"MAKER"`, `1`, SideSell, MatchRoleMaker},
	}
	for _, tt := range tests {
		fields := []string{`"side":` + tt.side}
		if tt.matchRole != "" {
			fields = append(fields, `"matchRole":`+tt.matchRole)
		}
		if tt.role != "" {
			fields = append(fields, `"role":`+tt.role)
		}
		client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":[{`+strings.Join(fields, ",")+`}]}`)

		fills, err := client.Fills(context.Background(), "BTCUSDT", FillsFilter{})
		if err != nil {
			t.Fatalf("Fills returned error: %v", err)
		}
		if fills[0].Side != tt.wantSide || fills[0].Role != tt.wantRole {
			t.Errorf("fill with %v decoded as side %q role %q, want %q %q", fields, fills[0].Side, fills[0].Role, tt.wantSide, tt.wantRole)
		}
	}
}

func TestPlaceOrder(t *testing.T) {
	tests := []struct {
		name     string
		order    OrderRequest
		wantBody string
	}{
		{
			name: "limit order",
			order: OrderRequest{
				ClientOrderID: "my-order-1",
				Side:          SideBuy,
				Type:          OrderTypeLimit,
				TimeInForce:   TimeInForceGTC,
				Price:         d("30000.00"),
				Quantity:      d("1"),
			},
			wantBody: `{"symbol":"BTCUSDT","clOrdId":"my-order-1","side":"BUY","ordType":"LIMIT","timeInForce":"GTC",` +
				`"ordPrice":"30000","ordQty":"1","timestamp":1629291143107}`,
		},
		{
			name:     "post-only order with a tiny price",
			order:    OrderRequest{Side: SideSell, Type: OrderTypePostOnly, Price: d("9.2e-10"), Quantity: d("12323231243")},
			wantBody: `{"symbol":"BTCUSDT","side":"SELL","ordType":"POST_ONLY","ordPrice":"0.00000000092","ordQty":"12323231243","timestamp":1629291143107}`,
		},
		{
			name:     "market buy order",
			order:    OrderRequest{Side: SideBuy, Type: OrderTypeMarket, QuoteAmount: d("25.5")},
			wantBody: `{"symbol":"BTCUSDT","side":"BUY","ordType":"MARKET","ordAmt":"25.5","timestamp":1629291143107}`,
		},
		{
			name:     "market sell order",
			order:    OrderRequest{Side: SideSell, Type: OrderTypeMarket, TimeInForce: TimeInForceIOC, Quantity: d("0.5")},
			wantBody: `{"symbol":"BTCUSDT","side":"SELL","ordType":"MARKET","timeInForce":"IOC","ordQty":"0.5","timestamp":1629291143107}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			client, api := serveOnce(t, http.StatusOK, `{"code":"0","data":{"order_id":11594964764657880}}`)

			orderID, err := client.PlaceOrder(context.Background(), "BTCUSDT", tt.order)
			if err != nil {
				t.Fatalf("PlaceOrder returned error: %v", err)
			}

			req := api.onlyRequest(t)
			assertRequest(t, req, http.MethodPost, "/api/trade/order/place", "", tt.wantBody)
			assertSigned(t, req)
			if orderID != 11594964764657880 {
				t.Errorf("PlaceOrder = %d, want 11594964764657880", orderID)
			}
		})
	}
}

func TestPlaceOrderReadsDocumentedIDKey(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":{"ordId":"1722364585836585"}}`)

	orderID, err := client.PlaceOrder(context.Background(), "BTCUSDT", OrderRequest{
		Side: SideBuy, Type: OrderTypeLimit, Price: d("1"), Quantity: d("1"),
	})
	if err != nil {
		t.Fatalf("PlaceOrder returned error: %v", err)
	}
	if orderID != 1722364585836585 {
		t.Errorf("PlaceOrder = %d, want 1722364585836585", orderID)
	}
}

func TestPlaceOrderRequiresIDInResponse(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":{}}`)

	_, err := client.PlaceOrder(context.Background(), "BTCUSDT", OrderRequest{
		Side: SideBuy, Type: OrderTypeLimit, Price: d("1"), Quantity: d("1"),
	})

	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Errorf("PlaceOrder error = %v, want ErrOutcomeUnknown", err)
	}
}

func TestOrderRequestValidation(t *testing.T) {
	tests := map[string]OrderRequest{
		"missing side":               {Type: OrderTypeLimit, Price: d("1"), Quantity: d("1")},
		"unknown side":               {Side: "HOLD", Type: OrderTypeLimit, Price: d("1"), Quantity: d("1")},
		"missing type":               {Side: SideBuy, Price: d("1"), Quantity: d("1")},
		"unknown type":               {Side: SideBuy, Type: "STOP", Price: d("1"), Quantity: d("1")},
		"limit without price":        {Side: SideBuy, Type: OrderTypeLimit, Quantity: d("1")},
		"limit without quantity":     {Side: SideBuy, Type: OrderTypeLimit, Price: d("1")},
		"post-only without price":    {Side: SideSell, Type: OrderTypePostOnly, Quantity: d("1")},
		"market buy without amount":  {Side: SideBuy, Type: OrderTypeMarket, Quantity: d("1")},
		"market sell without amount": {Side: SideSell, Type: OrderTypeMarket, QuoteAmount: d("1")},
		"negative price":             {Side: SideBuy, Type: OrderTypeMarket, QuoteAmount: d("1"), Price: d("-1")},
		"negative quantity":          {Side: SideSell, Type: OrderTypeLimit, Price: d("1"), Quantity: d("-1")},
		"negative quote amount":      {Side: SideSell, Type: OrderTypeMarket, Quantity: d("1"), QuoteAmount: d("-1")},
		"limit with quote amount":    {Side: SideBuy, Type: OrderTypeLimit, Price: d("1"), Quantity: d("1"), QuoteAmount: d("1")},
		"post-only with quote amount": {
			Side: SideSell, Type: OrderTypePostOnly, Price: d("1"), Quantity: d("1"), QuoteAmount: d("1"),
		},
		"market with price":             {Side: SideBuy, Type: OrderTypeMarket, QuoteAmount: d("100"), Price: d("80000")},
		"market buy with quantity":      {Side: SideBuy, Type: OrderTypeMarket, QuoteAmount: d("100"), Quantity: d("1")},
		"market sell with quote amount": {Side: SideSell, Type: OrderTypeMarket, Quantity: d("1"), QuoteAmount: d("100")},
	}
	for name, order := range tests {
		t.Run(name, func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, `{"code":0}`)

			if _, err := client.PlaceOrder(context.Background(), "BTCUSDT", order); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("PlaceOrder error = %v, want ErrInvalidRequest", err)
			}
			if n := api.requestCount(); n != 0 {
				t.Errorf("server received %d requests, want none", n)
			}
		})
	}
}

func TestPlaceOrderRequiresSymbol(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0}`)
	order := OrderRequest{Side: SideBuy, Type: OrderTypeLimit, Price: d("1"), Quantity: d("1")}

	if _, err := client.PlaceOrder(context.Background(), "", order); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("PlaceOrder error = %v, want ErrInvalidRequest", err)
	}
}

func batchOrders(t *testing.T, req capturedRequest) []string {
	t.Helper()
	var body struct {
		Orders []struct {
			ClOrdID string `json:"clOrdId"`
		} `json:"orders"`
	}
	if err := json.Unmarshal([]byte(req.Body), &body); err != nil {
		t.Errorf("decode batch request %s: %v", req.Body, err)
	}
	ids := make([]string, len(body.Orders))
	for i, order := range body.Orders {
		ids[i] = order.ClOrdID
	}
	return ids
}

func TestPlaceOrders(t *testing.T) {
	api := newRespondingAPI(t, func(req capturedRequest) (int, string) {
		ids := batchOrders(t, req)

		return http.StatusOK, `{"code":0,"data":[` +
			`{"ordId":0,"clOrdId":"d0344830639d4f6d94be4110bf98c759","errno":3113,"errMsg":"account-insufficient"},` +
			`{"ordId":1722364585836585,"clOrdId":"` + ids[0] + `"}]}`
	})
	client := newTestClient(t, api)

	results, err := client.PlaceOrders(context.Background(), "BTCUSDT", []OrderRequest{
		{Side: SideBuy, Type: OrderTypeLimit, Price: d("28000"), Quantity: d("2")},
		{ClientOrderID: "d0344830639d4f6d94be4110bf98c759", Side: SideBuy, Type: OrderTypeLimit, Price: d("30000"), Quantity: d("1")},
	})
	if err != nil {
		t.Fatalf("PlaceOrders returned error: %v", err)
	}

	req := api.onlyRequest(t)
	generatedID := batchOrders(t, req)[0]
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(generatedID) {
		t.Fatalf("generated client order ID %q is not 32 hexadecimal digits", generatedID)
	}
	assertRequest(t, req, http.MethodPost, "/api/trade/order/placeBatch", "",
		`{"symbol":"BTCUSDT","orders":[`+
			`{"clOrdId":"`+generatedID+`","side":"BUY","ordType":"LIMIT","ordPrice":"28000","ordQty":"2"},`+
			`{"clOrdId":"d0344830639d4f6d94be4110bf98c759","side":"BUY","ordType":"LIMIT","ordPrice":"30000","ordQty":"1"}`+
			`],"timestamp":1629291143107}`)
	assertSigned(t, req)
	assertEqual(t, results, []PlaceOrderResult{
		{OrderID: 1722364585836585, ClientOrderID: generatedID},
		{
			ClientOrderID: "d0344830639d4f6d94be4110bf98c759",
			Err: &APIError{
				Method:     http.MethodPost,
				Path:       "/trade/order/placeBatch",
				HTTPStatus: http.StatusOK,
				Code:       CodeInsufficientBalance,
				Message:    "account-insufficient",
			},
		},
	})
}

func TestPlaceOrdersReportsUnconfirmedOrders(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":[`+
		`{"clOrdId":"rejected","errMsg":"rejected"},`+
		`{"clOrdId":"silent"},`+
		`{"ordId":7,"clOrdId":"stranger"},`+
		`{"ordId":8,"clOrdId":"contradicted","errno":3113,"errMsg":"account-insufficient"},`+
		`{"clOrdId":"failed","errno":"10001","errMsg":"System Error. Please Contact The System Administrator."}]}`)
	limit := OrderRequest{Side: SideSell, Type: OrderTypeLimit, Price: d("1"), Quantity: d("1")}
	clientOrderIDs := []string{"rejected", "silent", "omitted", "contradicted", "failed"}
	orders := make([]OrderRequest, len(clientOrderIDs))
	for i, id := range clientOrderIDs {
		orders[i] = limit
		orders[i].ClientOrderID = id
	}

	results, err := client.PlaceOrders(context.Background(), "BTCUSDT", orders)
	if err != nil {
		t.Fatalf("PlaceOrders returned error: %v", err)
	}

	if len(results) != len(orders) {
		t.Fatalf("PlaceOrders returned %d results, want one per order", len(results))
	}
	var apiErr *APIError
	if !errors.As(results[0].Err, &apiErr) || apiErr.Message != "rejected" || errors.Is(results[0].Err, ErrOutcomeUnknown) {
		t.Errorf("order rejected without an error code: Err = %v, want an APIError", results[0].Err)
	}
	for _, i := range []int{1, 2} {
		if results[i].OrderID != 0 || !errors.Is(results[i].Err, ErrOutcomeUnknown) {
			t.Errorf("order %q: result %+v, want no order ID and ErrOutcomeUnknown", results[i].ClientOrderID, results[i])
		}
	}
	contradicted := results[3]
	if contradicted.OrderID != 8 || !errors.Is(contradicted.Err, ErrOutcomeUnknown) ||
		!errors.As(contradicted.Err, &apiErr) || apiErr.Code != CodeInsufficientBalance {
		t.Errorf("order reported as placed and rejected: result %+v, want order ID 8 and ErrOutcomeUnknown wrapping the rejection", contradicted)
	}
	failed := results[4]
	if failed.OrderID != 0 || !errors.Is(failed.Err, ErrOutcomeUnknown) || !errors.As(failed.Err, &apiErr) || apiErr.Code != CodeSystemError {
		t.Errorf("order failing inside Coinstore: result %+v, want ErrOutcomeUnknown wrapping the system error", failed)
	}
}

func TestPlaceOrdersReportsBatchFailures(t *testing.T) {
	orders := []OrderRequest{
		{Side: SideBuy, Type: OrderTypeLimit, Price: d("1"), Quantity: d("1")},
		{ClientOrderID: "mine", Side: SideSell, Type: OrderTypeLimit, Price: d("2"), Quantity: d("1")},
	}

	t.Run("outcome unknown", func(t *testing.T) {
		bodies := make(chan capturedRequest, 1)
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			bodies <- capturedRequest{Body: string(body)}
			<-release
		}))
		t.Cleanup(server.Close)
		t.Cleanup(func() { close(release) })
		client, err := NewClient(
			WithBaseURL(server.URL),
			WithCredentials(testAPIKey, testSecretKey),
			WithHTTPClient(&http.Client{Timeout: 100 * time.Millisecond}),
		)
		if err != nil {
			t.Fatalf("NewClient returned error: %v", err)
		}

		results, err := client.PlaceOrders(context.Background(), "BTCUSDT", orders)

		if !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("PlaceOrders error = %v, want ErrOutcomeUnknown", err)
		}
		sent := batchOrders(t, receive(t, bodies))
		if len(results) != len(orders) || len(sent) != len(orders) {
			t.Fatalf("PlaceOrders returned %d results for %d sent orders, want %d", len(results), len(sent), len(orders))
		}
		for i, result := range results {
			if result.ClientOrderID != sent[i] || result.OrderID != 0 || result.Err != err {
				t.Errorf("result %d = %+v, want client order ID %q and the batch's error", i, result, sent[i])
			}
		}
		if sent[1] != "mine" {
			t.Errorf("sent client order ID %q, want the given one", sent[1])
		}
	})

	t.Run("rejected", func(t *testing.T) {
		client, _ := serveOnce(t, http.StatusOK, `{"code":3013,"message":"trade-limit","data":null}`)

		results, err := client.PlaceOrders(context.Background(), "BTCUSDT", orders)

		var apiErr *APIError
		if !errors.As(err, &apiErr) || errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("PlaceOrders error = %v, want an APIError without ErrOutcomeUnknown", err)
		}
		if len(results) != len(orders) || results[1].ClientOrderID != "mine" || results[0].Err != err || results[1].Err != err {
			t.Errorf("PlaceOrders returned %+v, want a result carrying the batch's error for every order", results)
		}
	})
}

func TestPlaceOrdersGeneratesDistinctClientOrderIDs(t *testing.T) {
	api := newFakeAPI(t, http.StatusOK, `{"code":0,"data":[]}`)
	client := newTestClient(t, api)
	order := OrderRequest{Side: SideBuy, Type: OrderTypeMarket, QuoteAmount: d("10")}

	results, err := client.PlaceOrders(context.Background(), "BTCUSDT", []OrderRequest{order, order, order})
	if err != nil {
		t.Fatalf("PlaceOrders returned error: %v", err)
	}

	ids := batchOrders(t, api.onlyRequest(t))
	if len(ids) != 3 || ids[0] == ids[1] || ids[1] == ids[2] || ids[0] == ids[2] {
		t.Errorf("generated client order IDs %q, want three distinct IDs", ids)
	}
	for i, result := range results {
		if result.ClientOrderID != ids[i] {
			t.Errorf("result %d reports client order ID %q, want %q", i, result.ClientOrderID, ids[i])
		}
	}
}

func TestPlaceOrdersValidation(t *testing.T) {
	valid := OrderRequest{Side: SideBuy, Type: OrderTypeLimit, Price: d("1"), Quantity: d("1")}
	tests := []struct {
		name    string
		symbol  string
		orders  []OrderRequest
		wantMsg string
	}{
		{"missing symbol", "", []OrderRequest{valid}, "symbol is required"},
		{"empty batch", "BTCUSDT", nil, "at least one order"},
		{"invalid order", "BTCUSDT", []OrderRequest{valid, {Side: SideBuy, Type: OrderTypeLimit}}, "(order 1 of the batch)"},
		{"repeated client order ID", "BTCUSDT", []OrderRequest{
			{ClientOrderID: "a", Side: SideBuy, Type: OrderTypeLimit, Price: d("1"), Quantity: d("1")},
			{ClientOrderID: "a", Side: SideBuy, Type: OrderTypeLimit, Price: d("1"), Quantity: d("2")},
		}, `client order ID "a" appears twice`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, `{"code":0}`)

			_, err := client.PlaceOrders(context.Background(), tt.symbol, tt.orders)

			if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("PlaceOrders error = %v, want ErrInvalidRequest mentioning %q", err, tt.wantMsg)
			}
			if n := api.requestCount(); n != 0 {
				t.Errorf("server received %d requests, want none", n)
			}
		})
	}
}

func TestCancelOrder(t *testing.T) {

	const response = `{"code":0,"data":{"clientOrderId":"b1b2ea5e00a84b888d419cb73f8eb203","state":"CANCELED",` +
		`"ordId":1722183748419690}}`
	client, api := serveOnce(t, http.StatusOK, response)

	result, err := client.CancelOrder(context.Background(), "LUFFYUSDT", 1722183748419690)
	if err != nil {
		t.Fatalf("CancelOrder returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodPost, "/api/trade/order/cancel", "",
		`{"symbol":"LUFFYUSDT","ordId":1722183748419690,"orderId":1722183748419690}`)
	assertSigned(t, req)
	assertEqual(t, result, CancelResult{
		OrderID:       1722183748419690,
		ClientOrderID: "b1b2ea5e00a84b888d419cb73f8eb203",
		Status:        OrderStatusCanceled,
	})
}

func TestCancelOrders(t *testing.T) {

	const response = `{"data":{"success":[1722364585836586,1722364585836585],"reject":["1722364585836584"]},"code":0}`
	client, api := serveOnce(t, http.StatusOK, response)

	result, err := client.CancelOrders(context.Background(), "LUFFYUSDT", []int64{1722364585836586, 1722364585836585, 1722364585836584})
	if err != nil {
		t.Fatalf("CancelOrders returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodPost, "/api/trade/order/cancelBatch", "",
		`{"symbol":"LUFFYUSDT","orderIds":[1722364585836586,1722364585836585,1722364585836584],`+
			`"orderId":[1722364585836586,1722364585836585,1722364585836584]}`)
	assertSigned(t, req)
	assertEqual(t, result, CancelOrdersResult{
		Canceled:    []int64{1722364585836586, 1722364585836585},
		Rejected:    []int64{1722364585836584},
		Unconfirmed: []int64{},
	})
}

func TestCancelOrdersReportsUnconfirmedOrders(t *testing.T) {
	tests := []struct {
		name     string
		response string
		want     CancelOrdersResult
	}{
		{
			name:     "orders missing from both lists",
			response: `{"code":0,"data":{"success":[1],"reject":[]}}`,
			want:     CancelOrdersResult{Canceled: []int64{1}, Rejected: []int64{}, Unconfirmed: []int64{2, 3}},
		},
		{
			name:     "no lists",
			response: `{"code":0,"data":null}`,
			want:     CancelOrdersResult{Canceled: []int64{}, Rejected: []int64{}, Unconfirmed: []int64{1, 2, 3}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := serveOnce(t, http.StatusOK, tt.response)

			result, err := client.CancelOrders(context.Background(), "BTCUSDT", []int64{1, 2, 3, 2})
			if err != nil {
				t.Fatalf("CancelOrders returned error: %v", err)
			}
			assertEqual(t, result, tt.want)
		})
	}
}

func TestCancelOrdersByClientIDReportsUnconfirmedOrders(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":{"success":["a"],"reject":["c"]}}`)

	result, err := client.CancelOrdersByClientID(context.Background(), "BTCUSDT", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("CancelOrdersByClientID returned error: %v", err)
	}

	assertEqual(t, result, CancelOrdersByClientIDResult{
		Canceled:    []string{"a"},
		Rejected:    []string{"c"},
		Unconfirmed: []string{"b"},
	})
}

func TestCancelOrderWithoutResultReportsRequestedOrder(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":null}`)

	result, err := client.CancelOrder(context.Background(), "BTCUSDT", 42)
	if err != nil {
		t.Fatalf("CancelOrder returned error: %v", err)
	}

	assertEqual(t, result, CancelResult{OrderID: 42})
}

func TestCancelOrdersByClientID(t *testing.T) {

	const response = `{"data":{"success":["ccc666","ccc777","ccc888"]},"code":0}`
	client, api := serveOnce(t, http.StatusOK, response)

	result, err := client.CancelOrdersByClientID(context.Background(), "BTCUSDT", []string{"ccc666", "ccc777", "ccc888"})
	if err != nil {
		t.Fatalf("CancelOrdersByClientID returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodPost, "/api/trade/order/cancelBatchByClOrdId", "",
		`{"symbol":"BTCUSDT","clientOrderIds":["ccc666","ccc777","ccc888"]}`)
	assertSigned(t, req)
	assertEqual(t, result, CancelOrdersByClientIDResult{
		Canceled:    []string{"ccc666", "ccc777", "ccc888"},
		Rejected:    []string{},
		Unconfirmed: []string{},
	})
}

func TestOrderEndpointValidation(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)
	ctx := context.Background()
	calls := map[string]func() error{
		"order without ID": func() error { _, err := client.Order(ctx, 0); return err },
		"open orders with negative ID": func() error {
			_, err := client.OpenOrdersV2(ctx, OpenOrdersFilter{OrderID: -1})
			return err
		},
		"lookup without IDs": func() error { _, err := client.OrderV2(ctx, OrderLookup{}); return err },
		"lookup with both IDs": func() error {
			_, err := client.OrderV2(ctx, OrderLookup{OrderID: 1, ClientOrderID: "a"})
			return err
		},
		"lookup with negative ID": func() error { _, err := client.OrderV2(ctx, OrderLookup{OrderID: -1}); return err },
		"fills without symbol":    func() error { _, err := client.Fills(ctx, "", FillsFilter{}); return err },
		"fills with unknown side": func() error {
			_, err := client.Fills(ctx, "BTCUSDT", FillsFilter{Side: "LONG"})
			return err
		},
		"fills with negative page": func() error {
			_, err := client.Fills(ctx, "BTCUSDT", FillsFilter{Page: -1})
			return err
		},
		"fills with negative page size": func() error {
			_, err := client.Fills(ctx, "BTCUSDT", FillsFilter{PageSize: -1})
			return err
		},
		"cancel without symbol":   func() error { _, err := client.CancelOrder(ctx, "", 1); return err },
		"cancel without order ID": func() error { _, err := client.CancelOrder(ctx, "BTCUSDT", 0); return err },
		"batch cancel without IDs": func() error {
			_, err := client.CancelOrders(ctx, "BTCUSDT", nil)
			return err
		},
		"batch cancel with invalid ID": func() error {
			_, err := client.CancelOrders(ctx, "BTCUSDT", []int64{1, 0})
			return err
		},
		"batch cancel without symbol": func() error {
			_, err := client.CancelOrders(ctx, "", []int64{1})
			return err
		},
		"client ID cancel without IDs": func() error {
			_, err := client.CancelOrdersByClientID(ctx, "BTCUSDT", []string{})
			return err
		},
		"client ID cancel with empty ID": func() error {
			_, err := client.CancelOrdersByClientID(ctx, "BTCUSDT", []string{"a", ""})
			return err
		},
		"client ID cancel without symbol": func() error {
			_, err := client.CancelOrdersByClientID(ctx, "", []string{"a"})
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
	if n := api.requestCount(); n != 0 {
		t.Errorf("invalid requests reached the server %d times", n)
	}
}
