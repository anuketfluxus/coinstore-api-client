package coinstore

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

type endpointCall struct {
	name           string
	changesAccount bool
	call           func(*Client) error
}

func endpointCalls() []endpointCall {
	ctx := context.Background()
	limitOrder := OrderRequest{Side: SideBuy, Type: OrderTypeLimit, Price: d("1"), Quantity: d("1")}
	return []endpointCall{
		{"Tickers", false, func(c *Client) error { _, err := c.Tickers(ctx); return err }},
		{"OrderBook", false, func(c *Client) error { _, err := c.OrderBook(ctx, "BTCUSDT", 5); return err }},
		{"Klines", false, func(c *Client) error { _, err := c.Klines(ctx, "BTCUSDT", KlineInterval1Hour, 10); return err }},
		{"RecentTrades", false, func(c *Client) error { _, err := c.RecentTrades(ctx, "BTCUSDT", 10); return err }},
		{"LatestPrices", false, func(c *Client) error { _, err := c.LatestPrices(ctx); return err }},
		{"Symbols", false, func(c *Client) error { _, err := c.Symbols(ctx, SymbolFilter{}); return err }},
		{"Currency", false, func(c *Client) error { _, err := c.Currency(ctx, "ETH"); return err }},
		{"Balances", false, func(c *Client) error { _, err := c.Balances(ctx); return err }},
		{"DepositAddress", false, func(c *Client) error { _, err := c.DepositAddress(ctx, "USDT", "TRX"); return err }},
		{"Deposits", false, func(c *Client) error { _, err := c.Deposits(ctx, HistoryQuery{Currency: "USDT"}); return err }},
		{"Withdrawals", false, func(c *Client) error { _, err := c.Withdrawals(ctx, HistoryQuery{}); return err }},
		{"Withdraw", true, func(c *Client) error {
			_, err := c.Withdraw(ctx, WithdrawalRequest{Currency: "USDT", Amount: d("1"), Address: "a", ChainProtocol: "trc20"})
			return err
		}},
		{"CancelWithdrawal", true, func(c *Client) error { return c.CancelWithdrawal(ctx, 1) }},
		{"Transfer", true, func(c *Client) error {
			return c.Transfer(ctx, TransferRequest{Currency: "USDT", Amount: d("1"), From: AccountSpot, To: AccountFutures})
		}},
		{"OpenOrders", false, func(c *Client) error { _, err := c.OpenOrders(ctx); return err }},
		{"OpenOrdersV2", false, func(c *Client) error { _, err := c.OpenOrdersV2(ctx, OpenOrdersFilter{}); return err }},
		{"Order", false, func(c *Client) error { _, err := c.Order(ctx, 1); return err }},
		{"OrderV2", false, func(c *Client) error { _, err := c.OrderV2(ctx, OrderLookup{OrderID: 1}); return err }},
		{"Fills", false, func(c *Client) error { _, err := c.Fills(ctx, "BTCUSDT", FillsFilter{}); return err }},
		{"PlaceOrder", true, func(c *Client) error { _, err := c.PlaceOrder(ctx, "BTCUSDT", limitOrder); return err }},
		{"PlaceOrders", true, func(c *Client) error {
			_, err := c.PlaceOrders(ctx, "BTCUSDT", []OrderRequest{limitOrder})
			return err
		}},
		{"CancelOrder", true, func(c *Client) error { _, err := c.CancelOrder(ctx, "BTCUSDT", 1); return err }},
		{"CancelOrders", true, func(c *Client) error { _, err := c.CancelOrders(ctx, "BTCUSDT", []int64{1}); return err }},
		{"CancelOrdersByClientID", true, func(c *Client) error {
			_, err := c.CancelOrdersByClientID(ctx, "BTCUSDT", []string{"a"})
			return err
		}},
	}
}

func TestEndpointsPropagateAPIErrors(t *testing.T) {
	for _, endpoint := range endpointCalls() {
		t.Run(endpoint.name, func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, `{"code":3013,"message":"trade-limit","data":null}`)

			err := endpoint.call(client)

			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Code != CodeTradingRestricted {
				t.Errorf("error = %v, want an APIError with code %d", err, CodeTradingRestricted)
			}
			if errors.Is(err, ErrOutcomeUnknown) {
				t.Errorf("error = %v, want a definite rejection", err)
			}
			if n := api.requestCount(); n != 1 {
				t.Errorf("server received %d requests, want 1", n)
			}
		})
	}
}

func TestEndpointsReportUnknownOutcomes(t *testing.T) {
	for _, endpoint := range endpointCalls() {
		t.Run(endpoint.name, func(t *testing.T) {
			client, _ := serveOnce(t, http.StatusGatewayTimeout, "<html>Gateway Timeout</html>")

			err := endpoint.call(client)

			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusGatewayTimeout {
				t.Errorf("error = %v, want an APIError with HTTP status 504", err)
			}
			if got := errors.Is(err, ErrOutcomeUnknown); got != endpoint.changesAccount {
				t.Errorf("errors.Is(%v, ErrOutcomeUnknown) = %t, want %t", err, got, endpoint.changesAccount)
			}
		})
	}
}
