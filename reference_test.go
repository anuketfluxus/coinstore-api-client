package coinstore

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestSymbols(t *testing.T) {

	const response = `{"code":"0","message":"Success","data":[{"symbolId":1,"symbolCode":"BTCUSDT",` +
		`"tradeCurrencyCode":"btc","quoteCurrencyCode":"usdt","openTrade":true,"onLineTime":1609813531019,` +
		`"tickSz":0,"lotSz":4,"minLmtPr":"0.0002","minLmtSz":"0.2","minMktVa":"0.1","minMktSz":"0.1",` +
		`"makerFee":"0.006","takerFee":"0.003"}]}`
	client, api := serveOnce(t, http.StatusOK, response)

	symbols, err := client.Symbols(context.Background(), SymbolFilter{Symbols: []string{"BTCUSDT"}, IDs: []int64{10}})
	if err != nil {
		t.Fatalf("Symbols returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodPost, "/api/v2/public/config/spot/symbols", "", `{"symbolCodes":["BTCUSDT"],"symbolIds":[10]}`)
	assertUnsigned(t, req)
	assertEqual(t, symbols, []SymbolInfo{{
		ID:                1,
		Symbol:            "BTCUSDT",
		BaseCurrency:      "btc",
		QuoteCurrency:     "usdt",
		TradingEnabled:    true,
		ListedAt:          time.UnixMilli(1609813531019).UTC(),
		TickSize:          0,
		LotSize:           4,
		MinLimitPrice:     d("0.0002"),
		MinLimitQuantity:  d("0.2"),
		MinMarketValue:    d("0.1"),
		MinMarketQuantity: d("0.1"),
		MakerFee:          d("0.006"),
		TakerFee:          d("0.003"),
	}})
}

func TestSymbolsWithoutFilterSendsEmptyObject(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0,"data":[]}`)

	symbols, err := client.Symbols(context.Background(), SymbolFilter{})
	if err != nil {
		t.Fatalf("Symbols returned error: %v", err)
	}

	assertRequest(t, api.onlyRequest(t), http.MethodPost, "/api/v2/public/config/spot/symbols", "", `{}`)
	if symbols == nil || len(symbols) != 0 {
		t.Errorf("Symbols = %#v, want an empty slice", symbols)
	}
}

func TestCurrency(t *testing.T) {

	const response = `{"code":"0","message":"Success","data":{"name":"eth","code":"eth","showPrecision":6,` +
		`"chainDataList":[{"currencyCode":"eth","chainName":"eth","contractAddress":"23412374127384ghgdwq",` +
		`"depositOpen":0,"withdrawOpen":1,"withdrawFee":"100.00000000","withdrawMin":"1.00000000",` +
		`"withdrawMax":"0.00000000","depositMin":"1.00000000","withdrawFeePlatform":"0.00000000",` +
		`"burningState":0,"withdrawFeeOnChain":"0.00000000","tagType":0,"withdrawFeeMin":"0.00000000"}]}}`
	client, api := serveOnce(t, http.StatusOK, response)

	currency, err := client.Currency(context.Background(), "ETH")
	if err != nil {
		t.Fatalf("Currency returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/fi/v1/common/currency", "currencyCode=ETH", "")
	assertSigned(t, req)
	assertEqual(t, currency, Currency{
		Code:             "eth",
		Name:             "eth",
		DisplayPrecision: 6,
		Chains: []Chain{{
			Name:              "eth",
			ContractAddress:   "23412374127384ghgdwq",
			DepositEnabled:    false,
			WithdrawalEnabled: true,
			MinDeposit:        d("1.00000000"),
			MinWithdrawal:     d("1.00000000"),
			MaxWithdrawal:     d("0.00000000"),
			WithdrawalFee:     d("100.00000000"),
			MinWithdrawalFee:  d("0.00000000"),
			PlatformFeeRate:   d("0.00000000"),
			Burning:           false,
			OnChainBurnRate:   d("0.00000000"),
			Memo:              MemoNotRequired,
		}},
	})
}

func TestCurrencyRequiresCode(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)

	if _, err := client.Currency(context.Background(), ""); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("Currency error = %v, want ErrInvalidRequest", err)
	}
	if n := api.requestCount(); n != 0 {
		t.Errorf("server received %d requests, want none", n)
	}
}

func TestMemoRequirementString(t *testing.T) {
	tests := map[MemoRequirement]string{
		MemoNotRequired: "NotRequired",
		MemoRequired:    "Required",
		MemoOptional:    "Optional",
		7:               "MemoRequirement(7)",
	}
	for memo, want := range tests {
		if got := memo.String(); got != want {
			t.Errorf("MemoRequirement(%d).String() = %q, want %q", int(memo), got, want)
		}
	}
}
