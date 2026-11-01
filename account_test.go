package coinstore

import (
	"context"
	"net/http"
	"testing"
)

func TestBalances(t *testing.T) {

	const response = `{"data":[{"uid":315,"accountId":1134971,"currency":"USDT","balance":"0.05001","type":4,` +
		`"typeName":"FROZEN"},{"uid":315,"accountId":1134971,"currency":"USDT","balance":"13297.94999","type":1,` +
		`"typeName":"AVAILABLE"}],"code":0}`
	client, api := serveOnce(t, http.StatusOK, response)

	balances, err := client.Balances(context.Background())
	if err != nil {
		t.Fatalf("Balances returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodPost, "/api/spot/accountList", "", `{}`)
	assertSigned(t, req)
	assertEqual(t, balances, []Balance{
		{UserID: 315, AccountID: 1134971, Currency: "USDT", Amount: d("0.05001"), Type: BalanceFrozen},
		{UserID: 315, AccountID: 1134971, Currency: "USDT", Amount: d("13297.94999"), Type: BalanceAvailable},
	})
}

func TestBalancesAcceptsDocumentedFieldTableKeys(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":[{"userId":"42","accountId":7,"currencyId":30,"balance":1.5,"type":"1"}]}`)

	balances, err := client.Balances(context.Background())
	if err != nil {
		t.Fatalf("Balances returned error: %v", err)
	}

	assertEqual(t, balances, []Balance{{UserID: 42, AccountID: 7, CurrencyID: 30, Amount: d("1.5"), Type: BalanceAvailable}})
}

func TestBalanceTypeString(t *testing.T) {
	tests := map[BalanceType]string{
		BalanceAvailable: "Available",
		BalanceFrozen:    "Frozen",
		2:                "BalanceType(2)",
	}
	for balanceType, want := range tests {
		if got := balanceType.String(); got != want {
			t.Errorf("BalanceType(%d).String() = %q, want %q", int(balanceType), got, want)
		}
	}
}
