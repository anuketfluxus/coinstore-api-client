package coinstore

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestDepositAddress(t *testing.T) {

	const response = `{"code":"0","message":"Success","data":{"address":"TW4……","tag":"","depositMin":0.00010000}}`
	client, api := serveOnce(t, http.StatusOK, response)

	address, err := client.DepositAddress(context.Background(), "USDTTRX", "TRX")
	if err != nil {
		t.Fatalf("DepositAddress returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodPost, "/api/fi/v3/asset/deposit/do", "", `{"currencyCode":"USDTTRX","chain":"TRX"}`)
	assertSigned(t, req)
	assertEqual(t, address, DepositAddress{Address: "TW4……", MinDeposit: d("0.00010000")})
}

func TestDepositAddressRequiresAddressInResponse(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":{"address":"","tag":""}}`)

	if _, err := client.DepositAddress(context.Background(), "USDT", "TRX"); err == nil {
		t.Error("DepositAddress accepted a response without an address")
	}
}

func TestDepositAddressValidation(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)

	if _, err := client.DepositAddress(context.Background(), "", "TRX"); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("DepositAddress without currency: error = %v, want ErrInvalidRequest", err)
	}
	if _, err := client.DepositAddress(context.Background(), "USDT", ""); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("DepositAddress without chain: error = %v, want ErrInvalidRequest", err)
	}
	if n := api.requestCount(); n != 0 {
		t.Errorf("server received %d requests, want none", n)
	}
}

func TestDeposits(t *testing.T) {

	const response = `{"code":"0","message":"Success","data":{"depositList":[{"id":38948,"symbol":"doge",` +
		`"chainName":"doge","chainProtocol":"doge","amount":"10.61","createdAt":"2024-02-27 17:36:38","status":1,` +
		`"explorer":"19ceb820c143ceae503cf6ce5a0c9064721f438099529d041026e062e99a0bdc8",` +
		`"txId":"9ceb820c143ceae503cf6ce5a0c9064721f438099529d041026e062e99a0bdc8","internalType":0,` +
		`"lockAccount":false,"noDepositMsg":null}],"externalId":null,"label":null}}`
	client, api := serveOnce(t, http.StatusOK, response)
	singapore := time.FixedZone("UTC+8", 8*60*60)

	deposits, err := client.Deposits(context.Background(), HistoryQuery{
		Currency:   "USDT",
		Start:      time.Date(2024, 3, 1, 0, 0, 0, 0, singapore),
		End:        time.Date(2024, 3, 8, 23, 59, 59, 0, singapore),
		FromID:     5,
		Limit:      10,
		ExternalID: "ext-1",
		Label:      "label-1",
	})
	if err != nil {
		t.Fatalf("Deposits returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodPost, "/api/fi/v3/asset/deposit/record/list", "",
		`{"currencyCode":"USDT","startDate":"2024-03-01 00:00:00","endDate":"2024-03-08 23:59:59","fromId":5,`+
			`"limit":10,"externalId":"ext-1","label":"label-1"}`)
	assertSigned(t, req)
	assertEqual(t, deposits, []Deposit{{
		ID:            38948,
		Currency:      "doge",
		ChainName:     "doge",
		ChainProtocol: "doge",
		Amount:        d("10.61"),
		CreatedAt:     "2024-02-27 17:36:38",
		Status:        DepositCompleted,
		ExplorerURL:   "19ceb820c143ceae503cf6ce5a0c9064721f438099529d041026e062e99a0bdc8",
		TxID:          "9ceb820c143ceae503cf6ce5a0c9064721f438099529d041026e062e99a0bdc8",
		Internal:      false,
		Locked:        false,
	}})
}

func TestHistoryQueryWritesBothBoundsInOneLocation(t *testing.T) {
	singapore := time.FixedZone("UTC+8", 8*60*60)
	tests := []struct {
		name     string
		query    HistoryQuery
		wantBody string
	}{
		{
			name: "bounds in different locations",
			query: HistoryQuery{
				Currency: "USDT",
				Start:    time.Date(2024, 3, 1, 10, 0, 0, 0, singapore),
				End:      time.Date(2024, 3, 1, 5, 0, 0, 0, time.UTC),
			},
			wantBody: `{"currencyCode":"USDT","startDate":"2024-03-01 10:00:00","endDate":"2024-03-01 13:00:00"}`,
		},
		{
			name:     "end bound only",
			query:    HistoryQuery{Currency: "USDT", End: time.Date(2024, 3, 1, 5, 0, 0, 0, singapore)},
			wantBody: `{"currencyCode":"USDT","endDate":"2024-03-01 05:00:00"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, `{"code":0,"data":{"depositList":[]}}`)

			if _, err := client.Deposits(context.Background(), tt.query); err != nil {
				t.Fatalf("Deposits returned error: %v", err)
			}

			assertRequest(t, api.onlyRequest(t), http.MethodPost, "/api/fi/v3/asset/deposit/record/list", "", tt.wantBody)
		})
	}
}

func TestDepositsRequireCurrency(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)

	if _, err := client.Deposits(context.Background(), HistoryQuery{}); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("Deposits error = %v, want ErrInvalidRequest", err)
	}
	if n := api.requestCount(); n != 0 {
		t.Errorf("server received %d requests, want none", n)
	}
}

func TestWithdrawals(t *testing.T) {

	const response = `{"code":"0","message":"Success","data":{"withdrawList":[{"id":4886,"symbol":"DOGE",` +
		`"amount":"0.3","fee":"0","createdAt":"2024-02-27 18:20:31","status":5,"showStatus":2,` +
		`"explorer":"https://eth.btc.com/txinfo/",` +
		`"txId":"7b06acd90f839914f2676693927d994c264a0561893d3a5461a2a910d318acba",` +
		`"addressTo":"DFXm3XC49n3SBNqELip9LZHAo1nBaiDbdG","tag":null,"chainName":"DOGE","chainProtocol":"DOGE",` +
		`"internalType":0}],"externalId":null,"label":null}}`
	client, api := serveOnce(t, http.StatusOK, response)

	withdrawals, err := client.Withdrawals(context.Background(), HistoryQuery{Limit: 10})
	if err != nil {
		t.Fatalf("Withdrawals returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodPost, "/api/fi/v3/asset/withdraw/record/list", "", `{"limit":10}`)
	assertSigned(t, req)
	assertEqual(t, withdrawals, []Withdrawal{{
		ID:            4886,
		Currency:      "DOGE",
		Amount:        d("0.3"),
		Fee:           d("0"),
		CreatedAt:     "2024-02-27 18:20:31",
		Status:        WithdrawalCompleted,
		ExplorerURL:   "https://eth.btc.com/txinfo/",
		TxID:          "7b06acd90f839914f2676693927d994c264a0561893d3a5461a2a910d318acba",
		Address:       "DFXm3XC49n3SBNqELip9LZHAo1nBaiDbdG",
		ChainName:     "DOGE",
		ChainProtocol: "DOGE",
	}})
}

func TestHistoryQueryValidation(t *testing.T) {
	start := time.Date(2024, 3, 8, 0, 0, 0, 0, time.UTC)
	tests := map[string]HistoryQuery{
		"negative from ID":     {Currency: "USDT", FromID: -1},
		"negative limit":       {Currency: "USDT", Limit: -1},
		"end before the start": {Currency: "USDT", Start: start, End: start.Add(-time.Second)},
	}
	for name, query := range tests {
		t.Run(name, func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, `{"code":0}`)
			if _, err := client.Deposits(context.Background(), query); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("Deposits error = %v, want ErrInvalidRequest", err)
			}
			if _, err := client.Withdrawals(context.Background(), query); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("Withdrawals error = %v, want ErrInvalidRequest", err)
			}
			if n := api.requestCount(); n != 0 {
				t.Errorf("server received %d requests, want none", n)
			}
		})
	}
}

func TestWithdraw(t *testing.T) {
	tests := []struct {
		name     string
		tag      string
		wantBody string
	}{
		{
			name:     "without tag",
			wantBody: `{"currencyCode":"USDT","amount":"1000","address":"TU4vEruvZwLLkSfV9bNw12EJTPvNr7Pvaa","chainType":"trc20"}`,
		},
		{
			name:     "with tag",
			tag:      "698347",
			wantBody: `{"currencyCode":"USDT","amount":"1000","address":"TU4vEruvZwLLkSfV9bNw12EJTPvNr7Pvaa","tag":"698347","chainType":"trc20"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, `{"code":"0","message":"Success","data":{"id":4886}}`)

			id, err := client.Withdraw(context.Background(), WithdrawalRequest{
				Currency:      "USDT",
				Amount:        d("1000.00"),
				Address:       "TU4vEruvZwLLkSfV9bNw12EJTPvNr7Pvaa",
				Tag:           tt.tag,
				ChainProtocol: "trc20",
			})
			if err != nil {
				t.Fatalf("Withdraw returned error: %v", err)
			}

			req := api.onlyRequest(t)
			assertRequest(t, req, http.MethodPost, "/api/fi/v3/asset/doWithdraw", "", tt.wantBody)
			assertSigned(t, req)
			if id != 4886 {
				t.Errorf("Withdraw = %d, want 4886", id)
			}
		})
	}
}

func TestWithdrawRequiresIDInResponse(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0,"data":{}}`)

	_, err := client.Withdraw(context.Background(), WithdrawalRequest{
		Currency: "USDT", Amount: d("1"), Address: "addr", ChainProtocol: "trc20",
	})

	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Errorf("Withdraw error = %v, want ErrOutcomeUnknown", err)
	}
}

func TestWithdrawValidation(t *testing.T) {
	valid := WithdrawalRequest{Currency: "USDT", Amount: d("1"), Address: "addr", ChainProtocol: "trc20"}
	tests := map[string]func(*WithdrawalRequest){
		"missing currency":       func(r *WithdrawalRequest) { r.Currency = "" },
		"zero amount":            func(r *WithdrawalRequest) { r.Amount = decimal.Zero },
		"negative amount":        func(r *WithdrawalRequest) { r.Amount = d("-1") },
		"missing address":        func(r *WithdrawalRequest) { r.Address = " " },
		"missing chain protocol": func(r *WithdrawalRequest) { r.ChainProtocol = "" },
	}
	for name, modify := range tests {
		t.Run(name, func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, `{"code":0}`)
			req := valid
			modify(&req)

			if _, err := client.Withdraw(context.Background(), req); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("Withdraw error = %v, want ErrInvalidRequest", err)
			}
			if n := api.requestCount(); n != 0 {
				t.Errorf("server received %d requests, want none", n)
			}
		})
	}
}

func TestCancelWithdrawal(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":"0","message":"Success","data":{"id":4886}}`)

	if err := client.CancelWithdrawal(context.Background(), 4886); err != nil {
		t.Fatalf("CancelWithdrawal returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodPost, "/api/fi/v3/asset/cancelWithdraw", "", `{"withdrawId":4886}`)
	assertSigned(t, req)
}

func TestCancelWithdrawalRequiresID(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0}`)
	if err := client.CancelWithdrawal(context.Background(), 0); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("CancelWithdrawal error = %v, want ErrInvalidRequest", err)
	}
}

func TestTransfer(t *testing.T) {
	tests := []struct {
		name     string
		response string
	}{
		{"success code 0", `{"code":"0","message":"Success"}`},
		{"success code 200", `{"code":200,"message":"Transfer succeeded"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, tt.response)

			err := client.Transfer(context.Background(), TransferRequest{
				Type:     TransferWithinAccount,
				Currency: "USDT",
				Amount:   d("1"),
				From:     AccountSpot,
				To:       AccountFutures,
			})
			if err != nil {
				t.Fatalf("Transfer returned error: %v", err)
			}

			req := api.onlyRequest(t)
			assertRequest(t, req, http.MethodPost, "/futures/v1/future/transfer", "",
				`{"type":"0","currencyCode":"USDT","amount":"1","from":"1","to":"2"}`)
			assertSigned(t, req)
		})
	}
}

func TestTransferToSubAccount(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)

	err := client.Transfer(context.Background(), TransferRequest{
		Type:       TransferMainToSub,
		Currency:   "USDT",
		Amount:     d("2.5"),
		From:       AccountSpot,
		To:         AccountSpot,
		SubAccount: "trading-desk",
	})
	if err != nil {
		t.Fatalf("Transfer returned error: %v", err)
	}

	assertRequest(t, api.onlyRequest(t), http.MethodPost, "/futures/v1/future/transfer", "",
		`{"type":"1","currencyCode":"USDT","amount":"2.5","from":"1","to":"1","subAccount":"trading-desk"}`)
}

func TestTransferReportsFailure(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":102150808,"msg":"insufficient balance","data":null}`)

	err := client.Transfer(context.Background(), TransferRequest{
		Currency: "USDT", Amount: d("1"), From: AccountSpot, To: AccountFutures,
	})

	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != CodeTransferRejected {
		t.Errorf("Transfer error = %v, want an APIError with code %d", err, CodeTransferRejected)
	}
}

func TestTransferValidation(t *testing.T) {
	valid := TransferRequest{Currency: "USDT", Amount: d("1"), From: AccountSpot, To: AccountFutures}
	tests := map[string]func(*TransferRequest){
		"missing currency":                func(r *TransferRequest) { r.Currency = "" },
		"zero amount":                     func(r *TransferRequest) { r.Amount = decimal.Zero },
		"unknown source account":          func(r *TransferRequest) { r.From = 0 },
		"unknown destination account":     func(r *TransferRequest) { r.To = 3 },
		"same accounts within an account": func(r *TransferRequest) { r.To = AccountSpot },
		"sub-account transfer without sub-account": func(r *TransferRequest) {
			r.Type = TransferSubToMain
		},
		"unknown transfer type": func(r *TransferRequest) { r.Type = 3 },
	}
	for name, modify := range tests {
		t.Run(name, func(t *testing.T) {
			client, api := serveOnce(t, http.StatusOK, `{"code":0}`)
			req := valid
			modify(&req)

			if err := client.Transfer(context.Background(), req); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("Transfer error = %v, want ErrInvalidRequest", err)
			}
			if n := api.requestCount(); n != 0 {
				t.Errorf("server received %d requests, want none", n)
			}
		})
	}
}

func TestFundingStatusStrings(t *testing.T) {
	depositNames := map[DepositStatus]string{
		DepositPending:     "Pending",
		DepositCompleted:   "Completed",
		DepositAbnormal:    "Abnormal",
		DepositUnderReview: "UnderReview",
		DepositRejected:    "Rejected",
		9:                  "DepositStatus(9)",
	}
	for status, want := range depositNames {
		if got := status.String(); got != want {
			t.Errorf("DepositStatus(%d).String() = %q, want %q", int(status), got, want)
		}
	}
	withdrawalNames := map[WithdrawalStatus]string{
		WithdrawalPendingReview: "PendingReview",
		WithdrawalApproved:      "Approved",
		WithdrawalRejected:      "Rejected",
		WithdrawalProcessing:    "Processing",
		WithdrawalFailed:        "Failed",
		WithdrawalCompleted:     "Completed",
		WithdrawalCanceled:      "Canceled",
		9:                       "WithdrawalStatus(9)",
	}
	for status, want := range withdrawalNames {
		if got := status.String(); got != want {
			t.Errorf("WithdrawalStatus(%d).String() = %q, want %q", int(status), got, want)
		}
	}
}
