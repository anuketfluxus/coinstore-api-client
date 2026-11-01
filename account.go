package coinstore

import (
	"context"
	"fmt"

	"github.com/anuketfluxus/coinstore-api-client/internal/flexjson"
	"github.com/shopspring/decimal"
)

type BalanceType int

const (
	BalanceAvailable BalanceType = 1
	BalanceFrozen    BalanceType = 4
)

func (t BalanceType) String() string {
	switch t {
	case BalanceAvailable:
		return "Available"
	case BalanceFrozen:
		return "Frozen"
	default:
		return fmt.Sprintf("BalanceType(%d)", int(t))
	}
}

type Balance struct {
	UserID    int64
	AccountID int64

	Currency   string
	CurrencyID int64
	Amount     decimal.Decimal
	Type       BalanceType
}

func (c *Client) Balances(ctx context.Context) ([]Balance, error) {
	var balances []balanceJSON
	if err := c.do(ctx, c.post("/spot/accountList", nil, signed), &balances); err != nil {
		return nil, err
	}
	return convertAll(balances, balanceJSON.toBalance), nil
}

type balanceJSON struct {
	UID        flexjson.Int64   `json:"uid"`
	UserID     flexjson.Int64   `json:"userId"`
	AccountID  flexjson.Int64   `json:"accountId"`
	Currency   string           `json:"currency"`
	CurrencyID flexjson.Int64   `json:"currencyId"`
	Balance    flexjson.Decimal `json:"balance"`
	Type       flexjson.Int64   `json:"type"`
}

func (j balanceJSON) toBalance() Balance {
	userID := j.UID
	if userID == 0 {
		userID = j.UserID
	}
	return Balance{
		UserID:     int64(userID),
		AccountID:  int64(j.AccountID),
		Currency:   j.Currency,
		CurrencyID: int64(j.CurrencyID),
		Amount:     dec(j.Balance),
		Type:       BalanceType(j.Type),
	}
}
