package coinstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/anuketfluxus/coinstore-api-client/internal/flexjson"
	"github.com/shopspring/decimal"
)

const historyDateLayout = "2006-01-02 15:04:05"

const transferSucceededCode = 200

type DepositAddress struct {
	Address string

	Tag        string
	MinDeposit decimal.Decimal
}

type HistoryQuery struct {
	Currency string

	Start time.Time
	End   time.Time

	FromID int64

	Limit      int
	ExternalID string
	Label      string
}

type DepositStatus int

const (
	DepositPending     DepositStatus = 0
	DepositCompleted   DepositStatus = 1
	DepositAbnormal    DepositStatus = 2
	DepositUnderReview DepositStatus = 3
	DepositRejected    DepositStatus = 4
)

func (s DepositStatus) String() string {
	switch s {
	case DepositPending:
		return "Pending"
	case DepositCompleted:
		return "Completed"
	case DepositAbnormal:
		return "Abnormal"
	case DepositUnderReview:
		return "UnderReview"
	case DepositRejected:
		return "Rejected"
	default:
		return fmt.Sprintf("DepositStatus(%d)", int(s))
	}
}

type Deposit struct {
	ID            int64
	Currency      string
	ChainName     string
	ChainProtocol string
	Amount        decimal.Decimal

	CreatedAt   string
	Status      DepositStatus
	ExplorerURL string
	TxID        string

	Internal bool

	Locked bool
}

type WithdrawalStatus int

const (
	WithdrawalPendingReview WithdrawalStatus = 0
	WithdrawalApproved      WithdrawalStatus = 1
	WithdrawalRejected      WithdrawalStatus = 2
	WithdrawalProcessing    WithdrawalStatus = 3
	WithdrawalFailed        WithdrawalStatus = 4
	WithdrawalCompleted     WithdrawalStatus = 5
	WithdrawalCanceled      WithdrawalStatus = 6
)

func (s WithdrawalStatus) String() string {
	switch s {
	case WithdrawalPendingReview:
		return "PendingReview"
	case WithdrawalApproved:
		return "Approved"
	case WithdrawalRejected:
		return "Rejected"
	case WithdrawalProcessing:
		return "Processing"
	case WithdrawalFailed:
		return "Failed"
	case WithdrawalCompleted:
		return "Completed"
	case WithdrawalCanceled:
		return "Canceled"
	default:
		return fmt.Sprintf("WithdrawalStatus(%d)", int(s))
	}
}

type Withdrawal struct {
	ID       int64
	Currency string
	Amount   decimal.Decimal
	Fee      decimal.Decimal

	CreatedAt     string
	Status        WithdrawalStatus
	ExplorerURL   string
	TxID          string
	Address       string
	Tag           string
	ChainName     string
	ChainProtocol string

	Internal bool
}

type WithdrawalRequest struct {
	Currency string
	Amount   decimal.Decimal

	Address string

	Tag string

	ChainProtocol string
}

type TransferType int

const (
	TransferWithinAccount TransferType = 0

	TransferMainToSub TransferType = 1

	TransferSubToMain TransferType = 2
)

type AccountType int

const (
	AccountSpot    AccountType = 1
	AccountFutures AccountType = 2
)

type TransferRequest struct {
	Type     TransferType
	Currency string
	Amount   decimal.Decimal
	From     AccountType
	To       AccountType

	SubAccount string
}

func (c *Client) DepositAddress(ctx context.Context, currency, chain string) (DepositAddress, error) {
	if err := validateRequired("currency", currency); err != nil {
		return DepositAddress{}, err
	}
	if err := validateRequired("chain", chain); err != nil {
		return DepositAddress{}, err
	}
	body := depositAddressBody{CurrencyCode: currency, Chain: chain}

	const path = "/fi/v3/asset/deposit/do"
	var address depositAddressJSON
	if err := c.do(ctx, c.post(path, body, signed), &address); err != nil {
		return DepositAddress{}, err
	}
	if address.Address == "" {
		return DepositAddress{}, fmt.Errorf("coinstore: POST %s: response carries no address", path)
	}
	return DepositAddress{
		Address:    address.Address,
		Tag:        address.Tag,
		MinDeposit: dec(address.DepositMin),
	}, nil
}

func (c *Client) Deposits(ctx context.Context, query HistoryQuery) ([]Deposit, error) {
	if err := validateRequired("currency", query.Currency); err != nil {
		return nil, err
	}
	if err := query.validate(); err != nil {
		return nil, err
	}

	var history struct {
		DepositList []depositJSON `json:"depositList"`
	}
	if err := c.do(ctx, c.post("/fi/v3/asset/deposit/record/list", query.body(), signed), &history); err != nil {
		return nil, err
	}
	return convertAll(history.DepositList, depositJSON.toDeposit), nil
}

func (c *Client) Withdrawals(ctx context.Context, query HistoryQuery) ([]Withdrawal, error) {
	if err := query.validate(); err != nil {
		return nil, err
	}

	var history struct {
		WithdrawList []withdrawalJSON `json:"withdrawList"`
	}
	if err := c.do(ctx, c.post("/fi/v3/asset/withdraw/record/list", query.body(), signed), &history); err != nil {
		return nil, err
	}
	return convertAll(history.WithdrawList, withdrawalJSON.toWithdrawal), nil
}

func (c *Client) Withdraw(ctx context.Context, req WithdrawalRequest) (int64, error) {
	if err := req.validate(); err != nil {
		return 0, err
	}
	body := withdrawBody{
		CurrencyCode: req.Currency,
		Amount:       flexjson.Decimal(req.Amount),
		Address:      req.Address,
		Tag:          req.Tag,
		ChainType:    req.ChainProtocol,
	}

	var result withdrawalIDJSON
	if err := c.do(ctx, c.change("/fi/v3/asset/doWithdraw", body), &result); err != nil {
		return 0, err
	}
	if result.ID == 0 {
		return 0, outcomeUnknown(errors.New("coinstore: POST /fi/v3/asset/doWithdraw: the API reported success without a withdrawal ID"))
	}
	return int64(result.ID), nil
}

func (c *Client) CancelWithdrawal(ctx context.Context, withdrawalID int64) error {
	if err := validateID("withdrawal ID", withdrawalID); err != nil {
		return err
	}
	body := cancelWithdrawalBody{WithdrawID: withdrawalID}
	return c.do(ctx, c.change("/fi/v3/asset/cancelWithdraw", body), nil)
}

func (c *Client) Transfer(ctx context.Context, req TransferRequest) error {
	if err := req.validate(); err != nil {
		return err
	}
	body := transferBody{
		Type:         strconv.Itoa(int(req.Type)),
		CurrencyCode: req.Currency,
		Amount:       flexjson.Decimal(req.Amount),
		From:         strconv.Itoa(int(req.From)),
		To:           strconv.Itoa(int(req.To)),
		SubAccount:   req.SubAccount,
	}
	r := apiRequest{
		method:         http.MethodPost,
		baseURL:        c.futuresURL,
		path:           "/v1/future/transfer",
		body:           body,
		auth:           signed,
		changesAccount: true,
	}

	err := c.do(ctx, r, nil)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.HTTPStatus == http.StatusOK && apiErr.Code == transferSucceededCode {
		return nil
	}
	return err
}

func (q HistoryQuery) validate() error {
	if err := validateNotNegative("from ID", q.FromID); err != nil {
		return err
	}
	if err := validateNotNegative("limit", q.Limit); err != nil {
		return err
	}
	if !q.Start.IsZero() && !q.End.IsZero() && q.End.Before(q.Start) {
		return invalidRequest("end time %s is before start time %s", q.End, q.Start)
	}
	return nil
}

func (q HistoryQuery) body() historyBody {
	location := q.Start.Location()
	if q.Start.IsZero() {
		location = q.End.Location()
	}
	return historyBody{
		CurrencyCode: q.Currency,
		StartDate:    formatHistoryDate(q.Start, location),
		EndDate:      formatHistoryDate(q.End, location),
		FromID:       q.FromID,
		Limit:        q.Limit,
		ExternalID:   q.ExternalID,
		Label:        q.Label,
	}
}

func formatHistoryDate(t time.Time, location *time.Location) string {
	if t.IsZero() {
		return ""
	}
	return t.In(location).Format(historyDateLayout)
}

func (r WithdrawalRequest) validate() error {
	if err := validateRequired("currency", r.Currency); err != nil {
		return err
	}
	if err := validatePositiveAmount("amount", r.Amount); err != nil {
		return err
	}
	if err := validateRequired("address", r.Address); err != nil {
		return err
	}
	return validateRequired("chain protocol", r.ChainProtocol)
}

func (r TransferRequest) validate() error {
	if err := validateRequired("currency", r.Currency); err != nil {
		return err
	}
	if err := validatePositiveAmount("amount", r.Amount); err != nil {
		return err
	}
	for _, account := range []AccountType{r.From, r.To} {
		if account != AccountSpot && account != AccountFutures {
			return invalidRequest("account type %d is neither AccountSpot nor AccountFutures", account)
		}
	}
	switch r.Type {
	case TransferWithinAccount:
		if r.From == r.To {
			return invalidRequest("a transfer within an account needs different source and destination accounts")
		}
	case TransferMainToSub, TransferSubToMain:
		if err := validateRequired("sub-account", r.SubAccount); err != nil {
			return err
		}
	default:
		return invalidRequest("unknown transfer type %d", r.Type)
	}
	return nil
}

type depositAddressBody struct {
	CurrencyCode string `json:"currencyCode"`
	Chain        string `json:"chain"`
}

type depositAddressJSON struct {
	Address    string           `json:"address"`
	Tag        string           `json:"tag"`
	DepositMin flexjson.Decimal `json:"depositMin"`
}

type historyBody struct {
	CurrencyCode string `json:"currencyCode,omitempty"`
	StartDate    string `json:"startDate,omitempty"`
	EndDate      string `json:"endDate,omitempty"`
	FromID       int64  `json:"fromId,omitempty"`
	Limit        int    `json:"limit,omitempty"`
	ExternalID   string `json:"externalId,omitempty"`
	Label        string `json:"label,omitempty"`
}

type depositJSON struct {
	ID            flexjson.Int64   `json:"id"`
	Symbol        string           `json:"symbol"`
	ChainName     string           `json:"chainName"`
	ChainProtocol string           `json:"chainProtocol"`
	Amount        flexjson.Decimal `json:"amount"`
	CreatedAt     string           `json:"createdAt"`
	Status        flexjson.Int64   `json:"status"`
	Explorer      string           `json:"explorer"`
	TxID          string           `json:"txId"`
	InternalType  flexjson.Bool    `json:"internalType"`
	LockAccount   flexjson.Bool    `json:"lockAccount"`
}

func (j depositJSON) toDeposit() Deposit {
	return Deposit{
		ID:            int64(j.ID),
		Currency:      j.Symbol,
		ChainName:     j.ChainName,
		ChainProtocol: j.ChainProtocol,
		Amount:        dec(j.Amount),
		CreatedAt:     j.CreatedAt,
		Status:        DepositStatus(j.Status),
		ExplorerURL:   j.Explorer,
		TxID:          j.TxID,
		Internal:      bool(j.InternalType),
		Locked:        bool(j.LockAccount),
	}
}

type withdrawalJSON struct {
	ID            flexjson.Int64   `json:"id"`
	Symbol        string           `json:"symbol"`
	Amount        flexjson.Decimal `json:"amount"`
	Fee           flexjson.Decimal `json:"fee"`
	CreatedAt     string           `json:"createdAt"`
	Status        flexjson.Int64   `json:"status"`
	Explorer      string           `json:"explorer"`
	TxID          string           `json:"txId"`
	AddressTo     string           `json:"addressTo"`
	Tag           string           `json:"tag"`
	ChainName     string           `json:"chainName"`
	ChainProtocol string           `json:"chainProtocol"`
	InternalType  flexjson.Bool    `json:"internalType"`
}

func (j withdrawalJSON) toWithdrawal() Withdrawal {
	return Withdrawal{
		ID:            int64(j.ID),
		Currency:      j.Symbol,
		Amount:        dec(j.Amount),
		Fee:           dec(j.Fee),
		CreatedAt:     j.CreatedAt,
		Status:        WithdrawalStatus(j.Status),
		ExplorerURL:   j.Explorer,
		TxID:          j.TxID,
		Address:       j.AddressTo,
		Tag:           j.Tag,
		ChainName:     j.ChainName,
		ChainProtocol: j.ChainProtocol,
		Internal:      bool(j.InternalType),
	}
}

type withdrawBody struct {
	CurrencyCode string           `json:"currencyCode"`
	Amount       flexjson.Decimal `json:"amount"`
	Address      string           `json:"address"`
	Tag          string           `json:"tag,omitempty"`
	ChainType    string           `json:"chainType"`
}

type withdrawalIDJSON struct {
	ID flexjson.Int64 `json:"id"`
}

type cancelWithdrawalBody struct {
	WithdrawID int64 `json:"withdrawId"`
}

type transferBody struct {
	Type         string           `json:"type"`
	CurrencyCode string           `json:"currencyCode"`
	Amount       flexjson.Decimal `json:"amount"`
	From         string           `json:"from"`
	To           string           `json:"to"`
	SubAccount   string           `json:"subAccount,omitempty"`
}
