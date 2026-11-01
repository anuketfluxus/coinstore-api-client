package coinstore

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/anuketfluxus/coinstore-api-client/internal/flexjson"
	"github.com/shopspring/decimal"
)

type SymbolInfo struct {
	ID             int64
	Symbol         string
	BaseCurrency   string
	QuoteCurrency  string
	TradingEnabled bool
	ListedAt       time.Time

	TickSize int
	LotSize  int

	MinLimitPrice    decimal.Decimal
	MinLimitQuantity decimal.Decimal

	MinMarketValue    decimal.Decimal
	MinMarketQuantity decimal.Decimal
	MakerFee          decimal.Decimal
	TakerFee          decimal.Decimal
}

type SymbolFilter struct {
	Symbols []string
	IDs     []int64
}

type MemoRequirement int

const (
	MemoNotRequired MemoRequirement = 0
	MemoRequired    MemoRequirement = 1
	MemoOptional    MemoRequirement = 2
)

func (m MemoRequirement) String() string {
	switch m {
	case MemoNotRequired:
		return "NotRequired"
	case MemoRequired:
		return "Required"
	case MemoOptional:
		return "Optional"
	default:
		return fmt.Sprintf("MemoRequirement(%d)", int(m))
	}
}

type Currency struct {
	Code string
	Name string

	DisplayPrecision int
	Chains           []Chain
}

type Chain struct {
	Name              string
	Protocol          string
	ContractAddress   string
	DepositEnabled    bool
	WithdrawalEnabled bool

	MinDeposit    decimal.Decimal
	MinWithdrawal decimal.Decimal
	MaxWithdrawal decimal.Decimal
	WithdrawalFee decimal.Decimal

	MinWithdrawalFee decimal.Decimal

	PlatformFeeRate decimal.Decimal

	Burning         bool
	OnChainBurnRate decimal.Decimal
	Memo            MemoRequirement
}

func (c *Client) Symbols(ctx context.Context, filter SymbolFilter) ([]SymbolInfo, error) {
	body := symbolsBody{SymbolCodes: filter.Symbols, SymbolIDs: filter.IDs}
	var symbols []symbolInfoJSON
	if err := c.do(ctx, c.post("/v2/public/config/spot/symbols", body, public), &symbols); err != nil {
		return nil, err
	}
	return convertAll(symbols, symbolInfoJSON.toSymbolInfo), nil
}

func (c *Client) Currency(ctx context.Context, code string) (Currency, error) {
	if err := validateRequired("currency code", code); err != nil {
		return Currency{}, err
	}
	query := url.Values{"currencyCode": {code}}
	var currency currencyJSON
	if err := c.do(ctx, c.get("/fi/v1/common/currency", query, signed), &currency); err != nil {
		return Currency{}, err
	}
	return currency.toCurrency(), nil
}

type symbolsBody struct {
	SymbolCodes []string `json:"symbolCodes,omitempty"`
	SymbolIDs   []int64  `json:"symbolIds,omitempty"`
}

type symbolInfoJSON struct {
	SymbolID          flexjson.Int64   `json:"symbolId"`
	SymbolCode        string           `json:"symbolCode"`
	TradeCurrencyCode string           `json:"tradeCurrencyCode"`
	QuoteCurrencyCode string           `json:"quoteCurrencyCode"`
	OpenTrade         flexjson.Bool    `json:"openTrade"`
	OnLineTime        flexjson.Int64   `json:"onLineTime"`
	TickSz            flexjson.Int64   `json:"tickSz"`
	LotSz             flexjson.Int64   `json:"lotSz"`
	MinLmtPr          flexjson.Decimal `json:"minLmtPr"`
	MinLmtSz          flexjson.Decimal `json:"minLmtSz"`
	MinMktVa          flexjson.Decimal `json:"minMktVa"`
	MinMktSz          flexjson.Decimal `json:"minMktSz"`
	MakerFee          flexjson.Decimal `json:"makerFee"`
	TakerFee          flexjson.Decimal `json:"takerFee"`
}

func (j symbolInfoJSON) toSymbolInfo() SymbolInfo {
	return SymbolInfo{
		ID:                int64(j.SymbolID),
		Symbol:            j.SymbolCode,
		BaseCurrency:      j.TradeCurrencyCode,
		QuoteCurrency:     j.QuoteCurrencyCode,
		TradingEnabled:    bool(j.OpenTrade),
		ListedAt:          timeFromUnixMilli(j.OnLineTime),
		TickSize:          int(j.TickSz),
		LotSize:           int(j.LotSz),
		MinLimitPrice:     dec(j.MinLmtPr),
		MinLimitQuantity:  dec(j.MinLmtSz),
		MinMarketValue:    dec(j.MinMktVa),
		MinMarketQuantity: dec(j.MinMktSz),
		MakerFee:          dec(j.MakerFee),
		TakerFee:          dec(j.TakerFee),
	}
}

type currencyJSON struct {
	Name          string         `json:"name"`
	Code          string         `json:"code"`
	ShowPrecision flexjson.Int64 `json:"showPrecision"`
	ChainDataList []chainJSON    `json:"chainDataList"`
}

func (j currencyJSON) toCurrency() Currency {
	return Currency{
		Code:             j.Code,
		Name:             j.Name,
		DisplayPrecision: int(j.ShowPrecision),
		Chains:           convertAll(j.ChainDataList, chainJSON.toChain),
	}
}

type chainJSON struct {
	ChainName           string           `json:"chainName"`
	ChainProtocol       string           `json:"chainProtocol"`
	ContractAddress     string           `json:"contractAddress"`
	DepositOpen         flexjson.Bool    `json:"depositOpen"`
	WithdrawOpen        flexjson.Bool    `json:"withdrawOpen"`
	WithdrawFee         flexjson.Decimal `json:"withdrawFee"`
	WithdrawMin         flexjson.Decimal `json:"withdrawMin"`
	WithdrawMax         flexjson.Decimal `json:"withdrawMax"`
	DepositMin          flexjson.Decimal `json:"depositMin"`
	WithdrawFeePlatform flexjson.Decimal `json:"withdrawFeePlatform"`
	BurningState        flexjson.Bool    `json:"burningState"`
	WithdrawFeeOnChain  flexjson.Decimal `json:"withdrawFeeOnChain"`
	TagType             flexjson.Int64   `json:"tagType"`
	WithdrawFeeMin      flexjson.Decimal `json:"withdrawFeeMin"`
}

func (j chainJSON) toChain() Chain {
	return Chain{
		Name:              j.ChainName,
		Protocol:          j.ChainProtocol,
		ContractAddress:   j.ContractAddress,
		DepositEnabled:    bool(j.DepositOpen),
		WithdrawalEnabled: bool(j.WithdrawOpen),
		MinDeposit:        dec(j.DepositMin),
		MinWithdrawal:     dec(j.WithdrawMin),
		MaxWithdrawal:     dec(j.WithdrawMax),
		WithdrawalFee:     dec(j.WithdrawFee),
		MinWithdrawalFee:  dec(j.WithdrawFeeMin),
		PlatformFeeRate:   dec(j.WithdrawFeePlatform),
		Burning:           bool(j.BurningState),
		OnChainBurnRate:   dec(j.WithdrawFeeOnChain),
		Memo:              MemoRequirement(j.TagType),
	}
}
