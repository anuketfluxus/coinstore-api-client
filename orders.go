package coinstore

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/anuketfluxus/coinstore-api-client/internal/flexjson"
	"github.com/shopspring/decimal"
)

type OrderType string

const (
	OrderTypeMarket OrderType = "MARKET"
	OrderTypeLimit  OrderType = "LIMIT"

	OrderTypePostOnly OrderType = "POST_ONLY"
)

type TimeInForce string

const (
	TimeInForceGTC TimeInForce = "GTC"

	TimeInForceIOC TimeInForce = "IOC"

	TimeInForceFOK TimeInForce = "FOK"
)

type OrderStatus string

const (
	OrderStatusNotFound      OrderStatus = "NOT_FOUND"
	OrderStatusRejected      OrderStatus = "REJECTED"
	OrderStatusSubmitting    OrderStatus = "SUBMITTING"
	OrderStatusSubmitted     OrderStatus = "SUBMITTED"
	OrderStatusPartialFilled OrderStatus = "PARTIAL_FILLED"
	OrderStatusCanceling     OrderStatus = "CANCELING"
	OrderStatusCanceled      OrderStatus = "CANCELED"
	OrderStatusExpired       OrderStatus = "EXPIRED"
	OrderStatusStopped       OrderStatus = "STOPPED"
	OrderStatusFilled        OrderStatus = "FILLED"
)

type MatchRole string

const (
	MatchRoleTaker MatchRole = "TAKER"
	MatchRoleMaker MatchRole = "MAKER"
)

type OrderRequest struct {
	ClientOrderID string
	Side          Side
	Type          OrderType

	TimeInForce TimeInForce
	Price       decimal.Decimal
	Quantity    decimal.Decimal
	QuoteAmount decimal.Decimal
}

type Order struct {
	OrderID       int64
	ClientOrderID string
	AccountID     int64
	Symbol        string
	BaseCurrency  string
	QuoteCurrency string
	Side          Side
	Type          OrderType
	TimeInForce   TimeInForce
	Status        OrderStatus
	Price         decimal.Decimal
	Quantity      decimal.Decimal

	QuoteAmount decimal.Decimal

	FilledQuantity    decimal.Decimal
	FilledAmount      decimal.Decimal
	RemainingQuantity decimal.Decimal
	AveragePrice      decimal.Decimal
	FeeCurrency       string
	CreatedAt         time.Time

	UpdatedAt time.Time
}

type OpenOrdersFilter struct {
	Symbol        string
	OrderID       int64
	ClientOrderID string
}

type OrderLookup struct {
	OrderID       int64
	ClientOrderID string
}

type FillsFilter struct {
	OrderID int64

	Side Side

	Page     int
	PageSize int
}

type Fill struct {
	ID              int64
	TradeID         int64
	MatchID         int64
	OrderID         int64
	AccountID       int64
	SymbolID        int64
	BaseCurrencyID  int64
	QuoteCurrencyID int64
	Side            Side
	Role            MatchRole

	Quantity      decimal.Decimal
	QuoteAmount   decimal.Decimal
	Fee           decimal.Decimal
	FeeCurrencyID int64
	FeeRate       decimal.Decimal

	RemainingQuantity decimal.Decimal

	SelfTradeQuantity decimal.Decimal

	OrderState int
	Time       time.Time
}

type PlaceOrderResult struct {
	OrderID int64

	ClientOrderID string

	Err error
}

type CancelResult struct {
	OrderID       int64
	ClientOrderID string
	Status        OrderStatus
}

type CancelOrdersResult struct {
	Canceled []int64
	Rejected []int64

	Unconfirmed []int64
}

type CancelOrdersByClientIDResult struct {
	Canceled []string
	Rejected []string

	Unconfirmed []string
}

func (c *Client) OpenOrders(ctx context.Context) ([]Order, error) {
	var orders []orderJSON
	if err := c.do(ctx, c.get("/trade/order/active", nil, signed), &orders); err != nil {
		return nil, err
	}
	return convertAll(orders, orderJSON.toOrder), nil
}

func (c *Client) OpenOrdersV2(ctx context.Context, filter OpenOrdersFilter) ([]Order, error) {
	if err := validateNotNegative("order ID", filter.OrderID); err != nil {
		return nil, err
	}
	query := url.Values{}
	if filter.Symbol != "" {
		query.Set("symbol", filter.Symbol)
	}
	if filter.OrderID != 0 {
		query.Set("ordId", strconv.FormatInt(filter.OrderID, 10))
	}
	if filter.ClientOrderID != "" {
		query.Set("clOrdId", filter.ClientOrderID)
	}

	var orders []orderJSON
	if err := c.do(ctx, c.get("/v2/trade/order/active", query, signed), &orders); err != nil {
		return nil, err
	}
	return convertAll(orders, orderJSON.toOrder), nil
}

func (c *Client) Order(ctx context.Context, orderID int64) (Order, error) {
	if err := validateID("order ID", orderID); err != nil {
		return Order{}, err
	}
	query := url.Values{"ordId": {strconv.FormatInt(orderID, 10)}}

	const path = "/trade/order/orderInfo"
	var order orderJSON
	if err := c.do(ctx, c.get(path, query, signed), &order); err != nil {
		return Order{}, err
	}
	if !order.exists() {
		return Order{}, fmt.Errorf("%w (GET %s)", ErrOrderNotFound, path)
	}
	return order.toOrder(), nil
}

func (c *Client) OrderV2(ctx context.Context, lookup OrderLookup) (Order, error) {
	if err := lookup.validate(); err != nil {
		return Order{}, err
	}
	query := url.Values{}
	if lookup.OrderID != 0 {
		query.Set("ordId", strconv.FormatInt(lookup.OrderID, 10))
	} else {
		query.Set("clOrdId", lookup.ClientOrderID)
	}

	const path = "/v2/trade/order/orderInfo"
	var orders orderInfoJSON
	if err := c.do(ctx, c.get(path, query, signed), &orders); err != nil {
		return Order{}, err
	}
	switch {
	case len(orders) == 0 || (len(orders) == 1 && !orders[0].exists()):
		return Order{}, fmt.Errorf("%w (GET %s)", ErrOrderNotFound, path)
	case len(orders) == 1:
		return orders[0].toOrder(), nil
	default:
		return Order{}, fmt.Errorf("coinstore: GET %s: lookup matched %d orders, want 1", path, len(orders))
	}
}

func (c *Client) Fills(ctx context.Context, symbol string, filter FillsFilter) ([]Fill, error) {
	if err := validateSymbol(symbol); err != nil {
		return nil, err
	}
	if err := filter.validate(); err != nil {
		return nil, err
	}
	query := url.Values{"symbol": {symbol}}
	if filter.OrderID != 0 {
		query.Set("ordId", strconv.FormatInt(filter.OrderID, 10))
	}
	if filter.Page != 0 {
		query.Set("pageNum", strconv.Itoa(filter.Page))
	}
	if filter.PageSize != 0 {
		query.Set("pageSize", strconv.Itoa(filter.PageSize))
	}
	if code, ok := sideCode(filter.Side); ok {
		query.Set("side", code)
	}

	var fills []fillJSON
	if err := c.do(ctx, c.get("/trade/match/accountMatches", query, signed), &fills); err != nil {
		return nil, err
	}
	return convertAll(fills, fillJSON.toFill), nil
}

func (c *Client) PlaceOrder(ctx context.Context, symbol string, order OrderRequest) (int64, error) {
	if err := validateSymbol(symbol); err != nil {
		return 0, err
	}
	if err := order.validate(); err != nil {
		return 0, err
	}
	issuedAt := c.now()
	body := placeOrderBody{
		Symbol:    symbol,
		orderBody: order.body(),
		Timestamp: issuedAt.UnixMilli(),
	}
	r := c.change("/trade/order/place", body)
	r.issuedAt = issuedAt

	var result placeOrderResultJSON
	if err := c.do(ctx, r, &result); err != nil {
		return 0, err
	}
	orderID := result.orderID()
	if orderID == 0 {
		return 0, outcomeUnknown(errors.New("coinstore: POST /trade/order/place: the API reported success without an order ID"))
	}
	return orderID, nil
}

func (c *Client) PlaceOrders(ctx context.Context, symbol string, orders []OrderRequest) ([]PlaceOrderResult, error) {
	if err := validateSymbol(symbol); err != nil {
		return nil, err
	}
	if len(orders) == 0 {
		return nil, invalidRequest("a batch needs at least one order")
	}
	bodies := make([]orderBody, len(orders))
	clientOrderIDs := make([]string, len(orders))
	seen := make(map[string]bool, len(orders))
	for i, order := range orders {
		if err := order.validate(); err != nil {
			return nil, fmt.Errorf("%w (order %d of the batch)", err, i)
		}
		if order.ClientOrderID == "" {
			id, err := newClientOrderID()
			if err != nil {
				return nil, fmt.Errorf("coinstore: generate a client order ID: %w", err)
			}
			order.ClientOrderID = id
		}
		if seen[order.ClientOrderID] {
			return nil, invalidRequest("client order ID %q appears twice in the batch", order.ClientOrderID)
		}
		seen[order.ClientOrderID] = true
		bodies[i] = order.body()
		clientOrderIDs[i] = order.ClientOrderID
	}
	issuedAt := c.now()
	r := c.change("/trade/order/placeBatch", placeOrdersBody{
		Symbol:    symbol,
		Orders:    bodies,
		Timestamp: issuedAt.UnixMilli(),
	})
	r.issuedAt = issuedAt

	var outcomes []placeOrderOutcomeJSON
	if err := c.do(ctx, r, &outcomes); err != nil {
		results := make([]PlaceOrderResult, len(orders))
		for i, clientOrderID := range clientOrderIDs {
			results[i] = PlaceOrderResult{ClientOrderID: clientOrderID, Err: err}
		}
		return results, err
	}
	byClientOrderID := make(map[string]placeOrderOutcomeJSON, len(outcomes))
	for _, outcome := range outcomes {
		byClientOrderID[string(outcome.ClOrdID)] = outcome
	}
	results := make([]PlaceOrderResult, len(orders))
	for i, clientOrderID := range clientOrderIDs {
		outcome, reported := byClientOrderID[clientOrderID]
		results[i] = outcome.toPlaceOrderResult(r, clientOrderID, reported)
	}
	return results, nil
}

func (c *Client) CancelOrder(ctx context.Context, symbol string, orderID int64) (CancelResult, error) {
	if err := validateSymbol(symbol); err != nil {
		return CancelResult{}, err
	}
	if err := validateID("order ID", orderID); err != nil {
		return CancelResult{}, err
	}
	body := cancelOrderBody{Symbol: symbol, OrdID: orderID, OrderID: orderID}

	var result cancelResultJSON
	if err := c.do(ctx, c.change("/trade/order/cancel", body), &result); err != nil {
		return CancelResult{}, err
	}
	return CancelResult{
		OrderID:       cmp.Or(int64(result.OrdID), orderID),
		ClientOrderID: string(result.ClientOrderID),
		Status:        OrderStatus(result.State),
	}, nil
}

func (c *Client) CancelOrders(ctx context.Context, symbol string, orderIDs []int64) (CancelOrdersResult, error) {
	if err := validateSymbol(symbol); err != nil {
		return CancelOrdersResult{}, err
	}
	if len(orderIDs) == 0 {
		return CancelOrdersResult{}, invalidRequest("at least one order ID is required")
	}
	for _, id := range orderIDs {
		if err := validateID("order ID", id); err != nil {
			return CancelOrdersResult{}, err
		}
	}
	body := cancelOrdersBody{Symbol: symbol, OrderIDs: orderIDs, OrderID: orderIDs}

	var result struct {
		Success []flexjson.Int64 `json:"success"`
		Reject  []flexjson.Int64 `json:"reject"`
	}
	if err := c.do(ctx, c.change("/trade/order/cancelBatch", body), &result); err != nil {
		return CancelOrdersResult{}, err
	}
	canceled, rejected := int64s(result.Success), int64s(result.Reject)
	return CancelOrdersResult{
		Canceled:    canceled,
		Rejected:    rejected,
		Unconfirmed: unmentioned(orderIDs, canceled, rejected),
	}, nil
}

func (c *Client) CancelOrdersByClientID(ctx context.Context, symbol string, clientOrderIDs []string) (CancelOrdersByClientIDResult, error) {
	if err := validateSymbol(symbol); err != nil {
		return CancelOrdersByClientIDResult{}, err
	}
	if len(clientOrderIDs) == 0 {
		return CancelOrdersByClientIDResult{}, invalidRequest("at least one client order ID is required")
	}
	for _, id := range clientOrderIDs {
		if err := validateRequired("client order ID", id); err != nil {
			return CancelOrdersByClientIDResult{}, err
		}
	}
	body := cancelOrdersByClientIDBody{Symbol: symbol, ClientOrderIDs: clientOrderIDs}

	var result struct {
		Success []flexjson.String `json:"success"`
		Reject  []flexjson.String `json:"reject"`
	}
	if err := c.do(ctx, c.change("/trade/order/cancelBatchByClOrdId", body), &result); err != nil {
		return CancelOrdersByClientIDResult{}, err
	}
	canceled, rejected := stringsOf(result.Success), stringsOf(result.Reject)
	return CancelOrdersByClientIDResult{
		Canceled:    canceled,
		Rejected:    rejected,
		Unconfirmed: unmentioned(clientOrderIDs, canceled, rejected),
	}, nil
}

func unmentioned[T comparable](requested []T, reported ...[]T) []T {
	mentioned := make(map[T]bool)
	for _, list := range reported {
		for _, item := range list {
			mentioned[item] = true
		}
	}
	missing := make([]T, 0)
	for _, item := range requested {
		if !mentioned[item] {
			missing = append(missing, item)
			mentioned[item] = true
		}
	}
	return missing
}

func (o OrderRequest) validate() error {
	if o.Side != SideBuy && o.Side != SideSell {
		return invalidRequest("order side %q is neither BUY nor SELL", o.Side)
	}
	amounts := []struct {
		name  string
		value decimal.Decimal
	}{
		{"price", o.Price},
		{"quantity", o.Quantity},
		{"quote amount", o.QuoteAmount},
	}
	for _, amount := range amounts {
		if amount.value.IsNegative() {
			return invalidRequest("order %s must not be negative, got %s", amount.name, amount.value)
		}
	}
	switch o.Type {
	case OrderTypeLimit, OrderTypePostOnly:
		if !o.Price.IsPositive() || !o.Quantity.IsPositive() {
			return invalidRequest("%s orders need a positive price and quantity", o.Type)
		}
		if !o.QuoteAmount.IsZero() {
			return invalidRequest("%s orders take no quote amount", o.Type)
		}
	case OrderTypeMarket:
		if !o.Price.IsZero() {
			return invalidRequest("MARKET orders take no price")
		}
		if o.Side == SideBuy {
			if !o.QuoteAmount.IsPositive() {
				return invalidRequest("MARKET buy orders need a positive quote amount")
			}
			if !o.Quantity.IsZero() {
				return invalidRequest("MARKET buy orders take a quote amount, not a quantity")
			}
		} else {
			if !o.Quantity.IsPositive() {
				return invalidRequest("MARKET sell orders need a positive quantity")
			}
			if !o.QuoteAmount.IsZero() {
				return invalidRequest("MARKET sell orders take a quantity, not a quote amount")
			}
		}
	default:
		return invalidRequest("order type %q is not one of MARKET, LIMIT and POST_ONLY", o.Type)
	}
	return nil
}

func (o OrderRequest) body() orderBody {
	return orderBody{
		ClOrdID:     o.ClientOrderID,
		Side:        o.Side,
		OrdType:     o.Type,
		TimeInForce: o.TimeInForce,
		OrdPrice:    flexjson.Decimal(o.Price),
		OrdQty:      flexjson.Decimal(o.Quantity),
		OrdAmt:      flexjson.Decimal(o.QuoteAmount),
	}
}

func (l OrderLookup) validate() error {
	if err := validateNotNegative("order ID", l.OrderID); err != nil {
		return err
	}
	switch hasID, hasClientID := l.OrderID != 0, l.ClientOrderID != ""; {
	case hasID && hasClientID:
		return invalidRequest("an order lookup takes an order ID or a client order ID, not both")
	case !hasID && !hasClientID:
		return invalidRequest("an order lookup needs an order ID or a client order ID")
	}
	return nil
}

func (f FillsFilter) validate() error {
	if err := validateNotNegative("order ID", f.OrderID); err != nil {
		return err
	}
	if err := validateNotNegative("page", f.Page); err != nil {
		return err
	}
	if err := validateNotNegative("page size", f.PageSize); err != nil {
		return err
	}
	if _, ok := sideCode(f.Side); f.Side != "" && !ok {
		return invalidRequest("fill side %q is neither BUY nor SELL", f.Side)
	}
	return nil
}

func sideCode(side Side) (code string, ok bool) {
	switch side {
	case SideBuy:
		return "1", true
	case SideSell:
		return "-1", true
	default:
		return "", false
	}
}

func sideFromCode(code flexjson.String) Side {
	switch code {
	case "1":
		return SideBuy
	case "-1":
		return SideSell
	default:
		return Side(code)
	}
}

func matchRoleFromCode(code flexjson.String) MatchRole {
	switch code {
	case "1":
		return MatchRoleTaker
	case "-1":
		return MatchRoleMaker
	default:
		return MatchRole(code)
	}
}

type orderBody struct {
	ClOrdID     string           `json:"clOrdId,omitempty"`
	Side        Side             `json:"side"`
	OrdType     OrderType        `json:"ordType"`
	TimeInForce TimeInForce      `json:"timeInForce,omitempty"`
	OrdPrice    flexjson.Decimal `json:"ordPrice,omitzero"`
	OrdQty      flexjson.Decimal `json:"ordQty,omitzero"`
	OrdAmt      flexjson.Decimal `json:"ordAmt,omitzero"`
}

type placeOrderBody struct {
	Symbol string `json:"symbol"`
	orderBody
	Timestamp int64 `json:"timestamp"`
}

type placeOrdersBody struct {
	Symbol    string      `json:"symbol"`
	Orders    []orderBody `json:"orders"`
	Timestamp int64       `json:"timestamp"`
}

type placeOrderResultJSON struct {
	OrdID flexjson.Int64 `json:"ordId"`

	OrderID flexjson.Int64 `json:"order_id"`
}

func (j placeOrderResultJSON) orderID() int64 {
	if j.OrdID != 0 {
		return int64(j.OrdID)
	}
	return int64(j.OrderID)
}

type placeOrderOutcomeJSON struct {
	OrdID   flexjson.Int64  `json:"ordId"`
	ClOrdID flexjson.String `json:"clOrdId"`
	ErrNo   flexjson.Int64  `json:"errno"`
	ErrMsg  string          `json:"errMsg"`
}

func (j placeOrderOutcomeJSON) toPlaceOrderResult(r apiRequest, clientOrderID string, reported bool) PlaceOrderResult {
	result := PlaceOrderResult{OrderID: int64(j.OrdID), ClientOrderID: clientOrderID}
	switch {
	case !reported:
		result.Err = outcomeUnknown(fmt.Errorf("coinstore: %s %s: the response omits order %q", r.method, r.path, clientOrderID))
	case j.ErrNo != 0 || (j.OrdID == 0 && j.ErrMsg != ""):
		rejection := &APIError{
			Method:     r.method,
			Path:       r.path,
			HTTPStatus: http.StatusOK,
			Code:       ErrorCode(j.ErrNo),
			Message:    j.ErrMsg,
		}
		if j.OrdID != 0 {
			result.Err = outcomeUnknown(fmt.Errorf("%w, although the response also gives order %q the order ID %d", rejection, clientOrderID, j.OrdID))
		} else {
			result.Err = r.rejectedOrUnconfirmed(rejection)
		}
	case j.OrdID == 0:
		result.Err = outcomeUnknown(fmt.Errorf("coinstore: %s %s: the response reports order %q without an order ID or an error", r.method, r.path, clientOrderID))
	}
	return result
}

func newClientOrderID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

type cancelOrderBody struct {
	Symbol  string `json:"symbol"`
	OrdID   int64  `json:"ordId"`
	OrderID int64  `json:"orderId"`
}

type cancelResultJSON struct {
	OrdID         flexjson.Int64  `json:"ordId"`
	ClientOrderID flexjson.String `json:"clientOrderId"`
	State         string          `json:"state"`
}

type cancelOrdersBody struct {
	Symbol   string  `json:"symbol"`
	OrderIDs []int64 `json:"orderIds"`
	OrderID  []int64 `json:"orderId"`
}

type cancelOrdersByClientIDBody struct {
	Symbol         string   `json:"symbol"`
	ClientOrderIDs []string `json:"clientOrderIds"`
}

type orderInfoJSON []orderJSON

func (o *orderInfoJSON) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) > 0 && data[0] == '{' {
		var order orderJSON
		if err := json.Unmarshal(data, &order); err != nil {
			return err
		}
		*o = orderInfoJSON{order}
		return nil
	}
	return json.Unmarshal(data, (*[]orderJSON)(o))
}

type orderJSON struct {
	OrdID         flexjson.Int64  `json:"ordId"`
	ClOrdID       flexjson.String `json:"clOrdId"`
	AccountID     flexjson.Int64  `json:"accountId"`
	Symbol        string          `json:"symbol"`
	BaseCurrency  string          `json:"baseCurrency"`
	QuoteCurrency string          `json:"quoteCurrency"`
	Side          string          `json:"side"`
	OrdType       string          `json:"ordType"`
	TimeInForce   string          `json:"timeInForce"`
	OrdStatus     string          `json:"ordStatus"`

	OrdState        string           `json:"ordState"`
	OrdPrice        flexjson.Decimal `json:"ordPrice"`
	OrdQty          flexjson.Decimal `json:"ordQty"`
	OrdAmt          flexjson.Decimal `json:"ordAmt"`
	CumQty          flexjson.Decimal `json:"cumQty"`
	CumAmt          flexjson.Decimal `json:"cumAmt"`
	LeavesQty       flexjson.Decimal `json:"leavesQty"`
	AvgPrice        flexjson.Decimal `json:"avgPrice"`
	FeeCurrency     string           `json:"feeCurrency"`
	Timestamp       flexjson.Int64   `json:"timestamp"`
	OrderUpdateTime flexjson.Int64   `json:"orderUpdateTime"`
}

func (j orderJSON) status() OrderStatus {
	if j.OrdStatus != "" {
		return OrderStatus(j.OrdStatus)
	}
	return OrderStatus(j.OrdState)
}

func (j orderJSON) exists() bool {
	return j.OrdID != 0 && j.status() != OrderStatusNotFound
}

func (j orderJSON) toOrder() Order {
	return Order{
		OrderID:           int64(j.OrdID),
		ClientOrderID:     string(j.ClOrdID),
		AccountID:         int64(j.AccountID),
		Symbol:            j.Symbol,
		BaseCurrency:      j.BaseCurrency,
		QuoteCurrency:     j.QuoteCurrency,
		Side:              Side(j.Side),
		Type:              OrderType(j.OrdType),
		TimeInForce:       TimeInForce(j.TimeInForce),
		Status:            j.status(),
		Price:             dec(j.OrdPrice),
		Quantity:          dec(j.OrdQty),
		QuoteAmount:       dec(j.OrdAmt),
		FilledQuantity:    dec(j.CumQty),
		FilledAmount:      dec(j.CumAmt),
		RemainingQuantity: dec(j.LeavesQty),
		AveragePrice:      dec(j.AvgPrice),
		FeeCurrency:       j.FeeCurrency,
		CreatedAt:         timeFromUnixMilli(j.Timestamp),
		UpdatedAt:         timeFromUnixMilli(j.OrderUpdateTime),
	}
}

type fillJSON struct {
	ID              flexjson.Int64   `json:"id"`
	TradeID         flexjson.Int64   `json:"tradeId"`
	MatchID         flexjson.Int64   `json:"matchId"`
	OrderID         flexjson.Int64   `json:"orderId"`
	AccountID       flexjson.Int64   `json:"accountId"`
	InstrumentID    flexjson.Int64   `json:"instrumentId"`
	BaseCurrencyID  flexjson.Int64   `json:"baseCurrencyId"`
	QuoteCurrencyID flexjson.Int64   `json:"quoteCurrencyId"`
	Side            flexjson.String  `json:"side"`
	MatchRole       flexjson.String  `json:"matchRole"`
	Role            flexjson.String  `json:"role"`
	ExecQty         flexjson.Decimal `json:"execQty"`
	ExecAmt         flexjson.Decimal `json:"execAmt"`
	Fee             flexjson.Decimal `json:"fee"`
	FeeCurrencyID   flexjson.Int64   `json:"feeCurrencyId"`

	ActuralFeeRate flexjson.Decimal `json:"acturalFeeRate"`
	RemainingQty   flexjson.Decimal `json:"remainingQty"`
	SelfDealingQty flexjson.Decimal `json:"selfDealingQty"`
	OrderState     flexjson.Int64   `json:"orderState"`

	MatchTime flexjson.Int64 `json:"matchTime"`
}

func (j fillJSON) toFill() Fill {
	role := j.MatchRole
	if role == "" {
		role = j.Role
	}
	return Fill{
		ID:                int64(j.ID),
		TradeID:           int64(j.TradeID),
		MatchID:           int64(j.MatchID),
		OrderID:           int64(j.OrderID),
		AccountID:         int64(j.AccountID),
		SymbolID:          int64(j.InstrumentID),
		BaseCurrencyID:    int64(j.BaseCurrencyID),
		QuoteCurrencyID:   int64(j.QuoteCurrencyID),
		Side:              sideFromCode(j.Side),
		Role:              matchRoleFromCode(role),
		Quantity:          dec(j.ExecQty),
		QuoteAmount:       dec(j.ExecAmt),
		Fee:               dec(j.Fee),
		FeeCurrencyID:     int64(j.FeeCurrencyID),
		FeeRate:           dec(j.ActuralFeeRate),
		RemainingQuantity: dec(j.RemainingQty),
		SelfTradeQuantity: dec(j.SelfDealingQty),
		OrderState:        int(j.OrderState),
		Time:              timeFromUnix(j.MatchTime),
	}
}
