package coinstore

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/anuketfluxus/coinstore-api-client/internal/flexjson"
)

const (
	opSubscribe   = "SUB"
	opUnsubscribe = "UNSUB"
	opList        = "LIST"
	opRequest     = "REQ"

	opPong = "pong"
)

const (
	messageEcho     = "echo"
	messageResponse = "resp"
	messageTrade    = "trade"
	messageKline    = "kline"
	messageTicker   = "ticker"
	messageDepth    = "depth"
)

const responseCodeOK = 200

type channelCommand struct {
	Op      string          `json:"op"`
	Channel []string        `json:"channel"`
	Param   *subscribeParam `json:"param,omitempty"`
	ID      uint64          `json:"id"`
}

type subscribeParam struct {
	Size int `json:"size"`
}

type requestCommand struct {
	Op    string            `json:"op"`
	Param klineRequestParam `json:"param"`
	ID    uint64            `json:"id"`
}

type klineRequestParam struct {
	Channel string `json:"channel"`

	EndTime *int64 `json:"endTime"`
	Limit   int    `json:"limit,omitempty"`
}

func (p klineRequestParam) fits(payload json.RawMessage) bool {
	if p.EndTime == nil {
		return true
	}
	var reply struct {
		Item []struct {
			StartTime flexjson.Int64 `json:"startTime"`
		} `json:"item"`
	}
	if err := json.Unmarshal(payload, &reply); err != nil {
		return true
	}
	for _, kline := range reply.Item {
		if int64(kline.StartTime) > *p.EndTime {
			return false
		}
	}
	return true
}

func (s *StreamClient) subscribeCommand(group *subscriptionGroup) channelCommand {
	command := channelCommand{Op: opSubscribe, Channel: []string{group.channel}, ID: s.lastCommandID.Add(1)}
	if backfill := group.backfill(); backfill > 0 {
		command.Param = &subscribeParam{Size: backfill}
	}
	return command
}

func (s *StreamClient) unsubscribeCommand(channel string) channelCommand {
	return channelCommand{Op: opUnsubscribe, Channel: []string{channel}, ID: s.lastCommandID.Add(1)}
}

type streamMessage struct {
	Seq  flexjson.Int64 `json:"S"`
	Type string         `json:"T"`

	Op           string          `json:"op"`
	Code         flexjson.Int64  `json:"C"`
	Meaning      string          `json:"M"`
	ID           flexjson.Int64  `json:"id"`
	Channel      string          `json:"channel"`
	Symbol       string          `json:"symbol"`
	InstrumentID flexjson.Int64  `json:"instrumentId"`
	Data         json.RawMessage `json:"data"`
	Item         json.RawMessage `json:"item"`
	Subs         json.RawMessage `json:"subs"`
}

func (s *StreamClient) handleMessage(sess *streamSession, handlers *dispatcher, payload []byte) {
	var msg streamMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		handlers.reportError(fmt.Errorf("coinstore: stream: decode message: %w", err))
		return
	}
	if err := sess.trackSequence(int64(msg.Seq)); err != nil {
		handlers.reportError(err)
	}

	switch {
	case msg.Type == messageEcho:

	case msg.Op == opPong:

	case msg.Subs != nil:

		match := newReplyMatch([]string{listRequestKey}, s.pairs.placed)
		if outcome, _ := sess.resolve(listRequest, uint64(msg.ID), match, payload); outcome == unmatched {
			handlers.reportError(errors.New("coinstore: stream: discarded a subscription list that answers no pending request"))
		}
	case msg.Item != nil:
		learned := s.pairs.learn(msg.Channel, msg.Symbol, int64(msg.InstrumentID))
		s.resolveKlineReply(sess, handlers, msg, payload)
		if learned {

			s.requests.reconsider()
		}
	case msg.Type == messageResponse:
		if msg.Code != 0 && msg.Code != responseCodeOK {
			handlers.reportError(&StreamError{Code: int(msg.Code), Message: msg.Meaning})
		}
	default:
		updates, err := decodeUpdates(msg, payload)
		if err != nil {
			handlers.reportError(fmt.Errorf("coinstore: stream: %w", err))
			return
		}
		for _, u := range updates {
			s.learnPairNames(u)
			channel, err := s.subscribedChannel(u.route)
			if err != nil {
				handlers.reportError(err)
				continue
			}

			if channel != "" {
				handlers.dispatchUpdate(channel, u.update)
			}
		}
	}
}

func (s *StreamClient) resolveKlineReply(sess *streamSession, handlers *dispatcher, msg streamMessage, payload []byte) {
	if msg.ID == 0 && msg.Channel == "" {
		handlers.reportError(discardedKlineReply("", "names neither its request nor its channel"))
		return
	}
	r := newRoute(msg.Channel, "", msg.Symbol, int64(msg.InstrumentID))
	match := newReplyMatch(s.pairs.withAliases(r.channelNames()), s.pairs.placed)
	switch outcome, keys := sess.resolve(klineRequest, uint64(msg.ID), match, payload); outcome {
	case misfit:
		handlers.reportError(discardedKlineReply(msg.Channel, "does not fit the pending request"))
	case unmatched:
		handlers.reportError(discardedKlineReply(msg.Channel, "answers no pending request"))
	case unconfirmed:
		handlers.reportError(discardedKlineReply(msg.Channel,
			"names no pending request but may answer one under a name the client cannot relate to the reply's pair: "+strings.Join(keys, ", ")))
	case ambiguous:
		handlers.reportError(discardedKlineReply(msg.Channel,
			"could answer several requests, or one that an ambiguous reply failed: "+strings.Join(keys, ", ")))
	}
}

func discardedKlineReply(channel, reason string) error {
	if channel == "" {
		return fmt.Errorf("coinstore: stream: discarded a kline history reply that %s", reason)
	}
	return fmt.Errorf("coinstore: stream: discarded a kline history reply for %s that %s", channel, reason)
}

type routedUpdate struct {
	route  route
	update any

	itemRoutes []route
}

func decodeUpdates(msg streamMessage, payload []byte) ([]routedUpdate, error) {
	items, batched, err := msg.updateItems(payload)
	if err != nil {
		return nil, fmt.Errorf("decode %s update: %w", cmp.Or(msg.Type, "market data"), err)
	}
	var updates []routedUpdate
	kind := msg.updateKind(items)
	switch kind {
	case "":
		return nil, fmt.Errorf("ignored a message of unknown kind: %s", summarizeBody(payload))
	case messageTrade:
		updates, err = decodeEach[tradeItemJSON](msg.Channel, items)
	case messageKline:
		updates, err = decodeEach[klineItemJSON](msg.Channel, items)
	case messageDepth:
		updates, err = decodeEach[depthItemJSON](msg.Channel, items)
	case messageTicker:

		if batched && (msg.Channel == "" || msg.Channel == allTickersChannel) {
			updates, err = decodeAllTickers(items)
		} else {
			updates, err = decodeEach[tickerItemJSON](msg.Channel, items)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("decode %s update: %w", kind, err)
	}
	return updates, nil
}

func (m streamMessage) updateItems(payload []byte) (items []json.RawMessage, batched bool, err error) {
	switch {
	case isAbsent(m.Data):
		return []json.RawMessage{payload}, false, nil
	case m.Data[0] == '[':
		if err := json.Unmarshal(m.Data, &items); err != nil {
			return nil, false, err
		}
		return items, true, nil
	default:
		return []json.RawMessage{m.Data}, false, nil
	}
}

func (m streamMessage) updateKind(items []json.RawMessage) string {
	if m.Type != "" {
		return m.Type
	}
	if len(items) == 0 {
		return ""
	}
	channel := m.Channel
	if channel == "" {
		var first struct {
			Channel string `json:"channel"`
		}
		if json.Unmarshal(items[0], &first) == nil {
			channel = first.Channel
		}
	}
	if _, topic, found := strings.Cut(channel, "@"); found {
		kind, _, _ := strings.Cut(topic, "@")
		return kind
	}
	return kindByFields(items[0])
}

func kindByFields(item json.RawMessage) string {
	var fields struct {
		TradeID   json.RawMessage `json:"tradeId"`
		StartTime json.RawMessage `json:"startTime"`
		Interval  json.RawMessage `json:"interval"`
		Bids      json.RawMessage `json:"b"`
		Asks      json.RawMessage `json:"a"`
		Close     json.RawMessage `json:"close"`
	}
	if json.Unmarshal(item, &fields) != nil {
		return ""
	}
	switch {
	case fields.TradeID != nil:
		return messageTrade
	case fields.StartTime != nil || fields.Interval != nil:
		return messageKline
	case fields.Bids != nil || fields.Asks != nil:
		return messageDepth
	case fields.Close != nil:
		return messageTicker
	default:
		return ""
	}
}

type streamUpdate interface {
	decode(messageChannel string) routedUpdate
}

func decodeEach[T streamUpdate](messageChannel string, items []json.RawMessage) ([]routedUpdate, error) {
	updates := make([]routedUpdate, len(items))
	for i, item := range items {
		var decoded T
		if err := json.Unmarshal(item, &decoded); err != nil {
			return nil, err
		}
		updates[i] = decoded.decode(messageChannel)
	}
	return updates, nil
}

func decodeAllTickers(items []json.RawMessage) ([]routedUpdate, error) {
	tickers := make([]Ticker, len(items))
	itemRoutes := make([]route, len(items))
	for i, item := range items {
		var ticker tickerItemJSON
		if err := json.Unmarshal(item, &ticker); err != nil {
			return nil, err
		}
		tickers[i] = ticker.toTicker()
		itemRoutes[i] = newRoute(ticker.Channel, messageTicker, tickers[i].Symbol, tickers[i].SymbolID)
	}
	return []routedUpdate{{route: route{channel: allTickersChannel}, update: tickers, itemRoutes: itemRoutes}}, nil
}

func (s *StreamClient) learnPairNames(u routedUpdate) {
	learned := false
	if u.itemRoutes != nil {
		for _, r := range u.itemRoutes {
			learned = s.pairs.learn(r.channel, r.symbol, r.symbolID) || learned
		}
	} else {
		learned = s.pairs.learn(u.route.channel, u.route.symbol, u.route.symbolID)
	}
	if learned {
		s.requests.reconsider()
	}
}

func (s *StreamClient) subscribedChannel(r route) (string, error) {
	names := s.pairs.withAliases(r.channelNames())
	pairs := r.pairs()
	for _, pair := range pairs {
		if other, ok := s.pairs.alias(pair); ok && !slices.Contains(pairs, other) {
			pairs = append(pairs, other)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range names {
		if s.groups[name] != nil {
			return name, nil
		}
	}
	if r.channel != "" {
		return "", nil
	}

	kind, _, _ := strings.Cut(r.topic, "@")
	if len(pairs) == 0 {
		return "", fmt.Errorf("coinstore: stream: ignored a %s update that names neither its channel nor its pair", kind)
	}
	var candidates []string
	for channel := range s.groups {
		pair, topic, _ := strings.Cut(channel, "@")
		channelKind, _, _ := strings.Cut(topic, "@")
		if channelKind == kind && slices.Contains(pairs, pair) {
			candidates = append(candidates, channel)
		}
	}
	switch len(candidates) {
	case 0:
		return "", nil
	case 1:
		return candidates[0], nil
	default:
		slices.Sort(candidates)
		return "", fmt.Errorf("coinstore: stream: ignored a %s update without a channel, whose fields point to %s, which is not subscribed, while the pair has several %s subscriptions: %s",
			kind, names[0], kind, strings.Join(candidates, ", "))
	}
}

func (s *StreamClient) deliver(channel string, update any) {
	s.mu.Lock()
	var subscriptions []*Subscription
	if group := s.groups[channel]; group != nil {
		subscriptions = slices.Clone(group.subscriptions)
	}
	s.mu.Unlock()

	for _, sub := range subscriptions {
		if sub.active.Load() && !sub.deliver(update) {
			s.handleError(fmt.Errorf("coinstore: stream: channel %s carried an update of type %T, which its subscription does not take", channel, update))
		}
	}
}

type route struct {
	channel string

	topic    string
	symbol   string
	symbolID int64
}

func newRoute(channel, defaultTopic, symbol string, symbolID int64) route {
	topic := defaultTopic
	if _, channelTopic, found := strings.Cut(channel, "@"); found {
		topic = channelTopic
	}
	return route{channel: channel, topic: topic, symbol: symbol, symbolID: symbolID}
}

func (r route) pairs() []string {
	var pairs []string
	if r.symbolID != 0 {
		pairs = append(pairs, strconv.FormatInt(r.symbolID, 10))
	}
	if r.symbol != "" {
		pairs = append(pairs, strings.ToLower(r.symbol))
	}
	return pairs
}

func (r route) channelNames() []string {
	names := make([]string, 0, 3)
	add := func(name string) {
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	if r.channel != "" {
		add(r.channel)
	}
	if r.topic != "" {
		if r.symbolID != 0 {
			add(strconv.FormatInt(r.symbolID, 10) + "@" + r.topic)
		}
		if r.symbol != "" {
			add(strings.ToLower(r.symbol) + "@" + r.topic)
		}
	}
	return names
}

type tradeItemJSON struct {
	Channel string `json:"channel"`
	tradeJSON
}

func (j tradeItemJSON) decode(messageChannel string) routedUpdate {
	trade := j.toTrade()
	return routedUpdate{
		route:  newRoute(cmp.Or(j.Channel, messageChannel), messageTrade, trade.Symbol, trade.SymbolID),
		update: trade,
	}
}

type klineItemJSON struct {
	Channel string `json:"channel"`
	klineJSON
}

func (j klineItemJSON) decode(messageChannel string) routedUpdate {
	kline := j.toKline()
	topic := messageKline + "@" + string(kline.Interval)
	return routedUpdate{
		route:  newRoute(cmp.Or(j.Channel, messageChannel), topic, kline.Symbol, kline.SymbolID),
		update: kline,
	}
}

type tickerItemJSON struct {
	Channel string `json:"channel"`
	tickerJSON
}

func (j tickerItemJSON) decode(messageChannel string) routedUpdate {
	ticker := j.toTicker()
	return routedUpdate{
		route:  newRoute(cmp.Or(j.Channel, messageChannel), messageTicker, ticker.Symbol, ticker.SymbolID),
		update: ticker,
	}
}

type depthItemJSON struct {
	Channel string         `json:"channel"`
	Level   flexjson.Int64 `json:"level"`
	orderBookJSON
}

func (j depthItemJSON) decode(messageChannel string) routedUpdate {
	book := j.toOrderBook()
	topic := messageDepth + "@" + strconv.FormatInt(int64(j.Level), 10)
	return routedUpdate{
		route:  newRoute(cmp.Or(j.Channel, messageChannel), topic, book.Symbol, book.SymbolID),
		update: book,
	}
}
