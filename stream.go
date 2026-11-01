package coinstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anuketfluxus/coinstore-api-client/internal/flexjson"
	"github.com/gorilla/websocket"
)

const DefaultStreamURL = "wss://ws.coinstore.com/s/ws"

const (
	serverPingInterval = 3 * time.Minute

	defaultStreamIdleTimeout       = 2*serverPingInterval + 30*time.Second
	defaultStreamKeepaliveInterval = time.Minute
	defaultMinReconnectDelay       = time.Second
	defaultMaxReconnectDelay       = 30 * time.Second

	stableConnectionAge = time.Minute

	defaultAbandonedRequestTTL = 10 * time.Second

	streamHandshakeTimeout = 30 * time.Second
	streamWriteTimeout     = 10 * time.Second

	streamCloseTimeout = time.Second

	maxStreamMessageBytes = 16 << 20

	allTickersChannel = "!@ticker"
	maxKlineHistory   = 200

	listRequestKey = "LIST"
)

var ErrStreamDisconnected = errors.New("coinstore: stream: disconnected before the response arrived")

var ErrStreamStopped = errors.New("coinstore: stream: Run is not executing")

var ErrAmbiguousReply = errors.New("coinstore: stream: the reply could belong to another request")

var ErrUnconfirmedReply = errors.New("coinstore: stream: a reply naming the pair another way may have answered the request")

type StreamClient struct {
	url                 string
	dialer              *websocket.Dialer
	onError             func(error)
	onConnect           func()
	idleTimeout         time.Duration
	keepaliveInterval   time.Duration
	minReconnectDelay   time.Duration
	maxReconnectDelay   time.Duration
	abandonedRequestTTL time.Duration
	backlogLimits       backlogLimits

	lastCommandID atomic.Uint64

	mu sync.Mutex

	groups map[string]*subscriptionGroup

	session *streamSession

	running bool

	runDone <-chan struct{}

	changed chan struct{}

	pairs pairNames

	requests requestScheduler
}

type StreamOption func(*streamOptions)

type streamOptions struct {
	url               string
	onError           func(error)
	onConnect         func()
	idleTimeout       time.Duration
	keepaliveInterval time.Duration
	minReconnectDelay time.Duration
	maxReconnectDelay time.Duration
}

func WithStreamURL(streamURL string) StreamOption {
	return func(o *streamOptions) {
		o.url = streamURL
	}
}

func WithStreamErrorHandler(handler func(error)) StreamOption {
	return func(o *streamOptions) {
		o.onError = handler
	}
}

func WithStreamConnectHandler(handler func()) StreamOption {
	return func(o *streamOptions) {
		o.onConnect = handler
	}
}

func WithStreamReconnectDelay(minDelay, maxDelay time.Duration) StreamOption {
	return func(o *streamOptions) {
		o.minReconnectDelay = minDelay
		o.maxReconnectDelay = maxDelay
	}
}

func WithStreamIdleTimeout(timeout time.Duration) StreamOption {
	return func(o *streamOptions) {
		o.idleTimeout = timeout
	}
}

func WithStreamKeepaliveInterval(interval time.Duration) StreamOption {
	return func(o *streamOptions) {
		o.keepaliveInterval = interval
	}
}

func NewStreamClient(opts ...StreamOption) (*StreamClient, error) {
	o := streamOptions{
		url:               DefaultStreamURL,
		idleTimeout:       defaultStreamIdleTimeout,
		keepaliveInterval: defaultStreamKeepaliveInterval,
		minReconnectDelay: defaultMinReconnectDelay,
		maxReconnectDelay: defaultMaxReconnectDelay,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if err := o.validate(); err != nil {
		return nil, err
	}

	client := &StreamClient{
		url: o.url,
		dialer: &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: streamHandshakeTimeout,
		},
		onError:             o.onError,
		onConnect:           o.onConnect,
		idleTimeout:         o.idleTimeout,
		keepaliveInterval:   o.keepaliveInterval,
		minReconnectDelay:   o.minReconnectDelay,
		maxReconnectDelay:   o.maxReconnectDelay,
		abandonedRequestTTL: defaultAbandonedRequestTTL,
		backlogLimits:       backlogLimits{updates: defaultMaxBacklogUpdates, errors: defaultMaxBacklogErrors},
		groups:              make(map[string]*subscriptionGroup),
		changed:             make(chan struct{}),
	}
	client.requests.names = &client.pairs
	return client, nil
}

func (o streamOptions) validate() error {
	u, err := url.Parse(o.url)
	if err != nil {
		return fmt.Errorf("coinstore: invalid stream URL: %w", err)
	}
	if (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
		return fmt.Errorf("coinstore: invalid stream URL %q: want an absolute ws or wss URL", o.url)
	}
	durations := []struct {
		name  string
		value time.Duration
	}{
		{"idle timeout", o.idleTimeout},
		{"keepalive interval", o.keepaliveInterval},
		{"minimum reconnect delay", o.minReconnectDelay},
		{"maximum reconnect delay", o.maxReconnectDelay},
	}
	for _, d := range durations {
		if d.value <= 0 {
			return fmt.Errorf("coinstore: stream %s must be positive, got %s", d.name, d.value)
		}
	}
	if o.minReconnectDelay > o.maxReconnectDelay {
		return fmt.Errorf("coinstore: minimum reconnect delay %s exceeds maximum %s", o.minReconnectDelay, o.maxReconnectDelay)
	}
	return nil
}

func (s *StreamClient) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := s.begin(runCtx.Done()); err != nil {
		return err
	}

	handlers := startDispatcher(s.backlogLimits, s.deliver, s.onError, cancel)
	stopHalting := context.AfterFunc(runCtx, handlers.halt)
	defer func() {
		stopHalting()
		handlers.halt()
		handlers.wait()
		s.end()
		handlers.reraise()
	}()

	delays := backoff{minDelay: s.minReconnectDelay, maxDelay: s.maxReconnectDelay}
	for {
		connected, err := s.connectAndServe(runCtx, handlers)
		if runCtx.Err() != nil {
			return ctx.Err()
		}
		handlers.reportError(err)

		if !connected.IsZero() && time.Since(connected) >= stableConnectionAge {
			delays.reset()
		}
		timer := time.NewTimer(delays.next())
		select {
		case <-runCtx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *StreamClient) begin(done <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return errors.New("coinstore: stream: Run is already executing")
	}
	s.running = true
	s.runDone = done
	s.broadcastChange()
	return nil
}

func (s *StreamClient) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	s.broadcastChange()
}

func (s *StreamClient) broadcastChange() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *StreamClient) connectAndServe(ctx context.Context, handlers *dispatcher) (connected time.Time, err error) {
	conn, resp, err := s.dialer.DialContext(ctx, s.url, http.Header{"User-Agent": {userAgent}})
	if err != nil {
		if resp != nil {
			return time.Time{}, fmt.Errorf("coinstore: stream: connect: %w (HTTP %d)", err, resp.StatusCode)
		}
		return time.Time{}, fmt.Errorf("coinstore: stream: connect: %w", err)
	}
	connected = time.Now()
	return connected, s.serve(ctx, newStreamSession(conn), handlers)
}

func (s *StreamClient) serve(ctx context.Context, sess *streamSession, handlers *dispatcher) error {

	defer sess.terminate(errSessionEnded)

	sess.conn.SetReadLimit(maxStreamMessageBytes)
	if err := sess.extendReadDeadline(s.idleTimeout); err != nil {
		sess.terminate(err)
		return fmt.Errorf("coinstore: stream: %w", sess.cause())
	}
	sess.conn.SetPingHandler(func(appData string) error {
		return sess.answerPing(appData, s.idleTimeout)
	})
	stopShutdown := context.AfterFunc(ctx, sess.closeGracefully)
	defer stopShutdown()
	go sess.sendKeepalives(s.keepaliveInterval)

	s.attach(sess)
	defer s.detach(sess)
	handlers.beginConnection(s.onConnect)

	for {
		_, payload, err := sess.conn.ReadMessage()
		if err != nil {
			sess.terminate(fmt.Errorf("connection lost: %w", err))
			return fmt.Errorf("coinstore: stream: %w", sess.cause())
		}
		if err := sess.extendReadDeadline(s.idleTimeout); err != nil {
			sess.terminate(err)
			return fmt.Errorf("coinstore: stream: %w", sess.cause())
		}
		s.handleMessage(sess, handlers, payload)
	}
}

func (s *StreamClient) attach(sess *streamSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.session = sess
	s.broadcastChange()
	for _, channel := range slices.Sorted(maps.Keys(s.groups)) {

		_ = sess.send(s.subscribeCommand(s.groups[channel]))
	}
}

func (s *StreamClient) detach(sess *streamSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session == sess {
		s.session = nil
		s.broadcastChange()
	}
}

func (s *StreamClient) awaitSession(ctx context.Context) (*streamSession, error) {
	for {
		s.mu.Lock()
		sess, runDone, changed := s.session, s.runDone, s.changed
		s.mu.Unlock()
		if isClosed(runDone) {
			return nil, ErrStreamStopped
		}
		if sess != nil && sess.acceptsRequests() {
			return sess, nil
		}
		select {
		case <-changed:
		case <-runDone:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (s *StreamClient) handleError(err error) {
	if s.onError != nil {
		s.onError(err)
	}
}

type TradeStream struct {
	Symbol string

	Backfill int
}

type KlineStream struct {
	Symbol   string
	Interval KlineInterval
}

type TickerStream struct {
	Symbol string
}

type OrderBookStream struct {
	Symbol string

	Depth int
}

type KlineHistoryRequest struct {
	Symbol   string
	Interval KlineInterval

	End time.Time

	Limit int
}

type ChannelInfo struct {
	Channel string

	Type         string
	SubscribedAt time.Time

	MessageCount int64
}

type StreamError struct {
	Code    int
	Message string
}

func (e *StreamError) Error() string {
	return fmt.Sprintf("coinstore: stream: server error %d: %s", e.Code, e.Message)
}

type SequenceGapError struct {
	Expected int64
	Received int64
}

func (e *SequenceGapError) Error() string {
	return fmt.Sprintf("coinstore: stream: message sequence gap: expected %d, received %d", e.Expected, e.Received)
}

type Subscription struct {
	client  *StreamClient
	channel string

	backfill int

	deliver func(update any) bool
	active  atomic.Bool
}

func (sub *Subscription) Channel() string {
	return sub.channel
}

func (sub *Subscription) Unsubscribe() {
	if sub.active.CompareAndSwap(true, false) {
		sub.client.remove(sub)
	}
}

type subscriptionGroup struct {
	channel       string
	subscriptions []*Subscription
}

func (g *subscriptionGroup) backfill() int {
	most := 0
	for _, sub := range g.subscriptions {
		most = max(most, sub.backfill)
	}
	return most
}

func (s *StreamClient) SubscribeTrades(stream TradeStream, handler func(Trade)) (*Subscription, error) {
	market, err := streamMarket(stream.Symbol)
	if err != nil {
		return nil, err
	}
	if err := validateNotNegative("trade backfill", stream.Backfill); err != nil {
		return nil, err
	}
	return subscribe(s, market+"@trade", stream.Backfill, handler)
}

func (s *StreamClient) SubscribeKlines(stream KlineStream, handler func(Kline)) (*Subscription, error) {
	channel, err := klineChannel(stream.Symbol, stream.Interval)
	if err != nil {
		return nil, err
	}
	return subscribe(s, channel, 0, handler)
}

func (s *StreamClient) SubscribeTicker(stream TickerStream, handler func(Ticker)) (*Subscription, error) {
	market, err := streamMarket(stream.Symbol)
	if err != nil {
		return nil, err
	}
	return subscribe(s, market+"@ticker", 0, handler)
}

func (s *StreamClient) SubscribeAllTickers(handler func([]Ticker)) (*Subscription, error) {
	return subscribe(s, allTickersChannel, 0, handler)
}

func (s *StreamClient) SubscribeOrderBook(stream OrderBookStream, handler func(OrderBook)) (*Subscription, error) {
	market, err := streamMarket(stream.Symbol)
	if err != nil {
		return nil, err
	}
	switch stream.Depth {
	case 5, 10, 20, 50, 100:
	default:
		return nil, invalidRequest("order book depth %d is not one of 5, 10, 20, 50 and 100", stream.Depth)
	}
	return subscribe(s, market+"@depth@"+strconv.Itoa(stream.Depth), 0, handler)
}

func subscribe[U any](s *StreamClient, channel string, backfill int, handler func(U)) (*Subscription, error) {
	if handler == nil {
		return nil, invalidRequest("a subscription needs a handler")
	}
	deliver := func(update any) bool {
		u, ok := update.(U)
		if ok {
			handler(u)
		}
		return ok
	}
	return s.add(channel, backfill, deliver), nil
}

func (s *StreamClient) add(channel string, backfill int, deliver func(update any) bool) *Subscription {
	sub := &Subscription{client: s, channel: channel, backfill: backfill, deliver: deliver}
	sub.active.Store(true)

	s.mu.Lock()
	defer s.mu.Unlock()
	group, exists := s.groups[channel]
	if !exists {
		group = &subscriptionGroup{channel: channel}
		s.groups[channel] = group
	}
	group.subscriptions = append(group.subscriptions, sub)
	if !exists && s.session != nil {

		_ = s.session.send(s.subscribeCommand(group))
	}
	return sub
}

func (s *StreamClient) remove(sub *Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	group := s.groups[sub.channel]
	if group == nil {
		return
	}
	group.subscriptions = slices.DeleteFunc(group.subscriptions, func(other *Subscription) bool {
		return other == sub
	})
	if len(group.subscriptions) == 0 {
		delete(s.groups, sub.channel)
		if s.session != nil {

			_ = s.session.send(s.unsubscribeCommand(sub.channel))
		}
	}
}

func (s *StreamClient) RequestKlines(ctx context.Context, req KlineHistoryRequest) ([]Kline, error) {
	channel, err := klineChannel(req.Symbol, req.Interval)
	if err != nil {
		return nil, err
	}
	if req.Limit < 0 || req.Limit > maxKlineHistory {
		return nil, invalidRequest("kline history limit must be between 0 and %d, got %d", maxKlineHistory, req.Limit)
	}
	param := klineRequestParam{Channel: channel, Limit: req.Limit}
	if !req.End.IsZero() {

		end := req.End.Unix()
		if req.End.Nanosecond() > 0 {
			end++
		}
		param.EndTime = &end
	}

	payload, err := s.request(ctx, klineRequest, channelRequestKey(channel), param.fits, func(id uint64) any {
		return requestCommand{Op: opRequest, Param: param, ID: id}
	})
	if err != nil {
		return nil, err
	}
	var series klineSeriesJSON
	if err := json.Unmarshal(payload, &series); err != nil {
		return nil, fmt.Errorf("coinstore: stream: decode kline history: %w", err)
	}
	klines := series.toKlines()
	if !req.End.IsZero() {

		klines = slices.DeleteFunc(klines, func(kline Kline) bool {
			return !kline.StartTime.Before(req.End)
		})
	}
	return klines, nil
}

func (s *StreamClient) ListSubscriptions(ctx context.Context) ([]ChannelInfo, error) {
	payload, err := s.request(ctx, listRequest, subscriptionListKey, nil, func(id uint64) any {
		return channelCommand{Op: opList, Channel: []string{}, ID: id}
	})
	if err != nil {
		return nil, err
	}
	var response struct {
		Subs []channelInfoJSON `json:"subs"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		return nil, fmt.Errorf("coinstore: stream: decode subscription list: %w", err)
	}
	return convertAll(response.Subs, channelInfoJSON.toChannelInfo), nil
}

func (s *StreamClient) request(ctx context.Context, kind requestKind, key requestKey, fits func(json.RawMessage) bool, newCommand func(id uint64) any) (json.RawMessage, error) {

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sess, err := s.awaitSession(ctx)
	if err != nil {
		return nil, err
	}
	leave, err := s.requests.enter(ctx, sess.closing, key)
	if errors.Is(err, errRequestAborted) {

		return nil, sess.refusalError()
	}
	if err != nil {
		return nil, err
	}

	id := s.lastCommandID.Add(1)
	pending, err := sess.addPending(kind, id, key.matchKey(), fits)
	if err != nil {
		leave()
		return nil, err
	}
	if err := sess.send(newCommand(id)); err != nil {
		sess.removePending(pending)
		leave()

		if refusal := sess.refusalError(); refusal != nil {
			return nil, fmt.Errorf("%w: send request: %v", refusal, err)
		}
		return nil, fmt.Errorf("coinstore: stream: send request: %w", err)
	}
	select {
	case result := <-pending.result:
		leave()

		if isClosed(pending.failed) {
			return nil, pending.failure
		}
		return result.payload, result.err
	case <-pending.failed:
		go s.drainAbandoned(sess, pending, leave)
		return nil, pending.failure
	case <-ctx.Done():
		go s.drainAbandoned(sess, pending, leave)
		return nil, ctx.Err()
	}
}

func (s *StreamClient) drainAbandoned(sess *streamSession, pending *pendingRequest, leave func()) {
	defer leave()
	timer := time.NewTimer(s.abandonedRequestTTL)
	defer timer.Stop()
	select {
	case <-pending.result:
	case <-timer.C:
		sess.removePending(pending)
	}
}

func streamMarket(symbol string) (string, error) {
	market := strings.ToLower(strings.TrimSpace(symbol))
	if market == "" {
		return "", invalidRequest("symbol is required")
	}
	if strings.ContainsAny(market, "@! \t\r\n") {
		return "", invalidRequest("symbol %q contains characters that channel names reserve", symbol)
	}
	if namesPairByID(market) {
		market = strings.TrimLeft(market, "0")
		if market == "" {
			market = "0"
		}
	}
	return market, nil
}

func klineChannel(symbol string, interval KlineInterval) (string, error) {
	market, err := streamMarket(symbol)
	if err != nil {
		return "", err
	}
	if !interval.isStreamable() {
		return "", invalidRequest("kline interval %q is not available from the WebSocket API", interval)
	}
	return market + "@kline@" + string(interval), nil
}

type channelInfoJSON struct {
	Name string `json:"name"`
	Type struct {
		Name string `json:"name"`
	} `json:"type"`
	LifeStartTime flexjson.Int64 `json:"lifeStartTime"`
	MsgCount      flexjson.Int64 `json:"msgCount"`
}

func (j channelInfoJSON) toChannelInfo() ChannelInfo {
	return ChannelInfo{
		Channel:      j.Name,
		Type:         j.Type.Name,
		SubscribedAt: timeFromUnix(j.LifeStartTime),
		MessageCount: int64(j.MsgCount),
	}
}
