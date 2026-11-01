package coinstore

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestKeyConflicts(t *testing.T) {
	var names pairNames
	names.learn("", "BTCUSDT", 4)
	tests := []struct {
		a, b requestKey
		want bool
	}{
		{channelRequestKey("4@kline@day_1"), channelRequestKey("4@kline@day_1"), true},
		{channelRequestKey("4@kline@day_1"), channelRequestKey("btcusdt@kline@day_1"), true},
		{channelRequestKey("btcusdt@kline@day_1"), channelRequestKey("4@kline@day_1"), true},
		{channelRequestKey("4@kline@day_1"), channelRequestKey("5@kline@day_1"), false},
		{channelRequestKey("btcusdt@kline@day_1"), channelRequestKey("5@kline@day_1"), false},
		{channelRequestKey("ethusdt@kline@day_1"), channelRequestKey("4@kline@day_1"), false},

		{channelRequestKey("ethusdt@kline@day_1"), channelRequestKey("5@kline@day_1"), true},
		{channelRequestKey("4@kline@day_1"), channelRequestKey("btcusdt@kline@min_1"), false},
		{subscriptionListKey, subscriptionListKey, true},
		{subscriptionListKey, channelRequestKey("4@kline@day_1"), false},
	}
	for _, tt := range tests {
		if got := tt.a.conflicts(tt.b, &names); got != tt.want {
			t.Errorf("%+v.conflicts(%+v) = %t, want %t", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestRequestKeyMatchKey(t *testing.T) {
	if got := channelRequestKey("4@kline@day_1").matchKey(); got != "4@kline@day_1" {
		t.Errorf("matchKey of a kline request = %q, want its channel", got)
	}
	if got := subscriptionListKey.matchKey(); got != listRequestKey {
		t.Errorf("matchKey of a list request = %q, want %q", got, listRequestKey)
	}
}

func enterAsync(t *testing.T, rs *requestScheduler, key requestKey) <-chan func() {
	t.Helper()
	admitted := make(chan func(), 1)
	go func() {
		leave, err := rs.enter(t.Context(), nil, key)
		if err == nil {
			admitted <- leave
		}
	}()
	return admitted
}

func newTestScheduler() *requestScheduler {
	names := &pairNames{}
	names.learn("", "BTCUSDT", 4)
	names.learn("", "ETHUSDT", 5)
	return &requestScheduler{names: names}
}

func expectSchedulerIdle(t *testing.T, rs *requestScheduler) {
	t.Helper()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if len(rs.outstanding) != 0 || len(rs.waiting) != 0 {
		t.Errorf("scheduler holds %d outstanding and %d waiting requests, want none", len(rs.outstanding), len(rs.waiting))
	}
}

func TestRequestSchedulerAdmitsInOrderOfConflicts(t *testing.T) {
	rs := newTestScheduler()
	byID, err := rs.enter(context.Background(), nil, channelRequestKey("4@kline@day_1"))
	if err != nil {
		t.Fatalf("enter returned error: %v", err)
	}

	bySymbol := enterAsync(t, rs, channelRequestKey("btcusdt@kline@day_1"))
	expectNothing(t, bySymbol)
	byIDAgain := enterAsync(t, rs, channelRequestKey("4@kline@day_1"))
	expectNothing(t, byIDAgain)

	receive(t, enterAsync(t, rs, channelRequestKey("5@kline@day_1")))()
	receive(t, enterAsync(t, rs, channelRequestKey("ethusdt@kline@day_1")))()
	receive(t, enterAsync(t, rs, channelRequestKey("4@kline@min_1")))()

	byID()

	leaveBySymbol := receive(t, bySymbol)
	expectNothing(t, byIDAgain)
	leaveBySymbol()
	receive(t, byIDAgain)()
	expectSchedulerIdle(t, rs)
}

func TestRequestSchedulerAdmitsConcurrentRequestsForDistinctPairs(t *testing.T) {
	rs := newTestScheduler()
	keys := []requestKey{
		channelRequestKey("4@kline@day_1"),
		channelRequestKey("ethusdt@kline@day_1"),
		channelRequestKey("6@kline@day_1"),
		channelRequestKey("4@kline@hour_1"),
		subscriptionListKey,
	}
	for _, key := range keys {
		if _, err := rs.enter(context.Background(), nil, key); err != nil {
			t.Fatalf("enter(%+v) returned error: %v", key, err)
		}
	}
	if len(rs.outstanding) != len(keys) {
		t.Errorf("scheduler admitted %d requests, want %d", len(rs.outstanding), len(keys))
	}
}

func TestRequestSchedulerHoldsBackRequestsItCannotTellApart(t *testing.T) {
	rs := newTestScheduler()
	leave, err := rs.enter(context.Background(), nil, channelRequestKey("6@kline@day_1"))
	if err != nil {
		t.Fatalf("enter returned error: %v", err)
	}

	bySymbol := enterAsync(t, rs, channelRequestKey("xrpusdt@kline@day_1"))
	expectNothing(t, bySymbol)
	later := enterAsync(t, rs, channelRequestKey("8@kline@day_1"))
	expectNothing(t, later)
	rs.reconsider()
	expectNothing(t, bySymbol)

	rs.names.learn("", "XRPUSDT", 7)
	rs.reconsider()
	receive(t, bySymbol)()
	receive(t, later)()
	leave()
	expectSchedulerIdle(t, rs)
}

func TestRequestSchedulerReleasesRequestsThatGiveUp(t *testing.T) {
	rs := newTestScheduler()
	leave, err := rs.enter(context.Background(), nil, channelRequestKey("4@kline@day_1"))
	if err != nil {
		t.Fatalf("enter returned error: %v", err)
	}

	t.Run("context done", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if _, err := rs.enter(ctx, nil, channelRequestKey("btcusdt@kline@day_1")); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("enter returned %v, want context.DeadlineExceeded", err)
		}
	})
	t.Run("aborted", func(t *testing.T) {
		abort := make(chan struct{})
		close(abort)
		if _, err := rs.enter(context.Background(), abort, channelRequestKey("4@kline@day_1")); !errors.Is(err, errRequestAborted) {
			t.Errorf("enter returned %v, want errRequestAborted", err)
		}
	})

	waiting := enterAsync(t, rs, channelRequestKey("btcusdt@kline@day_1"))
	leave()
	leave()
	receive(t, waiting)()
	expectSchedulerIdle(t, rs)
}

func TestRequestSchedulerNeverAdmitsRequestsThatGaveUp(t *testing.T) {
	rs := newTestScheduler()
	done, cancel := context.WithCancel(context.Background())
	cancel()
	aborted := make(chan struct{})
	close(aborted)
	key := channelRequestKey("4@kline@day_1")

	for range 100 {
		if _, err := rs.enter(done, nil, key); !errors.Is(err, context.Canceled) {
			t.Fatalf("enter with a done context returned %v, want context.Canceled", err)
		}
		if _, err := rs.enter(context.Background(), aborted, key); !errors.Is(err, errRequestAborted) {
			t.Fatalf("enter with a closed abort channel returned %v, want errRequestAborted", err)
		}
		expectSchedulerIdle(t, rs)
	}
}

func TestRequestSchedulerNeverAdmitsConflictingRequestsTogether(t *testing.T) {
	rs := newTestScheduler()
	keys := []requestKey{
		channelRequestKey("4@kline@day_1"),
		channelRequestKey("btcusdt@kline@day_1"),
		channelRequestKey("5@kline@day_1"),
		channelRequestKey("ethusdt@kline@day_1"),
		channelRequestKey("6@kline@day_1"),
		channelRequestKey("xrpusdt@kline@day_1"),
		channelRequestKey("4@kline@min_1"),
		subscriptionListKey,
	}
	abort := make(chan struct{})
	var (
		mu        sync.Mutex
		held      = make(map[requestKey]int)
		conflicts int
		wg        sync.WaitGroup
	)
	for worker := range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			random := rand.New(rand.NewPCG(uint64(worker), 0))
			for i := range 300 {
				key := keys[random.IntN(len(keys))]
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(random.IntN(2000))*time.Microsecond)
				var abortCh <-chan struct{}
				if i%5 == 0 {
					abortCh = abort
				}
				leave, err := rs.enter(ctx, abortCh, key)
				cancel()
				if err != nil {
					continue
				}
				mu.Lock()
				for other, n := range held {
					if n > 0 && key.conflicts(other, rs.names) {
						conflicts++
					}
				}
				held[key]++
				mu.Unlock()
				time.Sleep(time.Duration(random.IntN(100)) * time.Microsecond)
				mu.Lock()
				held[key]--
				mu.Unlock()
				leave()
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(abort)
	wg.Wait()

	if conflicts != 0 {
		t.Errorf("admitted %d requests while a conflicting one was outstanding", conflicts)
	}
	expectSchedulerIdle(t, rs)
}

func TestRequestsWhoseContextIsDoneAreNotSent(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for range 50 {
		if _, err := client.RequestKlines(ctx, KlineHistoryRequest{Symbol: "4", Interval: KlineInterval1Day}); !errors.Is(err, context.Canceled) {
			t.Fatalf("RequestKlines returned %v, want context.Canceled", err)
		}
		if _, err := client.ListSubscriptions(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("ListSubscriptions returned %v, want context.Canceled", err)
		}
	}
	conn.expectNoCommand(t)
}

func matchNoneKnown(keys ...string) replyMatch {
	return newReplyMatch(keys, func(string) bool { return false })
}

func addPendingKlines(t *testing.T, ss *streamSession, keys ...string) []*pendingRequest {
	t.Helper()
	requests := make([]*pendingRequest, len(keys))
	for i, key := range keys {
		pending, err := ss.addPending(klineRequest, uint64(i+1), key, nil)
		if err != nil {
			t.Fatalf("addPending returned error: %v", err)
		}
		requests[i] = pending
	}
	return requests
}

func expectNoResult(t *testing.T, requests ...*pendingRequest) {
	t.Helper()
	for _, request := range requests {
		if len(request.result) != 0 {
			t.Errorf("the request for %s received a result", request.key)
		}
	}
}

func expectFailed(t *testing.T, want error, requests ...*pendingRequest) {
	t.Helper()
	for _, request := range requests {
		if !isClosed(request.failed) || !errors.Is(request.failure, want) {
			t.Errorf("the request for %s failed with %v, want %v", request.key, request.failure, want)
		}
	}
}

func expectNotFailed(t *testing.T, requests ...*pendingRequest) {
	t.Helper()
	for _, request := range requests {
		if isClosed(request.failed) {
			t.Errorf("the reply failed the request for %s with %v", request.key, request.failure)
		}
	}
}

func TestResolveGivesAReplyToTheOneRequestItCouldBelongTo(t *testing.T) {
	ss := newStreamSession(nil)
	requests := addPendingKlines(t, ss, "4@kline@day_1", "5@kline@day_1")

	got, _ := ss.resolve(klineRequest, 0, matchNoneKnown("5@kline@day_1"), json.RawMessage(`{"reply":1}`))

	if got != resolved {
		t.Fatalf("resolve = %d, want resolved", got)
	}
	if got := receive(t, requests[1].result); string(got.payload) != `{"reply":1}` {
		t.Errorf("the request received %s", got.payload)
	}
	expectNoResult(t, requests[0])
}

func TestResolveSkipsRequestsTheReplyDoesNotFit(t *testing.T) {
	ss := newStreamSession(nil)
	misfitting, err := ss.addPending(klineRequest, 1, "4@kline@day_1", func(json.RawMessage) bool { return false })
	if err != nil {
		t.Fatalf("addPending returned error: %v", err)
	}
	fitting, err := ss.addPending(klineRequest, 2, "btcusdt@kline@day_1", nil)
	if err != nil {
		t.Fatalf("addPending returned error: %v", err)
	}

	got, _ := ss.resolve(klineRequest, 0, matchNoneKnown("4@kline@day_1", "btcusdt@kline@day_1"), json.RawMessage(`{}`))

	if got != resolved {
		t.Fatalf("resolve = %d, want resolved", got)
	}
	receive(t, fitting.result)
	expectNoResult(t, misfitting)

	if _, err := ss.addPending(klineRequest, 3, "ethusdt@kline@day_1", nil); err != nil {
		t.Fatalf("addPending returned error: %v", err)
	}
	if got, _ := ss.resolve(klineRequest, 0, matchNoneKnown("4@kline@day_1"), json.RawMessage(`{}`)); got != misfit {
		t.Errorf("resolve of a reply fitting no request it names = %d, want misfit", got)
	}
}

func TestResolveFailsRequestsAReplyCouldEquallyAnswer(t *testing.T) {
	ss := newStreamSession(nil)
	requests := addPendingKlines(t, ss, "4@kline@day_1", "btcusdt@kline@day_1")

	got, keys := ss.resolve(klineRequest, 0, matchNoneKnown("4@kline@day_1", "btcusdt@kline@day_1"), json.RawMessage(`{}`))

	if got != ambiguous {
		t.Fatalf("resolve = %d, want ambiguous", got)
	}
	assertEqual(t, keys, []string{"4@kline@day_1", "btcusdt@kline@day_1"})
	expectFailed(t, ErrAmbiguousReply, requests...)
	expectNoResult(t, requests...)

	for _, channel := range []string{"4@kline@day_1", "btcusdt@kline@day_1"} {
		if got, _ := ss.resolve(klineRequest, 0, matchNoneKnown(channel), json.RawMessage(`{}`)); got != ambiguous {
			t.Errorf("resolve of a later reply for %s = %d, want ambiguous", channel, got)
		}
	}
	expectNoResult(t, requests...)
}

func TestResolveCannotRuleOutRequestsUnderUnplacedNames(t *testing.T) {
	ss := newStreamSession(nil)
	requests := addPendingKlines(t, ss, "4@kline@day_1", "btcusdt@kline@day_1")

	got, keys := ss.resolve(klineRequest, 0, matchNoneKnown("btcusdt@kline@day_1"), json.RawMessage(`{}`))

	if got != ambiguous {
		t.Fatalf("resolve = %d, want ambiguous", got)
	}
	assertEqual(t, keys, []string{"4@kline@day_1", "btcusdt@kline@day_1"})
	expectFailed(t, ErrAmbiguousReply, requests...)
	expectNoResult(t, requests...)
}

func TestResolveRulesOutRequestsUnderOtherNames(t *testing.T) {
	ss := newStreamSession(nil)
	requests := addPendingKlines(t, ss, "4@kline@day_1", "5@kline@day_1", "btcusdt@kline@day_1")
	placed := func(pair string) bool { return pair == "4" }

	got, _ := ss.resolve(klineRequest, 0, newReplyMatch([]string{"7@kline@day_1", "btcusdt@kline@day_1"}, placed), json.RawMessage(`{}`))

	if got != resolved {
		t.Fatalf("resolve = %d, want resolved", got)
	}
	receive(t, requests[2].result)
	expectNotFailed(t, requests[:2]...)
	expectNoResult(t, requests[:2]...)
}

func TestResolveNeverGivesAReplyToARequestItDoesNotName(t *testing.T) {
	ss := newStreamSession(nil)
	requests := addPendingKlines(t, ss, "4@kline@day_1", "5@kline@day_1", "ethusdt@kline@day_1")
	placed := func(pair string) bool { return pair == "ethusdt" }

	got, uncertain := ss.resolve(klineRequest, 0, newReplyMatch([]string{"btcusdt@kline@day_1"}, placed), json.RawMessage(`{}`))

	if got != unconfirmed {
		t.Errorf("resolve = %d, want unconfirmed", got)
	}
	assertEqual(t, uncertain, []string{"4@kline@day_1", "5@kline@day_1"})
	expectFailed(t, ErrUnconfirmedReply, requests[:2]...)
	expectNotFailed(t, requests[2])
	expectNoResult(t, requests...)
}

func TestResolveGivesRepliesOnlyToRequestsOfTheirKind(t *testing.T) {
	ss := newStreamSession(nil)
	list, err := ss.addPending(listRequest, 1, listRequestKey, nil)
	if err != nil {
		t.Fatalf("addPending returned error: %v", err)
	}

	for _, id := range []uint64{1, 0} {
		if got, _ := ss.resolve(klineRequest, id, matchNoneKnown(listRequestKey), json.RawMessage(`{}`)); got != unmatched {
			t.Errorf("resolve of a kline reply with ID %d = %d, want unmatched", id, got)
		}
	}
	expectNoResult(t, list)
}

func pairReply(start int64) string {
	return `{"channel":"4@kline@day_1","symbol":"BTCUSDT","instrumentId":4,"item":[{"interval":"day_1","startTime":` +
		strconv.FormatInt(start, 10) + `}]}`
}

func symbolOnlyReply(start int64) string {
	return `{"channel":"btcusdt@kline@day_1","symbol":"BTCUSDT","item":[{"interval":"day_1","startTime":` +
		strconv.FormatInt(start, 10) + `}]}`
}

type klineResult struct {
	klines []Kline
	err    error
}

func requestDailyKlines(client *StreamClient, symbol string, end int64) <-chan klineResult {
	return requestDailyKlinesWithin(client, symbol, end, testWait)
}

func requestDailyKlinesWithin(client *StreamClient, symbol string, end int64, timeout time.Duration) <-chan klineResult {
	results := make(chan klineResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		klines, err := client.RequestKlines(ctx, KlineHistoryRequest{Symbol: symbol, Interval: KlineInterval1Day, End: time.Unix(end, 0)})
		results <- klineResult{klines, err}
	}()
	return results
}

func expectKlinesFrom(t *testing.T, results <-chan klineResult, start int64) {
	t.Helper()
	got := receive(t, results)
	if got.err != nil || len(got.klines) != 1 || got.klines[0].StartTime.Unix() != start {
		t.Errorf("RequestKlines returned %+v, %v, want one kline from %d", got.klines, got.err, start)
	}
}

func expectRequestFor(t *testing.T, conn *fakeStreamConn, channel string) {
	t.Helper()
	if command := conn.expectCommand(t); command.Op != opRequest || !strings.Contains(string(command.Param), `"`+channel+`"`) {
		t.Fatalf("client sent %s, want a request for %s", command.raw, channel)
	}
}

func TestRequestKlinesForOnePairUnderBothNamesOnceTheNamesAreKnown(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)

	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1,"symbol":"BTCUSDT","instrumentId":4}`)
	syncReads(t, client, conn)

	byID := requestDailyKlines(client, "4", 10000)
	expectRequestFor(t, conn, "4@kline@day_1")
	bySymbol := requestDailyKlines(client, "btcusdt", 20000)
	conn.expectNoCommand(t)

	conn.send(t, pairReply(9000))
	expectKlinesFrom(t, byID, 9000)
	expectRequestFor(t, conn, "btcusdt@kline@day_1")
	conn.send(t, pairReply(19000))
	expectKlinesFrom(t, bySymbol, 19000)
}

func TestRequestKlinesForOnePairUnderBothNamesBeforeTheNamesAreKnown(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	runStream(t, client)
	conn := srv.accept(t)

	byID := requestDailyKlines(client, "4", 10000)
	expectRequestFor(t, conn, "4@kline@day_1")
	bySymbol := requestDailyKlines(client, "btcusdt", 20000)
	conn.expectNoCommand(t)

	conn.send(t, pairReply(9000))
	expectKlinesFrom(t, byID, 9000)
	expectRequestFor(t, conn, "btcusdt@kline@day_1")
	conn.send(t, symbolOnlyReply(19000))
	expectKlinesFrom(t, bySymbol, 19000)
	errs.expectNone(t)
}

func TestRequestKlinesUnderOtherNamesGoOutOnceThePairsAreToldApart(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)

	byID := requestDailyKlines(client, "5", 10000)
	expectRequestFor(t, conn, "5@kline@day_1")
	bySymbol := requestDailyKlines(client, "btcusdt", 20000)
	conn.expectNoCommand(t)

	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1,"symbol":"BTCUSDT"}`)
	expectRequestFor(t, conn, "btcusdt@kline@day_1")
	conn.send(t, pairReply(19000))
	expectKlinesFrom(t, bySymbol, 19000)
	conn.send(t, klineReply("5@kline@day_1", 9000))
	expectKlinesFrom(t, byID, 9000)
}

func TestRequestKlinesFailsRequestsAContradictingReplyCouldAnswer(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	client.abandonedRequestTTL = 200 * time.Millisecond
	runStream(t, client)
	conn := srv.accept(t)
	conn.send(t, `{"T":"trade","channel":"4@trade","tradeId":1,"symbol":"BTCUSDT"}`)
	syncReads(t, client, conn)

	byID := requestDailyKlines(client, "4", 20000)
	expectRequestFor(t, conn, "4@kline@day_1")
	bySymbol := requestDailyKlines(client, "ethusdt", 30000)
	expectRequestFor(t, conn, "ethusdt@kline@day_1")

	contradicting := func(start int64) string {
		return `{"channel":"4@kline@day_1","symbol":"ETHUSDT","instrumentId":4,"item":[{"interval":"day_1","startTime":` +
			strconv.FormatInt(start, 10) + `}]}`
	}
	conn.send(t, contradicting(19000))
	for _, results := range []<-chan klineResult{byID, bySymbol} {
		if got := receive(t, results); !errors.Is(got.err, ErrAmbiguousReply) {
			t.Errorf("RequestKlines returned %+v, %v, want ErrAmbiguousReply", got.klines, got.err)
		}
	}
	reportsAmbiguity := func(keys string) func(error) bool {
		return func(err error) bool {
			return err.Error() == "coinstore: stream: discarded a kline history reply for 4@kline@day_1 that could answer several requests, "+
				"or one that an ambiguous reply failed: "+keys
		}
	}
	errs.expect(t, reportsAmbiguity("4@kline@day_1, ethusdt@kline@day_1"))

	retry := requestDailyKlines(client, "4", 20000)
	conn.expectNoCommand(t)
	conn.send(t, contradicting(29000))
	errs.expect(t, reportsAmbiguity("ethusdt@kline@day_1"))
	expectRequestFor(t, conn, "4@kline@day_1")
	conn.send(t, contradicting(19000))
	expectKlinesFrom(t, retry, 19000)
}

func TestRequestKlinesFailsRequestsThatAReplyNamingTheirPairAnotherWayMayAnswer(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	client.abandonedRequestTTL = 200 * time.Millisecond
	runStream(t, client)
	conn := srv.accept(t)

	byID := requestDailyKlines(client, "4", 20000)
	expectRequestFor(t, conn, "4@kline@day_1")

	conn.send(t, symbolOnlyReply(19999))
	if got := receive(t, byID); !errors.Is(got.err, ErrUnconfirmedReply) {
		t.Errorf("RequestKlines returned %+v, %v, want ErrUnconfirmedReply", got.klines, got.err)
	}
	errs.expect(t, func(err error) bool {
		return err.Error() == "coinstore: stream: discarded a kline history reply for btcusdt@kline@day_1 that names no pending request "+
			"but may answer one under a name the client cannot relate to the reply's pair: 4@kline@day_1"
	})

	retry := requestDailyKlines(client, "4", 20000)
	conn.expectNoCommand(t)
	expectRequestFor(t, conn, "4@kline@day_1")
	conn.send(t, klineReply("4@kline@day_1", 19999))
	expectKlinesFrom(t, retry, 19999)
}

func TestRequestKlinesForDistinctPairsAreConcurrent(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)
	results := make(chan []Kline, 2)
	for _, symbol := range []string{"4", "5"} {
		go func() {
			klines, err := client.RequestKlines(context.Background(), KlineHistoryRequest{Symbol: symbol, Interval: KlineInterval1Day})
			if err != nil {
				t.Errorf("RequestKlines(%s) returned error: %v", symbol, err)
			}
			results <- klines
		}()
	}

	conn.expectCommand(t)
	conn.expectCommand(t)
	conn.send(t, klineReply("5@kline@day_1", 500))
	conn.send(t, klineReply("4@kline@day_1", 400))

	starts := map[int64]bool{}
	for range 2 {
		klines := receive(t, results)
		if len(klines) == 1 {
			starts[klines[0].StartTime.Unix()] = true
		}
	}
	if !starts[400] || !starts[500] {
		t.Errorf("requests received klines starting at %v, want 400 and 500", starts)
	}
}

func TestRequestKlinesExcludesTheKlineAtEnd(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	runStream(t, client)
	conn := srv.accept(t)
	results := make(chan []Kline, 1)
	go func() {
		klines, err := client.RequestKlines(context.Background(), KlineHistoryRequest{
			Symbol: "4", Interval: KlineInterval1Day, End: time.Unix(864000, 0),
		})
		if err != nil {
			t.Errorf("RequestKlines returned error: %v", err)
		}
		results <- klines
	}()
	conn.expectCommand(t)

	conn.send(t, klineReply("4@kline@day_1", 777600, 864000))

	klines := receive(t, results)
	if len(klines) != 1 || klines[0].StartTime.Unix() != 777600 {
		t.Errorf("RequestKlines returned %+v, want only the kline before End", klines)
	}
	errs.expectNone(t)
}

func TestRequestKlinesRoundsFractionalEndUp(t *testing.T) {
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)
	results := make(chan []Kline, 1)
	go func() {
		klines, err := client.RequestKlines(context.Background(), KlineHistoryRequest{
			Symbol: "4", Interval: KlineInterval1Minute, End: time.Unix(1020, 500_000_000),
		})
		if err != nil {
			t.Errorf("RequestKlines returned error: %v", err)
		}
		results <- klines
	}()

	if command := conn.expectCommand(t); !strings.Contains(string(command.Param), `"endTime":1021`) {
		t.Fatalf("client sent %s, want endTime 1021", command.raw)
	}
	conn.send(t, `{"channel":"4@kline@min_1","item":[{"interval":"min_1","startTime":960},{"interval":"min_1","startTime":1020}]}`)
	if klines := receive(t, results); len(klines) != 2 {
		t.Errorf("RequestKlines returned %+v, want the klines starting before End, including the one at 1020", klines)
	}
}

func TestStreamReportsRepliesThatAnswerNoRequest(t *testing.T) {
	srv := newFakeStreamServer(t)
	errs := newErrorCollector()
	client := newTestStreamClient(t, srv, WithStreamErrorHandler(errs.handle))
	runStream(t, client)
	conn := srv.accept(t)

	for reply, want := range map[string]string{
		klineReply("4@kline@day_1", 100): "discarded a kline history reply for 4@kline@day_1 that answers no pending request",
		`{"id":99,"item":[]}`:            "discarded a kline history reply that answers no pending request",
		`{"item":[]}`:                    "discarded a kline history reply that names neither its request nor its channel",
		`{"T":"resp","subs":[]}`:         "discarded a subscription list that answers no pending request",
	} {
		conn.send(t, reply)
		errs.expect(t, func(err error) bool { return err.Error() == "coinstore: stream: "+want })
	}
}

var klineReplyFormats = map[string]func(channel, pairID, symbol string, start int64) string{

	"echo": func(channel, _, _ string, start int64) string {
		return klineReply(channel, start)
	},

	"echo with symbol": func(channel, _, symbol string, start int64) string {
		return `{"channel":"` + channel + `","symbol":"` + symbol + `","item":[{"interval":"day_1","startTime":` +
			strconv.FormatInt(start, 10) + `}]}`
	},

	"documented": func(channel, pairID, symbol string, start int64) string {
		_, topic, _ := strings.Cut(channel, "@")
		return `{"channel":"` + pairID + "@" + topic + `","symbol":"` + symbol + `","instrumentId":` + pairID +
			`,"item":[{"interval":"day_1","startTime":` + strconv.FormatInt(start, 10) + `}]}`
	},
}

type shuffledServerRun struct {
	formats []string

	updates bool

	impatient bool
}

func TestRequestKlinesReceiveTheirOwnReplies(t *testing.T) {
	allFormats := []string{"echo", "echo with symbol", "documented"}
	for name, run := range map[string]shuffledServerRun{
		"echo":                       {formats: []string{"echo"}},
		"echo with symbol":           {formats: []string{"echo with symbol"}},
		"documented":                 {formats: []string{"documented"}},
		"mixed with updates":         {formats: allFormats, updates: true},
		"mixed with late replies":    {formats: allFormats, impatient: true},
		"echo with late replies":     {formats: []string{"echo"}, impatient: true},
		"mixed with updates and all": {formats: allFormats, updates: true, impatient: true},
	} {
		t.Run(name, func(t *testing.T) {
			for seed := range uint64(3) {
				requestKlinesFromShuffledServer(t, seed, run)
			}
		})
	}
}

func requestKlinesFromShuffledServer(t *testing.T, seed uint64, run shuffledServerRun) {
	t.Helper()
	srv := newFakeStreamServer(t)
	client := newTestStreamClient(t, srv)
	runStream(t, client)
	conn := srv.accept(t)

	stop := make(chan struct{})
	served := make(chan error, 1)
	go func() { served <- serveShuffled(conn, rand.New(rand.NewPCG(seed, 0)), run, stop) }()

	names := []string{"4", "btcusdt", "5", "ethusdt"}
	var lastEnd atomic.Int64
	lastEnd.Store(100000)
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			random := rand.New(rand.NewPCG(seed, uint64(worker)+1))
			for range 12 {
				name := names[random.IntN(len(names))]
				end := lastEnd.Add(10)
				timeout := testWait
				if run.impatient && random.IntN(4) == 0 {
					timeout = time.Millisecond
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				klines, err := client.RequestKlines(ctx, KlineHistoryRequest{Symbol: name, Interval: KlineInterval1Day, End: time.Unix(end, 0)})
				cancel()
				ownKlines := err == nil && len(klines) == 1 && klines[0].StartTime.Unix() == end-1
				gaveUp := timeout < testWait && errors.Is(err, context.DeadlineExceeded)
				if !ownKlines && !gaveUp {
					t.Errorf("seed %d: RequestKlines(%s, End %d) returned %+v, %v, want its own kline, starting at %d",
						seed, name, end, klines, err, end-1)
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	if err := <-served; err != nil {
		t.Errorf("seed %d: the server failed: %v", seed, err)
	}
}

func serveShuffled(conn *fakeStreamConn, random *rand.Rand, run shuffledServerRun, stop <-chan struct{}) error {
	pairs := map[string][2]string{
		"4": {"4", "BTCUSDT"}, "btcusdt": {"4", "BTCUSDT"},
		"5": {"5", "ETHUSDT"}, "ethusdt": {"5", "ETHUSDT"},
	}
	ids := []string{"4", "5"}
	var outstanding []klineRequestParam
	for {
		select {
		case <-stop:
			return nil
		case payload := <-conn.commands:
			var command requestCommand
			if err := json.Unmarshal(payload, &command); err != nil {
				return err
			}
			if command.Op == opRequest {
				outstanding = append(outstanding, command.Param)
			}
		case <-time.After(time.Duration(random.IntN(3)) * time.Millisecond):
			if run.updates && random.IntN(30) == 0 {
				pair := pairs[ids[random.IntN(len(ids))]]
				if err := conn.write(`{"T":"trade","channel":"` + pair[0] + `@trade","tradeId":1,"symbol":"` + pair[1] + `"}`); err != nil {
					return err
				}
			}
			if len(outstanding) == 0 {
				continue
			}
			i := random.IntN(len(outstanding))
			request := outstanding[i]
			outstanding = slices.Delete(outstanding, i, i+1)
			name, _, _ := strings.Cut(request.Channel, "@")
			pair := pairs[name]
			format := klineReplyFormats[run.formats[random.IntN(len(run.formats))]]
			if err := conn.write(format(request.Channel, pair[0], pair[1], *request.EndTime-1)); err != nil {
				return err
			}
		}
	}
}
