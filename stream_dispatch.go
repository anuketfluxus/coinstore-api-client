package coinstore

import (
	"fmt"
	"maps"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const (
	defaultMaxBacklogUpdates = 16384
	defaultMaxBacklogErrors  = 256
)

type BacklogOverflowError struct {
	DroppedUpdates map[string]int

	DroppedErrors int
}

func (e *BacklogOverflowError) Error() string {
	var losses []string
	for _, channel := range slices.Sorted(maps.Keys(e.DroppedUpdates)) {
		losses = append(losses, countOf(e.DroppedUpdates[channel], "update")+" of "+channel)
	}
	if e.DroppedErrors > 0 {
		losses = append(losses, countOf(e.DroppedErrors, "error"))
	}
	return "coinstore: stream: handlers fell behind, so the client discarded " + strings.Join(losses, ", ")
}

func countOf(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}

type HandlerPanic struct {
	Value any

	Stack []byte
}

func (p HandlerPanic) String() string {
	return fmt.Sprintf("coinstore: stream: handler panicked: %v\n\n%s", p.Value, p.Stack)
}

type backlogLimits struct {
	updates int
	errors  int
}

type dispatcher struct {
	deliver func(channel string, update any)

	onError func(error)

	onFailure func()

	mu      sync.Mutex
	backlog handlerBacklog
	halted  bool
	failure *handlerFailure

	wake chan struct{}

	quit chan struct{}
	done chan struct{}
}

type handlerFailure struct {
	exited bool
	value  any
	stack  []byte
}

func startDispatcher(limits backlogLimits, deliver func(channel string, update any), onError func(error), onFailure func()) *dispatcher {
	d := &dispatcher{
		deliver:   deliver,
		onError:   onError,
		onFailure: onFailure,
		backlog:   newHandlerBacklog(limits, onError != nil),
		wake:      make(chan struct{}, 1),
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go d.run()
	return d
}

func (d *dispatcher) beginConnection(onConnect func()) {
	d.mu.Lock()
	if !d.halted {
		d.backlog.beginConnection(onConnect)
	}
	d.mu.Unlock()
	d.signal()
}

func (d *dispatcher) reportError(err error) {
	if d.onError == nil {
		return
	}
	d.mu.Lock()
	if !d.halted {
		d.backlog.pushError(err)
	}
	d.mu.Unlock()
	d.signal()
}

func (d *dispatcher) dispatchUpdate(channel string, update any) {
	d.mu.Lock()
	if !d.halted {
		d.backlog.pushUpdate(channel, update)
	}
	d.mu.Unlock()
	d.signal()
}

func (d *dispatcher) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *dispatcher) halt() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.haltLocked()
}

func (d *dispatcher) haltLocked() {
	if d.halted {
		return
	}
	d.halted = true
	d.backlog.discard()
	close(d.quit)
}

func (d *dispatcher) wait() {
	<-d.done
}

func (d *dispatcher) reraise() {
	d.mu.Lock()
	failure := d.failure
	d.mu.Unlock()
	switch {
	case failure == nil:
	case failure.exited:
		runtime.Goexit()
	default:
		panic(HandlerPanic{Value: failure.value, Stack: failure.stack})
	}
}

func (d *dispatcher) run() {
	defer close(d.done)
	for {
		entry, ok := d.next()
		if !ok || !d.execute(entry) {
			return
		}
	}
}

func (d *dispatcher) next() (*backlogEntry, bool) {
	for {
		d.mu.Lock()
		if d.halted {
			d.mu.Unlock()
			return nil, false
		}
		entry, ok := d.backlog.pop()
		d.mu.Unlock()
		if ok {
			return entry, true
		}
		select {
		case <-d.wake:
		case <-d.quit:
		}
	}
}

func (d *dispatcher) execute(entry *backlogEntry) (returned bool) {
	defer func() {
		if !returned {
			d.fail(recover())
		}
	}()
	switch entry.kind {
	case connectEntry:
		entry.call()
	case errorEntry:
		d.onError(entry.err)
	case updateEntry:
		d.deliver(entry.channel, entry.update)
	case overflowEntry:
		d.onError(entry.overflow)
	}
	return true
}

func (d *dispatcher) fail(recovered any) {
	failure := &handlerFailure{exited: recovered == nil, value: recovered, stack: debug.Stack()}
	d.mu.Lock()
	d.failure = failure
	d.haltLocked()
	d.mu.Unlock()
	d.onFailure()
}

type entryKind int

const (
	connectEntry entryKind = iota

	errorEntry

	updateEntry

	overflowEntry
)

type backlogEntry struct {
	kind entryKind

	call func()

	err error

	channel string
	update  any
	stream  string

	tickerPositions map[string]int

	overflow *BacklogOverflowError
}

type handlerBacklog struct {
	limits backlogLimits

	reportsOverflow bool

	entries []*backlogEntry

	streams map[string]*backlogEntry

	updates int
	errors  int
}

func newHandlerBacklog(limits backlogLimits, reportsOverflow bool) handlerBacklog {
	return handlerBacklog{
		limits:          limits,
		reportsOverflow: reportsOverflow,
		streams:         make(map[string]*backlogEntry),
	}
}

func (b *handlerBacklog) beginConnection(onConnect func()) {
	clear(b.streams)
	if onConnect != nil {
		b.entries = append(b.entries, &backlogEntry{kind: connectEntry, call: onConnect})
	}
}

func (b *handlerBacklog) pushError(err error) {
	if b.errors >= b.limits.errors {
		if b.reportsOverflow {
			b.overflowReport().DroppedErrors++
		}
		return
	}
	b.errors++
	b.entries = append(b.entries, &backlogEntry{kind: errorEntry, err: err})
}

func (b *handlerBacklog) pushUpdate(channel string, update any) {
	stream := streamKey(channel, update)
	if stream != "" {
		if queued := b.streams[stream]; queued != nil {
			queued.replace(update)
			return
		}
	}
	if b.updates >= b.limits.updates {
		if b.reportsOverflow {
			report := b.overflowReport()
			if report.DroppedUpdates == nil {
				report.DroppedUpdates = make(map[string]int)
			}
			report.DroppedUpdates[channel]++
		}
		return
	}
	b.updates++
	entry := &backlogEntry{kind: updateEntry, channel: channel, update: update, stream: stream}
	b.entries = append(b.entries, entry)
	if stream != "" {
		b.streams[stream] = entry
	}
}

func (b *handlerBacklog) overflowReport() *BacklogOverflowError {
	if last := len(b.entries) - 1; last >= 0 && b.entries[last].kind == overflowEntry {
		return b.entries[last].overflow
	}
	report := &BacklogOverflowError{}
	b.entries = append(b.entries, &backlogEntry{kind: overflowEntry, overflow: report})
	return report
}

func (b *handlerBacklog) pop() (*backlogEntry, bool) {
	if len(b.entries) == 0 {
		return nil, false
	}
	entry := b.entries[0]
	b.entries[0] = nil
	b.entries = b.entries[1:]
	switch entry.kind {
	case updateEntry:
		b.updates--
		if entry.stream != "" && b.streams[entry.stream] == entry {
			delete(b.streams, entry.stream)
		}
	case errorEntry:
		b.errors--
	}
	return entry, true
}

func (b *handlerBacklog) discard() {
	b.entries = nil
	clear(b.streams)
	b.updates, b.errors = 0, 0
}

func streamKey(channel string, update any) string {
	switch update := update.(type) {
	case OrderBook:
		return "book " + channel
	case Ticker:
		return "ticker " + channel
	case []Ticker:
		return "tickers " + channel
	case Kline:
		return "kline " + channel + " " + strconv.FormatInt(update.StartTime.Unix(), 10)
	default:
		return ""
	}
}

func (e *backlogEntry) replace(next any) {
	batch, ok := next.([]Ticker)
	if !ok {
		e.update = next
		return
	}
	queued := e.update.([]Ticker)
	if e.tickerPositions == nil {
		e.tickerPositions = indexTickers(queued)
	}
	e.update = mergeTickers(queued, batch, e.tickerPositions)
}

func indexTickers(batch []Ticker) map[string]int {
	positions := make(map[string]int, 2*len(batch))
	for i, ticker := range batch {
		for _, name := range tickerNames(ticker) {
			positions[name] = i
		}
	}
	return positions
}

func mergeTickers(queued, next []Ticker, positions map[string]int) []Ticker {
	for _, ticker := range next {
		names := tickerNames(ticker)
		position, found := findPosition(positions, names)
		if found {
			queued[position] = ticker
		} else {
			position = len(queued)
			queued = append(queued, ticker)
		}
		for _, name := range names {
			positions[name] = position
		}
	}
	return queued
}

func findPosition(positions map[string]int, names []string) (int, bool) {
	for _, name := range names {
		if position, ok := positions[name]; ok {
			return position, true
		}
	}
	return 0, false
}

func tickerNames(ticker Ticker) []string {
	var names []string
	if ticker.Symbol != "" {
		names = append(names, "symbol "+strings.ToUpper(ticker.Symbol))
	}
	if ticker.SymbolID != 0 {
		names = append(names, "id "+strconv.FormatInt(ticker.SymbolID, 10))
	}
	return names
}
