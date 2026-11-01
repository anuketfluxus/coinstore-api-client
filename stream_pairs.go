package coinstore

import (
	"cmp"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const maxKnownPairs = 1 << 16

type pairNames struct {
	mu sync.RWMutex

	idOf     map[string]string
	symbolOf map[string]string
}

func (p *pairNames) learn(channel, symbol string, id int64) bool {
	symbol = strings.ToLower(symbol)
	var pairID string
	if id != 0 {
		pairID = strconv.FormatInt(id, 10)
	}
	if pair, _, found := strings.Cut(channel, "@"); found && pair != "" {
		switch {
		case namesPairByID(pair):
			pairID = cmp.Or(pairID, pair)
		case pair != "!":
			symbol = cmp.Or(symbol, pair)
		}
	}
	if symbol == "" || pairID == "" {
		return false
	}

	p.mu.RLock()
	known := p.idOf[symbol] == pairID
	p.mu.RUnlock()
	if known {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.idOf[symbol] == pairID {
		return false
	}
	if p.idOf == nil {
		p.idOf = make(map[string]string)
		p.symbolOf = make(map[string]string)
	}
	_, symbolKnown := p.idOf[symbol]
	_, idKnown := p.symbolOf[pairID]
	if !symbolKnown && !idKnown && len(p.idOf) >= maxKnownPairs {
		return false
	}

	delete(p.symbolOf, p.idOf[symbol])
	delete(p.idOf, p.symbolOf[pairID])
	p.idOf[symbol] = pairID
	p.symbolOf[pairID] = symbol
	return true
}

func (p *pairNames) alias(pair string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.aliasLocked(pair)
}

func (p *pairNames) aliasLocked(pair string) (string, bool) {
	if id, ok := p.idOf[pair]; ok {
		return id, true
	}
	symbol, ok := p.symbolOf[pair]
	return symbol, ok
}

func (p *pairNames) placed(pair string) bool {
	_, ok := p.alias(pair)
	return ok
}

func (p *pairNames) mayBeSame(a, b string) bool {
	if a == b {
		return true
	}
	if namesPairByID(a) == namesPairByID(b) {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if other, ok := p.aliasLocked(a); ok {
		return other == b
	}
	_, bPlaced := p.aliasLocked(b)
	return !bPlaced
}

func (p *pairNames) withAliases(channels []string) []string {
	names := slices.Clone(channels)
	for _, channel := range channels {
		pair, topic, found := strings.Cut(channel, "@")
		if !found {
			continue
		}
		if other, ok := p.alias(pair); ok && !slices.Contains(names, other+"@"+topic) {
			names = append(names, other+"@"+topic)
		}
	}
	return names
}

func namesPairByID(pair string) bool {
	if pair == "" {
		return false
	}
	for _, r := range pair {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
