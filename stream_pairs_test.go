package coinstore

import (
	"strconv"
	"testing"
)

func knownSame(names *pairNames, a, b string) bool {
	other, ok := names.alias(a)
	return ok && other == b
}

func TestPairNames(t *testing.T) {
	var names pairNames
	if !names.learn("", "BTCUSDT", 4) {
		t.Error("learn of a new pair reported that it learned nothing")
	}
	if names.learn("", "btcusdt", 4) {
		t.Error("learn of a known pair reported that it learned something")
	}
	names.learn("", "", 5)
	names.learn("", "ETHUSDT", 0)

	for _, tt := range []struct {
		pair, alias string
		ok          bool
	}{
		{"4", "btcusdt", true},
		{"btcusdt", "4", true},
		{"5", "", false},
		{"ethusdt", "", false},
	} {
		if alias, ok := names.alias(tt.pair); alias != tt.alias || ok != tt.ok {
			t.Errorf("alias(%q) = %q, %t, want %q, %t", tt.pair, alias, ok, tt.alias, tt.ok)
		}
	}
	assertEqual(t, names.withAliases([]string{"4@kline@day_1", "btcusdt@kline@day_1", "7@trade"}),
		[]string{"4@kline@day_1", "btcusdt@kline@day_1", "7@trade"})
	assertEqual(t, names.withAliases([]string{"4@kline@day_1"}), []string{"4@kline@day_1", "btcusdt@kline@day_1"})

	if !names.learn("", "BTCUSDT", 9) {
		t.Error("learn of a name that moved reported that it learned nothing")
	}
	if !knownSame(&names, "9", "btcusdt") || names.placed("4") {
		t.Error("the symbol still names its former pair")
	}
}

func TestPairNamesAreBounded(t *testing.T) {
	var names pairNames
	for id := range int64(maxKnownPairs) {
		names.learn("", "pair"+strconv.FormatInt(id, 10), id+1)
	}
	if names.learn("", "extra", maxKnownPairs+1) || names.placed("extra") {
		t.Error("learned a pair beyond the bound")
	}
	names.learn("", "pair0", 1)
	if !knownSame(&names, "pair0", "1") {
		t.Error("forgot a known pair")
	}
}

func TestPairNamesLearnedFromChannels(t *testing.T) {
	var names pairNames
	names.learn("4@trade", "BTCUSDT", 0)
	names.learn("ethusdt@ticker", "", 7)
	names.learn("xrpusdt@trade", "LTCUSDT", 9)
	names.learn("!@ticker", "DOGEUSDT", 0)
	names.learn("5@trade", "", 0)

	for _, tt := range [][2]string{{"4", "btcusdt"}, {"7", "ethusdt"}, {"9", "ltcusdt"}} {
		if !knownSame(&names, tt[0], tt[1]) {
			t.Errorf("did not learn that %s is %s", tt[0], tt[1])
		}
	}
	for _, name := range []string{"xrpusdt", "dogeusdt", "!", "5"} {
		if other, ok := names.alias(name); ok {
			t.Errorf("learned that %s is %s", name, other)
		}
	}
}

func TestPairNamesMayBeSame(t *testing.T) {
	var names pairNames
	names.learn("", "BTCUSDT", 4)

	for _, tt := range []struct {
		a, b string
		want bool
	}{
		{"4", "4", true},
		{"4", "btcusdt", true},
		{"btcusdt", "4", true},
		{"4", "5", false},
		{"btcusdt", "ethusdt", false},
		{"4", "ethusdt", false},
		{"ethusdt", "4", false},
		{"btcusdt", "5", false},
		{"5", "ethusdt", true},
		{"ethusdt", "5", true},
	} {
		if got := names.mayBeSame(tt.a, tt.b); got != tt.want {
			t.Errorf("mayBeSame(%q, %q) = %t, want %t", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestNamesPairByID(t *testing.T) {
	for pair, want := range map[string]bool{"4": true, "88066": true, "btcusdt": false, "1inchusdt": false, "": false} {
		if got := namesPairByID(pair); got != want {
			t.Errorf("namesPairByID(%q) = %t, want %t", pair, got, want)
		}
	}
}
