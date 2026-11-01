package flexjson

import (
	"encoding/json"
	"testing"

	"github.com/shopspring/decimal"
)

func TestInt64UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Int64
	}{
		{"number", `1722183748419690`, 1722183748419690},
		{"numeric string", `"1722183748419690"`, 1722183748419690},
		{"negative number", `-1`, -1},
		{"negative string", `"-1"`, -1},
		{"max int64", `9223372036854775807`, 9223372036854775807},
		{"null", `null`, 0},
		{"empty string", `""`, 0},
		{"surrounding whitespace", " 42 ", 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Int64(7)
			if err := got.UnmarshalJSON([]byte(tt.input)); err != nil {
				t.Fatalf("UnmarshalJSON(%s) returned error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("UnmarshalJSON(%s) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestInt64UnmarshalJSONRejectsInvalidInput(t *testing.T) {
	for _, input := range []string{`1.5`, `"abc"`, `true`, `{}`, `[]`, `9223372036854775808`, ``} {
		t.Run(input, func(t *testing.T) {
			var got Int64
			if err := got.UnmarshalJSON([]byte(input)); err == nil {
				t.Errorf("UnmarshalJSON(%s) = %d, want an error", input, got)
			}
		})
	}
}

func TestBoolUnmarshalJSON(t *testing.T) {
	tests := []struct {
		input string
		want  Bool
	}{
		{`true`, true},
		{`false`, false},
		{`1`, true},
		{`0`, false},
		{`"1"`, true},
		{`"0"`, false},
		{`"true"`, true},
		{`"false"`, false},
		{`null`, false},
		{`""`, false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := !tt.want
			if err := got.UnmarshalJSON([]byte(tt.input)); err != nil {
				t.Fatalf("UnmarshalJSON(%s) returned error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("UnmarshalJSON(%s) = %t, want %t", tt.input, got, tt.want)
			}
		})
	}
}

func TestBoolUnmarshalJSONRejectsInvalidInput(t *testing.T) {
	for _, input := range []string{`2`, `"yes"`, `-1`, `{}`, `[1]`} {
		t.Run(input, func(t *testing.T) {
			var got Bool
			if err := got.UnmarshalJSON([]byte(input)); err == nil {
				t.Errorf("UnmarshalJSON(%s) = %t, want an error", input, got)
			}
		})
	}
}

func TestStringUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  String
	}{
		{"string", `"b1b2ea5e00a84b888d419cb73f8eb203"`, "b1b2ea5e00a84b888d419cb73f8eb203"},
		{"escaped string", `"a\"b"`, `a"b`},
		{"integer keeps digits", `1722183748419690`, "1722183748419690"},
		{"integer beyond float precision", `12345678901234567890`, "12345678901234567890"},
		{"boolean", `true`, "true"},
		{"null", `null`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := String("stale")
			if err := got.UnmarshalJSON([]byte(tt.input)); err != nil {
				t.Fatalf("UnmarshalJSON(%s) returned error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("UnmarshalJSON(%s) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestStringUnmarshalJSONRejectsCompositeValues(t *testing.T) {
	for _, input := range []string{`{}`, `["a"]`} {
		var got String
		if err := got.UnmarshalJSON([]byte(input)); err == nil {
			t.Errorf("UnmarshalJSON(%s) = %q, want an error", input, got)
		}
	}
}

func TestDecimalUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"string", `"0.05001"`, "0.05001"},
		{"number", `13297.94999`, "13297.94999"},
		{"scientific notation number", `9.1E-10`, "0.00000000091"},
		{"scientific notation string", `"9.2e-10"`, "0.00000000092"},
		{"trailing zeros", `40.900000000000000000`, "40.9"},
		{"negative zero exponent", `0E-18`, "0"},
		{"negative", `"-1.25"`, "-1.25"},
		{"null", `null`, "0"},
		{"empty string", `""`, "0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Decimal(decimal.NewFromInt(99))
			if err := got.UnmarshalJSON([]byte(tt.input)); err != nil {
				t.Fatalf("UnmarshalJSON(%s) returned error: %v", tt.input, err)
			}
			if !decimal.Decimal(got).Equal(decimal.RequireFromString(tt.want)) {
				t.Errorf("UnmarshalJSON(%s) = %s, want %s", tt.input, got, tt.want)
			}
		})
	}
}

func TestDecimalUnmarshalJSONRejectsInvalidInput(t *testing.T) {
	for _, input := range []string{`"abc"`, `true`, `{}`, `"1.2.3"`, `"NaN"`} {
		t.Run(input, func(t *testing.T) {
			var got Decimal
			if err := got.UnmarshalJSON([]byte(input)); err == nil {
				t.Errorf("UnmarshalJSON(%s) = %s, want an error", input, got)
			}
		})
	}
}

func TestDecimalString(t *testing.T) {
	tests := []struct {
		input decimal.Decimal
		want  string
	}{
		{decimal.RequireFromString("1.50"), "1.5"},
		{decimal.RequireFromString("9.2e-10"), "0.00000000092"},
		{decimal.New(15, 2), "1500"},
		{decimal.RequireFromString("30000"), "30000"},
		{decimal.RequireFromString("-0.000100"), "-0.0001"},
		{decimal.Zero, "0"},
		{decimal.RequireFromString("0.000"), "0"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := Decimal(tt.input).String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDecimalMarshalJSONIgnoresGlobalSettings(t *testing.T) {
	restore := func(trim, scientific, unquoted bool) func() {
		return func() {
			decimal.TrimTrailingZeros = trim
			decimal.UseScientificNotation = scientific
			decimal.MarshalJSONWithoutQuotes = unquoted
		}
	}(decimal.TrimTrailingZeros, decimal.UseScientificNotation, decimal.MarshalJSONWithoutQuotes)
	t.Cleanup(restore)

	decimal.TrimTrailingZeros = false
	decimal.UseScientificNotation = true
	decimal.MarshalJSONWithoutQuotes = true

	got, err := json.Marshal(Decimal(decimal.New(15, 2)))
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if string(got) != `"1500"` {
		t.Errorf("Marshal = %s, want %q", got, `"1500"`)
	}
}

func TestDecimalOmitZero(t *testing.T) {
	type payload struct {
		Price    Decimal `json:"price,omitzero"`
		Quantity Decimal `json:"quantity,omitzero"`
	}
	got, err := json.Marshal(payload{
		Price:    Decimal(decimal.NewFromInt(0)),
		Quantity: Decimal(decimal.RequireFromString("0.25")),
	})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if want := `{"quantity":"0.25"}`; string(got) != want {
		t.Errorf("Marshal = %s, want %s", got, want)
	}
}

func TestTypesDecodeAsStructFields(t *testing.T) {
	var got struct {
		ID      Int64   `json:"id"`
		Open    Bool    `json:"open"`
		OrderID String  `json:"ordId"`
		Price   Decimal `json:"price"`
	}
	input := `{"id":"42","open":1,"ordId":1758065956225025,"price":9.1E-10}`
	if err := json.Unmarshal([]byte(input), &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if got.ID != 42 || !got.Open || got.OrderID != "1758065956225025" || got.Price.String() != "0.00000000091" {
		t.Errorf("Unmarshal decoded %+v", got)
	}
}
