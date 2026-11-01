package flexjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/shopspring/decimal"
)

type Int64 int64

func (v *Int64) UnmarshalJSON(data []byte) error {
	s, err := parseScalar(data)
	if err != nil {
		return fmt.Errorf("flexjson: decode integer: %w", err)
	}
	if s.isBlank() {
		*v = 0
		return nil
	}
	if s.kind == kindBool {
		return fmt.Errorf("flexjson: decode integer: unexpected boolean %s", s.text)
	}
	n, err := strconv.ParseInt(s.text, 10, 64)
	if err != nil {
		return fmt.Errorf("flexjson: decode integer: %w", err)
	}
	*v = Int64(n)
	return nil
}

type Bool bool

func (v *Bool) UnmarshalJSON(data []byte) error {
	s, err := parseScalar(data)
	if err != nil {
		return fmt.Errorf("flexjson: decode boolean: %w", err)
	}
	if s.isBlank() {
		*v = false
		return nil
	}
	switch s.text {
	case "true", "1":
		*v = true
	case "false", "0":
		*v = false
	default:
		return fmt.Errorf("flexjson: decode boolean: unexpected value %q", s.text)
	}
	return nil
}

type String string

func (v *String) UnmarshalJSON(data []byte) error {
	s, err := parseScalar(data)
	if err != nil {
		return fmt.Errorf("flexjson: decode string: %w", err)
	}
	*v = String(s.text)
	return nil
}

type Decimal decimal.Decimal

func (v *Decimal) UnmarshalJSON(data []byte) error {
	s, err := parseScalar(data)
	if err != nil {
		return fmt.Errorf("flexjson: decode decimal: %w", err)
	}
	if s.isBlank() {
		*v = Decimal{}
		return nil
	}
	if s.kind == kindBool {
		return fmt.Errorf("flexjson: decode decimal: unexpected boolean %s", s.text)
	}
	d, err := decimal.NewFromString(s.text)
	if err != nil {
		return fmt.Errorf("flexjson: decode decimal: %w", err)
	}
	*v = Decimal(d)
	return nil
}

func (v Decimal) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.String())
}

func (v Decimal) String() string {
	normalized := decimal.Decimal(v).Normalize()
	return normalized.StringFixed(max(0, -normalized.Exponent()))
}

func (v Decimal) IsZero() bool {
	return decimal.Decimal(v).IsZero()
}

type scalarKind int

const (
	kindNull scalarKind = iota
	kindString
	kindNumber
	kindBool
)

type scalar struct {
	kind scalarKind
	text string
}

func (s scalar) isBlank() bool {
	return s.kind == kindNull || (s.kind == kindString && s.text == "")
}

const maxQuotedInput = 64

func parseScalar(data []byte) (scalar, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return scalar{}, errors.New("empty input")
	}
	switch first := data[0]; {
	case first == '"':
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return scalar{}, err
		}
		return scalar{kind: kindString, text: text}, nil
	case first == '-' || ('0' <= first && first <= '9'):
		return scalar{kind: kindNumber, text: string(data)}, nil
	case string(data) == "null":
		return scalar{kind: kindNull}, nil
	case string(data) == "true" || string(data) == "false":
		return scalar{kind: kindBool, text: string(data)}, nil
	}
	if len(data) > maxQuotedInput {
		data = append(data[:maxQuotedInput:maxQuotedInput], "..."...)
	}
	return scalar{}, fmt.Errorf("unsupported JSON value %s", data)
}
