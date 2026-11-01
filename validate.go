package coinstore

import (
	"strings"

	"github.com/shopspring/decimal"
)

func validateRequired(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return invalidRequest("%s is required", name)
	}
	return nil
}

func validateSymbol(symbol string) error {
	return validateRequired("symbol", symbol)
}

func validateID(name string, id int64) error {
	if id <= 0 {
		return invalidRequest("%s must be positive, got %d", name, id)
	}
	return nil
}

func validateNotNegative[N int | int64](name string, value N) error {
	if value < 0 {
		return invalidRequest("%s must not be negative, got %d", name, value)
	}
	return nil
}

func validatePositiveAmount(name string, amount decimal.Decimal) error {
	if !amount.IsPositive() {
		return invalidRequest("%s must be positive, got %s", name, amount)
	}
	return nil
}
