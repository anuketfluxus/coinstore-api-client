package coinstore

import (
	"encoding/json"
	"time"

	"github.com/anuketfluxus/coinstore-api-client/internal/flexjson"
	"github.com/shopspring/decimal"
)

func convertAll[From, To any](items []From, convert func(From) To) []To {
	converted := make([]To, len(items))
	for i, item := range items {
		converted[i] = convert(item)
	}
	return converted
}

func timeFromUnixMilli(millis flexjson.Int64) time.Time {
	if millis == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(millis)).UTC()
}

func timeFromUnix(seconds flexjson.Int64) time.Time {
	if seconds == 0 {
		return time.Time{}
	}
	return time.Unix(int64(seconds), 0).UTC()
}

func dec(d flexjson.Decimal) decimal.Decimal {
	return decimal.Decimal(d)
}

func int64s(values []flexjson.Int64) []int64 {
	return convertAll(values, func(v flexjson.Int64) int64 { return int64(v) })
}

func stringsOf(values []flexjson.String) []string {
	return convertAll(values, func(v flexjson.String) string { return string(v) })
}

func isAbsent(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}
