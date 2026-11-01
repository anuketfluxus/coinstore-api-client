package coinstore

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"time"
)

const (
	headerAPIKey    = "X-CS-APIKEY"
	headerExpires   = "X-CS-EXPIRES"
	headerSignature = "X-CS-SIGN"
)

const signingWindowMillis = 30_000

type credentials struct {
	apiKey    string
	secretKey []byte
}

func newCredentials(apiKey, secretKey string) (*credentials, error) {
	switch {
	case apiKey == "" && secretKey == "":
		return nil, nil
	case apiKey == "" || secretKey == "":
		return nil, errors.New("coinstore: API key and secret key must be set together")
	}
	return &credentials{apiKey: apiKey, secretKey: []byte(secretKey)}, nil
}

func (c *credentials) sign(header http.Header, issuedAt time.Time, query string, body []byte) {
	expires := issuedAt.UnixMilli()
	payload := make([]byte, 0, len(query)+len(body))
	payload = append(payload, query...)
	payload = append(payload, body...)

	header.Set(headerAPIKey, c.apiKey)
	header.Set(headerExpires, strconv.FormatInt(expires, 10))
	header.Set(headerSignature, c.signature(expires, payload))
}

func (c *credentials) signature(expiresMillis int64, payload []byte) string {
	window := strconv.FormatInt(expiresMillis/signingWindowMillis, 10)
	signingKey := hmacSHA256Hex(c.secretKey, []byte(window))
	return hmacSHA256Hex([]byte(signingKey), payload)
}

func hmacSHA256Hex(key, message []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(message)
	return hex.EncodeToString(mac.Sum(nil))
}
