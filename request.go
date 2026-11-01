package coinstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/anuketfluxus/coinstore-api-client/internal/flexjson"
)

const maxResponseBytes = 16 << 20

const maxErrorBodyBytes = 256

type authMode int

const (
	public authMode = iota
	signed
)

type apiRequest struct {
	method  string
	baseURL *url.URL

	path  string
	query url.Values

	body any
	auth authMode

	changesAccount bool

	issuedAt time.Time
}

func (c *Client) get(path string, query url.Values, auth authMode) apiRequest {
	return apiRequest{method: http.MethodGet, baseURL: c.baseURL, path: path, query: query, auth: auth}
}

func (c *Client) post(path string, body any, auth authMode) apiRequest {
	return apiRequest{method: http.MethodPost, baseURL: c.baseURL, path: path, body: body, auth: auth}
}

func (c *Client) change(path string, body any) apiRequest {
	r := c.post(path, body, signed)
	r.changesAccount = true
	return r
}

func (c *Client) do(ctx context.Context, r apiRequest, out any) error {
	httpReq, err := c.newHTTPRequest(ctx, r)
	if err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("coinstore: %s %s: %w", r.method, r.path, err)
	}

	var progress sendProgress
	if r.changesAccount {
		httpReq = httpReq.WithContext(httptrace.WithClientTrace(httpReq.Context(), progress.trace()))
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		err = fmt.Errorf("coinstore: %s %s: %w", r.method, r.path, err)
		if r.changesAccount && !progress.provesUnsent() {
			return outcomeUnknown(err)
		}
		return err
	}
	defer resp.Body.Close()

	payload, err := readResponseBody(resp.Body)
	if err != nil {
		return r.unconfirmed(fmt.Errorf("coinstore: %s %s: read response: %w", r.method, r.path, err))
	}
	return r.decodeResponse(resp.StatusCode, payload, out)
}

type sendProgress struct {
	connecting atomic.Bool

	wrote atomic.Bool
}

func (p *sendProgress) trace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GetConn:      func(string) { p.connecting.Store(true) },
		GotConn:      func(httptrace.GotConnInfo) { p.connecting.Store(true) },
		WroteHeaders: func() { p.wrote.Store(true) },
		WroteRequest: func(httptrace.WroteRequestInfo) { p.wrote.Store(true) },
	}
}

func (p *sendProgress) provesUnsent() bool {
	return p.connecting.Load() && !p.wrote.Load()
}

func (c *Client) newHTTPRequest(ctx context.Context, r apiRequest) (*http.Request, error) {
	if r.auth == signed && c.credentials == nil {
		return nil, fmt.Errorf("%w (%s %s)", ErrMissingCredentials, r.method, r.path)
	}

	endpoint := r.baseURL.JoinPath(r.path)
	endpoint.RawQuery = r.query.Encode()

	body, err := r.encodeBody()
	if err != nil {
		return nil, fmt.Errorf("coinstore: %s %s: encode request body: %w", r.method, r.path, err)
	}
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	httpReq, err := http.NewRequestWithContext(ctx, r.method, endpoint.String(), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("coinstore: %s %s: %w", r.method, r.path, err)
	}

	httpReq.GetBody = nil

	header := httpReq.Header
	header.Set("Accept", "application/json")
	header.Set("Content-Type", "application/json")
	header.Set("User-Agent", userAgent)
	if c.language != "" {
		header.Set("Exch-Language", c.language)
	}
	if r.auth == signed {
		issuedAt := r.issuedAt
		if issuedAt.IsZero() {
			issuedAt = c.now()
		}
		c.credentials.sign(header, issuedAt, endpoint.RawQuery, body)
	}
	return httpReq, nil
}

func (r apiRequest) encodeBody() ([]byte, error) {
	if r.body != nil {
		return json.Marshal(r.body)
	}
	if r.method == http.MethodPost {
		return []byte("{}"), nil
	}
	return nil, nil
}

func readResponseBody(body io.Reader) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > maxResponseBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxResponseBytes)
	}
	return payload, nil
}

func (r apiRequest) decodeResponse(status int, payload []byte, out any) error {
	env, envErr := parseEnvelope(payload)
	if status < 200 || status > 299 {
		apiErr := &APIError{Method: r.method, Path: r.path, HTTPStatus: status}
		if envErr == nil && (env.hasCode || env.message != "") {
			apiErr.Code = ErrorCode(env.code)
			apiErr.Message = env.message
		} else {
			apiErr.Message = summarizeBody(payload)
		}
		if isRedirectStatus(status) {

			return r.unconfirmed(apiErr)
		}
		return r.rejectedOrUnconfirmed(apiErr)
	}
	if envErr != nil {
		return r.unconfirmed(fmt.Errorf("coinstore: %s %s: decode response: %w", r.method, r.path, envErr))
	}
	if !env.hasCode {
		return r.unconfirmed(fmt.Errorf("coinstore: %s %s: response carries no code", r.method, r.path))
	}
	if env.code != 0 {
		return r.rejectedOrUnconfirmed(&APIError{
			Method:     r.method,
			Path:       r.path,
			HTTPStatus: status,
			Code:       ErrorCode(env.code),
			Message:    env.message,
		})
	}
	if out == nil || isAbsent(env.data) {
		return nil
	}
	if err := json.Unmarshal(env.data, out); err != nil {
		return r.unconfirmed(fmt.Errorf("coinstore: %s %s: decode response data: %w", r.method, r.path, err))
	}
	return nil
}

func (r apiRequest) rejectedOrUnconfirmed(apiErr *APIError) error {
	if apiErr.reportsServerFailure() {
		return r.unconfirmed(apiErr)
	}
	return apiErr
}

func (r apiRequest) unconfirmed(err error) error {
	if r.changesAccount {
		return outcomeUnknown(err)
	}
	return err
}

type envelope struct {
	code    int64
	hasCode bool
	message string
	data    json.RawMessage
}

func parseEnvelope(payload []byte) (envelope, error) {

	var raw struct {
		Code    json.RawMessage `json:"code"`
		Message flexjson.String `json:"message"`
		Msg     flexjson.String `json:"msg"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return envelope{}, err
	}
	code, hasCode, err := parseResponseCode(raw.Code)
	if err != nil {
		return envelope{}, err
	}
	env := envelope{code: code, hasCode: hasCode, message: string(raw.Message), data: raw.Data}
	if env.message == "" {
		env.message = string(raw.Msg)
	}
	return env, nil
}

func parseResponseCode(raw json.RawMessage) (code int64, present bool, err error) {
	if isAbsent(raw) {
		return 0, false, nil
	}
	text := string(raw)
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, false, fmt.Errorf("decode response code: %w", err)
		}
		if text == "" {
			return 0, false, nil
		}
	}
	code, err = strconv.ParseInt(text, 10, 64)
	if err != nil {
		var numErr *strconv.NumError
		if errors.As(err, &numErr) {
			err = numErr.Err
		}
		return 0, false, fmt.Errorf("decode response code %s: %w", raw, err)
	}
	return code, true, nil
}

func summarizeBody(payload []byte) string {
	summary := bytes.TrimSpace(payload)
	if len(summary) <= maxErrorBodyBytes {
		return string(summary)
	}
	summary = summary[:maxErrorBodyBytes]

	for i := len(summary) - 1; i >= 0 && i >= len(summary)-utf8.UTFMax; i-- {
		if utf8.RuneStart(summary[i]) {
			if !utf8.FullRune(summary[i:]) {
				summary = summary[:i]
			}
			break
		}
	}
	return string(summary) + "..."
}
