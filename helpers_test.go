package coinstore

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

const (
	testAPIKey    = "test-api-key"
	testSecretKey = "test-secret-key"
)

var testTime = time.UnixMilli(1629291143107)

type capturedRequest struct {
	Method      string
	EscapedPath string
	RawQuery    string
	Body        string
	Header      http.Header
}

type responder func(capturedRequest) (status int, body string)

type fakeAPI struct {
	server  *httptest.Server
	respond responder

	mu       sync.Mutex
	requests []capturedRequest
}

func newFakeAPI(t *testing.T, status int, response string) *fakeAPI {
	t.Helper()
	return newRespondingAPI(t, func(capturedRequest) (int, string) { return status, response })
}

func newRespondingAPI(t *testing.T, respond responder) *fakeAPI {
	t.Helper()
	api := &fakeAPI{respond: respond}
	api.server = httptest.NewServer(http.HandlerFunc(api.serveHTTP))
	t.Cleanup(api.server.Close)
	return api
}

func (f *fakeAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := capturedRequest{
		Method:      r.Method,
		EscapedPath: r.URL.EscapedPath(),
		RawQuery:    r.URL.RawQuery,
		Body:        string(body),
		Header:      r.Header.Clone(),
	}
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()

	status, response := f.respond(req)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	io.WriteString(w, response)
}

func (f *fakeAPI) onlyRequest(t *testing.T) capturedRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 1 {
		t.Fatalf("server received %d requests, want 1", len(f.requests))
	}
	return f.requests[0]
}

func (f *fakeAPI) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func newTestClient(t *testing.T, api *fakeAPI, opts ...Option) *Client {
	t.Helper()
	defaults := []Option{
		WithBaseURL(api.server.URL + "/api"),
		WithFuturesBaseURL(api.server.URL + "/futures"),
		WithCredentials(testAPIKey, testSecretKey),
	}
	client, err := NewClient(append(defaults, opts...)...)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	client.now = func() time.Time { return testTime }
	return client
}

func serveOnce(t *testing.T, status int, response string) (*Client, *fakeAPI) {
	t.Helper()
	api := newFakeAPI(t, status, response)
	return newTestClient(t, api), api
}

func assertRequest(t *testing.T, req capturedRequest, method, path, rawQuery, body string) {
	t.Helper()
	if req.Method != method {
		t.Errorf("method = %s, want %s", req.Method, method)
	}
	if req.EscapedPath != path {
		t.Errorf("path = %s, want %s", req.EscapedPath, path)
	}
	if req.RawQuery != rawQuery {
		t.Errorf("query = %q, want %q", req.RawQuery, rawQuery)
	}
	if req.Body != body {
		t.Errorf("body = %s, want %s", req.Body, body)
	}
}

func assertSigned(t *testing.T, req capturedRequest) {
	t.Helper()
	if got := req.Header.Get(headerAPIKey); got != testAPIKey {
		t.Errorf("%s = %q, want %q", headerAPIKey, got, testAPIKey)
	}
	wantExpires := strconv.FormatInt(testTime.UnixMilli(), 10)
	if got := req.Header.Get(headerExpires); got != wantExpires {
		t.Errorf("%s = %q, want %q", headerExpires, got, wantExpires)
	}
	creds := &credentials{apiKey: testAPIKey, secretKey: []byte(testSecretKey)}
	want := creds.signature(testTime.UnixMilli(), []byte(req.RawQuery+req.Body))
	if got := req.Header.Get(headerSignature); got != want {
		t.Errorf("%s = %q, want %q", headerSignature, got, want)
	}
}

func assertUnsigned(t *testing.T, req capturedRequest) {
	t.Helper()
	for _, name := range []string{headerAPIKey, headerExpires, headerSignature} {
		if got := req.Header.Get(name); got != "" {
			t.Errorf("public request carries %s = %q", name, got)
		}
	}
}

func assertEqual[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got:\n%+v\nwant:\n%+v", got, want)
	}
}

func d(value string) decimal.Decimal {
	return decimal.RequireFromString(value)
}
