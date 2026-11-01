package coinstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestDoSendsStandardHeaders(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)

	if err := client.do(context.Background(), client.get("/test", nil, public), nil); err != nil {
		t.Fatalf("do returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/test", "", "")
	assertUnsigned(t, req)
	header := req.Header
	want := map[string]string{
		"Accept":        "application/json",
		"Content-Type":  "application/json",
		"User-Agent":    "coinstore-go",
		"Exch-Language": "en_US",
	}
	for name, value := range want {
		if got := header.Get(name); got != value {
			t.Errorf("header %s = %q, want %q", name, got, value)
		}
	}
}

func TestWithLanguage(t *testing.T) {
	tests := []struct {
		language string
		want     []string
	}{
		{"zh_CN", []string{"zh_CN"}},
		{"", nil},
	}
	for _, tt := range tests {
		t.Run(tt.language, func(t *testing.T) {
			api := newFakeAPI(t, http.StatusOK, `{"code":0}`)
			client := newTestClient(t, api, WithLanguage(tt.language))

			if err := client.do(context.Background(), client.get("/test", nil, public), nil); err != nil {
				t.Fatalf("do returned error: %v", err)
			}
			assertEqual(t, api.onlyRequest(t).Header.Values("Exch-Language"), tt.want)
		})
	}
}

func TestSignedPostWithoutBodySendsEmptyObject(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)

	if err := client.do(context.Background(), client.post("/test", nil, signed), nil); err != nil {
		t.Fatalf("do returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodPost, "/api/test", "", "{}")
	assertSigned(t, req)
}

func TestDoSignsWithIssuedAt(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)
	r := client.post("/test", map[string]int64{"timestamp": 1700000000000}, signed)
	r.issuedAt = time.UnixMilli(1700000000000)

	if err := client.do(context.Background(), r, nil); err != nil {
		t.Fatalf("do returned error: %v", err)
	}

	req := api.onlyRequest(t)
	if got := req.Header.Get(headerExpires); got != "1700000000000" {
		t.Errorf("%s = %q, want 1700000000000", headerExpires, got)
	}
	creds := &credentials{apiKey: testAPIKey, secretKey: []byte(testSecretKey)}
	if got, want := req.Header.Get(headerSignature), creds.signature(1700000000000, []byte(req.Body)); got != want {
		t.Errorf("%s = %q, want %q", headerSignature, got, want)
	}
}

func TestSignedRequestWithoutCredentialsSendsNothing(t *testing.T) {
	api := newFakeAPI(t, http.StatusOK, `{"code":0}`)
	client, err := NewClient(WithBaseURL(api.server.URL))
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}

	err = client.do(context.Background(), client.post("/test", nil, signed), nil)

	if !errors.Is(err, ErrMissingCredentials) {
		t.Errorf("do error = %v, want ErrMissingCredentials", err)
	}
	if n := api.requestCount(); n != 0 {
		t.Errorf("server received %d requests, want none", n)
	}
}

func TestDoDecodesEnvelope(t *testing.T) {
	type data struct {
		Value int `json:"value"`
	}
	tests := []struct {
		name     string
		status   int
		response string
		want     data
	}{
		{"numeric success code", http.StatusOK, `{"code":0,"data":{"value":1}}`, data{Value: 1}},
		{"string success code", http.StatusOK, `{"code":"0","message":"Success","data":{"value":2}}`, data{Value: 2}},
		{"code after data", http.StatusOK, `{"data":{"value":3},"code":0}`, data{Value: 3}},
		{"null data", http.StatusOK, `{"code":0,"data":null}`, data{}},
		{"absent data", http.StatusOK, `{"code":0}`, data{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := serveOnce(t, tt.status, tt.response)
			var got data
			if err := client.do(context.Background(), client.get("/test", nil, public), &got); err != nil {
				t.Fatalf("do returned error: %v", err)
			}
			assertEqual(t, got, tt.want)
		})
	}
}

func TestDoReportsAPIErrors(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		response string
		want     APIError
	}{
		{
			name:     "numeric error code",
			status:   http.StatusOK,
			response: `{"code":3113,"message":"account-insufficient","data":null}`,
			want:     APIError{HTTPStatus: 200, Code: CodeInsufficientBalance, Message: "account-insufficient"},
		},
		{
			name:     "string error code",
			status:   http.StatusOK,
			response: `{"code":"10001","message":"System Error. Please Contact The System Administrator.","data":{}}`,
			want:     APIError{HTTPStatus: 200, Code: 10001, Message: "System Error. Please Contact The System Administrator."},
		},
		{
			name:     "message under msg",
			status:   http.StatusOK,
			response: `{"code":101040502,"msg":"request field missing","data":null}`,
			want:     APIError{HTTPStatus: 200, Code: CodeRequestFieldMissing, Message: "request field missing"},
		},
		{
			name:     "error status with envelope",
			status:   http.StatusUnauthorized,
			response: `{"message":"user-not-login","code":401}`,
			want:     APIError{HTTPStatus: 401, Code: CodeSignatureFailed, Message: "user-not-login"},
		},
		{
			name:     "error status with page body",
			status:   http.StatusBadGateway,
			response: "<html><body>Bad Gateway</body></html>\n",
			want:     APIError{HTTPStatus: 502, Message: "<html><body>Bad Gateway</body></html>"},
		},
		{
			name:     "error status with empty body",
			status:   http.StatusTooManyRequests,
			response: "",
			want:     APIError{HTTPStatus: 429},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := serveOnce(t, tt.status, tt.response)

			err := client.do(context.Background(), client.get("/test", nil, public), nil)

			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("do error = %v, want an *APIError", err)
			}
			want := tt.want
			want.Method, want.Path = http.MethodGet, "/test"
			assertEqual(t, *apiErr, want)
			if errors.Is(err, ErrOutcomeUnknown) {
				t.Errorf("do error = %v for a request that reads data, want no ErrOutcomeUnknown", err)
			}
		})
	}
}

func TestAPIErrorReportsServerFailure(t *testing.T) {
	tests := []struct {
		name string
		err  APIError
		want bool
	}{
		{"rejection code", APIError{HTTPStatus: 200, Code: CodeInsufficientBalance}, false},
		{"code mirroring a client error status", APIError{HTTPStatus: 200, Code: CodeSignatureFailed}, false},
		{"system error code", APIError{HTTPStatus: 200, Code: CodeSystemError}, true},
		{"code mirroring a server error status", APIError{HTTPStatus: 200, Code: 503}, true},
		{"code beyond the server error statuses", APIError{HTTPStatus: 200, Code: 600}, false},
		{"client error status", APIError{HTTPStatus: http.StatusTooManyRequests}, false},
		{"server error status", APIError{HTTPStatus: http.StatusBadGateway}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.reportsServerFailure(); got != tt.want {
				t.Errorf("reportsServerFailure() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestDoRejectsMalformedResponses(t *testing.T) {
	tests := []struct {
		name     string
		response string
	}{
		{"not JSON", "OK"},
		{"array instead of envelope", `[1,2,3]`},
		{"null instead of envelope", `null`},
		{"empty envelope", `{}`},
		{"missing code", `{"data":{"value":4}}`},
		{"null code", `{"code":null,"msg":"system busy"}`},
		{"empty code", `{"code":"","msg":"system busy"}`},
		{"non-numeric code", `{"code":"oops","data":{}}`},
		{"boolean code", `{"code":false,"data":{}}`},
		{"data of the wrong shape", `{"code":0,"data":"text"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := serveOnce(t, http.StatusOK, tt.response)
			var out struct{ Value int }

			err := client.do(context.Background(), client.get("/test", nil, public), &out)

			var apiErr *APIError
			if err == nil || errors.As(err, &apiErr) {
				t.Errorf("do error = %v, want a decoding error", err)
			}
			if errors.Is(err, ErrOutcomeUnknown) {
				t.Errorf("do error = %v for a request that reads data, want no ErrOutcomeUnknown", err)
			}
		})
	}
}

func TestAccountChangesWithUnknownOutcome(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		response     string
		wantAPIError bool
	}{
		{"malformed response", http.StatusOK, "OK", false},
		{"response without code", http.StatusOK, `{"msg":"transfer failed"}`, false},
		{"success with undecodable data", http.StatusOK, `{"code":0,"data":"text"}`, false},
		{"server error page", http.StatusBadGateway, "<html>Bad Gateway</html>", true},
		{"server error envelope", http.StatusInternalServerError, `{"code":500,"message":"internal error"}`, true},
		{
			name:         "system error code",
			status:       http.StatusOK,
			response:     `{"code":"10001","message":"System Error. Please Contact The System Administrator.","data":{}}`,
			wantAPIError: true,
		},
		{"server error code", http.StatusOK, `{"code":500,"message":"internal error"}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := serveOnce(t, tt.status, tt.response)
			var out struct{ Value int }

			err := client.do(context.Background(), client.change("/test", nil), &out)

			if !errors.Is(err, ErrOutcomeUnknown) {
				t.Errorf("do error = %v, want ErrOutcomeUnknown", err)
			}
			var apiErr *APIError
			if got := errors.As(err, &apiErr); got != tt.wantAPIError {
				t.Errorf("errors.As(%v, *APIError) = %t, want %t", err, got, tt.wantAPIError)
			}
		})
	}
}

func TestAccountChangesWithKnownOutcome(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		response string
	}{
		{"API rejection", http.StatusOK, `{"code":3113,"message":"account-insufficient"}`},
		{"client error status", http.StatusTooManyRequests, "Too Many Requests"},
		{"client error envelope", http.StatusUnauthorized, `{"message":"user-not-login","code":401}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := serveOnce(t, tt.status, tt.response)

			err := client.do(context.Background(), client.change("/test", nil), nil)

			var apiErr *APIError
			if !errors.As(err, &apiErr) || errors.Is(err, ErrOutcomeUnknown) {
				t.Errorf("do error = %v, want an APIError without ErrOutcomeUnknown", err)
			}
		})
	}
}

func TestAccountChangeTimingOutAfterSendingHasUnknownOutcome(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-release
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	client, err := NewClient(
		WithBaseURL(server.URL),
		WithCredentials(testAPIKey, testSecretKey),
		WithHTTPClient(&http.Client{Timeout: 100 * time.Millisecond}),
	)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}

	err = client.do(context.Background(), client.change("/test", nil), nil)

	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Errorf("do error = %v, want ErrOutcomeUnknown", err)
	}
	if err == nil || !strings.Contains(err.Error(), "may have taken effect") {
		t.Errorf("do error = %v, want a message saying the request may have taken effect", err)
	}
}

func TestAccountChangeThatCannotConnectHasKnownOutcome(t *testing.T) {

	client, err := NewClient(WithBaseURL("http://127.0.0.1:1"), WithCredentials(testAPIKey, testSecretKey))
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}

	err = client.do(context.Background(), client.change("/test", nil), nil)

	if err == nil || errors.Is(err, ErrOutcomeUnknown) {
		t.Errorf("do error = %v, want a connection error without ErrOutcomeUnknown", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestAccountChangeThroughSilentTransportHasUnknownOutcome(t *testing.T) {
	sent := make(chan string, 1)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		sent <- string(body)
		return nil, errors.New("connection reset by peer")
	})
	client, err := NewClient(
		WithBaseURL("https://api.example"),
		WithCredentials(testAPIKey, testSecretKey),
		WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}

	err = client.do(context.Background(), client.change("/test", nil), nil)

	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Errorf("do error = %v, want ErrOutcomeUnknown", err)
	}
	if body := receive(t, sent); body != "{}" {
		t.Errorf("transport received body %q, want {}", body)
	}
}

func TestRequestWithDoneContextIsNotSent(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.do(ctx, client.change("/test", nil), nil)

	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrOutcomeUnknown) {
		t.Errorf("do error = %v, want context.Canceled without ErrOutcomeUnknown", err)
	}
	if n := api.requestCount(); n != 0 {
		t.Errorf("server received %d requests, want none", n)
	}
}

func TestParseResponseCode(t *testing.T) {
	tests := []struct {
		raw         string
		wantCode    int64
		wantPresent bool
		wantErr     bool
	}{
		{raw: ``, wantPresent: false},
		{raw: `null`, wantPresent: false},
		{raw: `""`, wantPresent: false},
		{raw: `0`, wantCode: 0, wantPresent: true},
		{raw: `"0"`, wantCode: 0, wantPresent: true},
		{raw: `3113`, wantCode: 3113, wantPresent: true},
		{raw: `"10001"`, wantCode: 10001, wantPresent: true},
		{raw: `102150808`, wantCode: 102150808, wantPresent: true},
		{raw: `"signature-failed"`, wantErr: true},
		{raw: `1.5`, wantErr: true},
		{raw: `true`, wantErr: true},
		{raw: `{}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			code, present, err := parseResponseCode([]byte(tt.raw))
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseResponseCode(%s) error = %v, want error: %t", tt.raw, err, tt.wantErr)
			}
			if code != tt.wantCode || present != tt.wantPresent {
				t.Errorf("parseResponseCode(%s) = %d, %t, want %d, %t", tt.raw, code, present, tt.wantCode, tt.wantPresent)
			}
		})
	}
}

func TestDoRejectsOversizedResponses(t *testing.T) {
	response := `{"code":0,"data":"` + strings.Repeat("x", maxResponseBytes) + `"}`
	client, _ := serveOnce(t, http.StatusOK, response)

	err := client.do(context.Background(), client.get("/test", nil, public), nil)

	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("do error = %v, want a size error", err)
	}
}

func TestDoReportsTruncatedResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		io.WriteString(w, `{"code":0`)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}

	err = client.do(context.Background(), client.get("/test", nil, public), nil)

	if err == nil || !strings.Contains(err.Error(), "read response") {
		t.Errorf("do error = %v, want a read error", err)
	}
}

func TestDoHonorsContextCancellation(t *testing.T) {
	client, _ := serveOnce(t, http.StatusOK, `{"code":0}`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.do(ctx, client.get("/test", nil, public), nil)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("do error = %v, want context.Canceled", err)
	}
}

func TestDoJoinsPathsAndEscapesQueries(t *testing.T) {
	client, api := serveOnce(t, http.StatusOK, `{"code":0}`)
	query := url.Values{"clOrdId": {"a b&c"}}

	if err := client.do(context.Background(), client.get("/v2/trade/order/orderInfo", query, signed), nil); err != nil {
		t.Fatalf("do returned error: %v", err)
	}

	req := api.onlyRequest(t)
	assertRequest(t, req, http.MethodGet, "/api/v2/trade/order/orderInfo", "clOrdId=a+b%26c", "")
	assertSigned(t, req)
}

func TestSummarizeBody(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"short body", "  Bad Gateway \n", "Bad Gateway"},
		{"long body", strings.Repeat("a", maxErrorBodyBytes+10), strings.Repeat("a", maxErrorBodyBytes) + "..."},
		{
			name:  "cut through a multi-byte character",
			input: strings.Repeat("a", maxErrorBodyBytes-1) + "é" + "tail",
			want:  strings.Repeat("a", maxErrorBodyBytes-1) + "...",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := summarizeBody([]byte(tt.input)); got != tt.want {
				t.Errorf("summarizeBody = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAPIErrorMessage(t *testing.T) {
	tests := []struct {
		name string
		err  APIError
		want string
	}{
		{
			name: "code and message",
			err:  APIError{Method: "POST", Path: "/trade/order/place", HTTPStatus: 200, Code: 3113, Message: "account-insufficient"},
			want: "coinstore: POST /trade/order/place: API error 3113 (HTTP 200): account-insufficient",
		},
		{
			name: "status only",
			err:  APIError{Method: "GET", Path: "/v1/market/tickers", HTTPStatus: 429},
			want: "coinstore: GET /v1/market/tickers: HTTP 429",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAPIErrorMatchesErrOrderNotFound(t *testing.T) {
	var err error = &APIError{Code: CodeOrderNotFound}
	if !errors.Is(err, ErrOrderNotFound) {
		t.Error("errors.Is(APIError{Code: CodeOrderNotFound}, ErrOrderNotFound) = false")
	}
	err = &APIError{Code: CodeInsufficientBalance}
	if errors.Is(err, ErrOrderNotFound) {
		t.Error("errors.Is(APIError{Code: CodeInsufficientBalance}, ErrOrderNotFound) = true")
	}
}
