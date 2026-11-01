package coinstore

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func newRedirectingServers(t *testing.T, status int) (api *httptest.Server, targetHits *atomic.Int32) {
	t.Helper()
	targetHits = new(atomic.Int32)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		_, _ = w.Write([]byte(`{"code":0,"data":{"ordId":42}}`))
	}))
	t.Cleanup(target.Close)
	api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/elsewhere", status)
	}))
	t.Cleanup(api.Close)
	return api, targetHits
}

func TestSignedRequestsNeverFollowRedirects(t *testing.T) {
	for _, status := range []int{http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			api, targetHits := newRedirectingServers(t, status)
			client, err := NewClient(WithBaseURL(api.URL), WithCredentials(testAPIKey, testSecretKey))
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}

			_, err = client.PlaceOrder(context.Background(), "BTCUSDT", OrderRequest{
				Side: SideBuy, Type: OrderTypeLimit, Price: d("1"), Quantity: d("1"),
			})

			if hits := targetHits.Load(); hits != 0 {
				t.Errorf("the redirect's target received %d requests, want 0", hits)
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.HTTPStatus != status {
				t.Errorf("PlaceOrder error = %v, want an APIError with HTTP status %d", err, status)
			}
			if !errors.Is(err, ErrOutcomeUnknown) {
				t.Errorf("errors.Is(%v, ErrOutcomeUnknown) = false, want true", err)
			}
		})
	}
}

func TestSignedRedirectRefusalKeepsCallerHTTPClientUnchanged(t *testing.T) {
	httpClient := &http.Client{}
	if _, err := NewClient(WithHTTPClient(httpClient), WithCredentials(testAPIKey, testSecretKey)); err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if httpClient.CheckRedirect != nil {
		t.Error("NewClient changed the CheckRedirect of the HTTP client passed to WithHTTPClient")
	}
}

func TestUnsignedRequestsFollowRedirects(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		_, _ = w.Write([]byte(`{"code":0,"data":[]}`))
	}))
	t.Cleanup(target.Close)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusFound)
	}))
	t.Cleanup(api.Close)
	client, err := NewClient(WithBaseURL(api.URL))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if _, err := client.Tickers(context.Background()); err != nil {
		t.Fatalf("Tickers: %v", err)
	}
	if got := redirected.Load(); got != 1 {
		t.Errorf("the redirect's target received %d requests, want 1", got)
	}
}
