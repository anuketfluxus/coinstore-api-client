package coinstore

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNewClientDefaults(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	if got := client.baseURL.String(); got != DefaultBaseURL {
		t.Errorf("base URL = %s, want %s", got, DefaultBaseURL)
	}
	if got := client.futuresURL.String(); got != DefaultFuturesBaseURL {
		t.Errorf("futures base URL = %s, want %s", got, DefaultFuturesBaseURL)
	}
	if client.language != "en_US" {
		t.Errorf("language = %q, want en_US", client.language)
	}
	if client.credentials != nil {
		t.Error("client without WithCredentials has credentials")
	}
	if client.httpClient.Timeout != 30*time.Second {
		t.Errorf("HTTP client timeout = %s, want 30s", client.httpClient.Timeout)
	}
}

func TestNewClientAppliesOptions(t *testing.T) {
	httpClient := &http.Client{Transport: &http.Transport{}, Timeout: 7 * time.Second}
	client, err := NewClient(
		WithHTTPClient(httpClient),
		WithBaseURL("http://localhost:8080/spot/"),
		WithFuturesBaseURL("http://localhost:8081/futures"),
		WithCredentials("key", "secret"),
		WithLanguage("zh_CN"),
	)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	if client.httpClient.Transport != httpClient.Transport || client.httpClient.Timeout != httpClient.Timeout {
		t.Error("NewClient did not use the HTTP client from WithHTTPClient")
	}
	if got := client.baseURL.String(); got != "http://localhost:8080/spot/" {
		t.Errorf("base URL = %s", got)
	}
	if got := client.futuresURL.String(); got != "http://localhost:8081/futures" {
		t.Errorf("futures base URL = %s", got)
	}
	if client.credentials == nil || client.credentials.apiKey != "key" || string(client.credentials.secretKey) != "secret" {
		t.Errorf("credentials = %+v", client.credentials)
	}
	if client.language != "zh_CN" {
		t.Errorf("language = %q, want zh_CN", client.language)
	}
}

func TestNewClientRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		option  Option
		wantErr string
	}{
		{"relative base URL", WithBaseURL("/api"), "invalid base URL"},
		{"base URL with unsupported scheme", WithBaseURL("ftp://api.coinstore.com/api"), "invalid base URL"},
		{"base URL with query", WithBaseURL("https://api.coinstore.com/api?x=1"), "must not have a query"},
		{"unparsable base URL", WithBaseURL("https://api.coinstore.com/%zz"), "invalid base URL"},
		{"invalid futures base URL", WithFuturesBaseURL("futures.api.coinstore.com"), "invalid base URL"},
		{"API key without secret key", WithCredentials("key", ""), "must be set together"},
		{"secret key without API key", WithCredentials("", "secret"), "must be set together"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClient(tt.option)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("NewClient error = %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}
