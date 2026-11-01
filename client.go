package coinstore

import (
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	DefaultBaseURL = "https://api.coinstore.com/api"

	DefaultFuturesBaseURL = "https://futures.api.coinstore.com/api"

	defaultLanguage    = "en_US"
	defaultHTTPTimeout = 30 * time.Second
	userAgent          = "coinstore-go"
)

type Client struct {
	httpClient  *http.Client
	baseURL     *url.URL
	futuresURL  *url.URL
	credentials *credentials
	language    string
	now         func() time.Time
}

type Option func(*clientOptions)

type clientOptions struct {
	httpClient     *http.Client
	baseURL        string
	futuresBaseURL string
	apiKey         string
	secretKey      string
	language       string
}

func WithCredentials(apiKey, secretKey string) Option {
	return func(o *clientOptions) {
		o.apiKey = apiKey
		o.secretKey = secretKey
	}
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(o *clientOptions) {
		o.httpClient = httpClient
	}
}

func WithBaseURL(baseURL string) Option {
	return func(o *clientOptions) {
		o.baseURL = baseURL
	}
}

func WithFuturesBaseURL(baseURL string) Option {
	return func(o *clientOptions) {
		o.futuresBaseURL = baseURL
	}
}

func WithLanguage(language string) Option {
	return func(o *clientOptions) {
		o.language = language
	}
}

func NewClient(opts ...Option) (*Client, error) {
	o := clientOptions{
		baseURL:        DefaultBaseURL,
		futuresBaseURL: DefaultFuturesBaseURL,
		language:       defaultLanguage,
	}
	for _, opt := range opts {
		opt(&o)
	}

	baseURL, err := parseBaseURL(o.baseURL)
	if err != nil {
		return nil, err
	}
	futuresURL, err := parseBaseURL(o.futuresBaseURL)
	if err != nil {
		return nil, err
	}
	creds, err := newCredentials(o.apiKey, o.secretKey)
	if err != nil {
		return nil, err
	}
	httpClient := o.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}

	return &Client{
		httpClient:  withoutSignedRedirects(httpClient),
		baseURL:     baseURL,
		futuresURL:  futuresURL,
		credentials: creds,
		language:    o.language,
		now:         time.Now,
	}, nil
}

func parseBaseURL(rawURL string) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("coinstore: invalid base URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("coinstore: invalid base URL %q: want an absolute http or https URL", rawURL)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("coinstore: invalid base URL %q: must not have a query or fragment", rawURL)
	}
	return u, nil
}
