package coinstore

import (
	"fmt"
	"net/http"
)

const maxRedirects = 10

func withoutSignedRedirects(client *http.Client) *http.Client {
	restricted := *client
	next := client.CheckRedirect
	restricted.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if isSigned(via[0]) {
			return http.ErrUseLastResponse
		}
		if next != nil {
			return next(req, via)
		}
		if len(via) >= maxRedirects {
			return fmt.Errorf("stopped after %d redirects", maxRedirects)
		}
		return nil
	}
	return &restricted
}

func isSigned(req *http.Request) bool {
	return req.Header.Get(headerAPIKey) != ""
}
