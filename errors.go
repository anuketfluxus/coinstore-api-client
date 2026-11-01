package coinstore

import (
	"errors"
	"fmt"
	"net/http"
)

var ErrMissingCredentials = errors.New("coinstore: API credentials are required for this endpoint")

var ErrInvalidRequest = errors.New("coinstore: invalid request")

var ErrOrderNotFound = errors.New("coinstore: order not found")

var ErrOutcomeUnknown = errors.New("coinstore: the request may have taken effect")

type outcomeUnknownError struct {
	err error
}

func (e *outcomeUnknownError) Error() string {
	return e.err.Error() + " (the request may have taken effect)"
}

func (e *outcomeUnknownError) Unwrap() []error {
	return []error{e.err, ErrOutcomeUnknown}
}

func outcomeUnknown(err error) error {
	return &outcomeUnknownError{err: err}
}

type ErrorCode int

const (
	CodeSignatureFailed ErrorCode = 401

	CodeUnauthorized ErrorCode = 1401

	CodeSignatureError ErrorCode = 3005

	CodeSymbolNotFound ErrorCode = 3011

	CodeSymbolTradingRestricted ErrorCode = 3012

	CodeTradingRestricted ErrorCode = 3013

	CodeSubAccountTradingRestricted ErrorCode = 3014

	CodeSubAccountSymbolTradingRestricted ErrorCode = 3015

	CodeOrderNotFound ErrorCode = 3103

	CodeDuplicateOrder ErrorCode = 3111

	CodeInsufficientBalance ErrorCode = 3113

	CodeSubAccountNotPermitted ErrorCode = 6001

	CodeSystemError ErrorCode = 10001

	CodeRequestFieldMissing ErrorCode = 101040502

	CodeTransferFailed ErrorCode = 102150400

	CodeTransferRejected ErrorCode = 102150808
)

type APIError struct {
	Method string
	Path   string

	HTTPStatus int

	Code ErrorCode

	Message string
}

func (e *APIError) Error() string {
	status := fmt.Sprintf("HTTP %d", e.HTTPStatus)
	if e.Code != 0 {
		status = fmt.Sprintf("API error %d (HTTP %d)", e.Code, e.HTTPStatus)
	}
	if e.Message == "" {
		return fmt.Sprintf("coinstore: %s %s: %s", e.Method, e.Path, status)
	}
	return fmt.Sprintf("coinstore: %s %s: %s: %s", e.Method, e.Path, status, e.Message)
}

func (e *APIError) Is(target error) bool {
	return target == ErrOrderNotFound && e.Code == CodeOrderNotFound
}

func (e *APIError) reportsServerFailure() bool {
	return isServerErrorStatus(e.HTTPStatus) || isServerErrorStatus(int(e.Code)) || e.Code == CodeSystemError
}

func isServerErrorStatus(status int) bool {
	return status >= http.StatusInternalServerError && status < 600
}

func isRedirectStatus(status int) bool {
	return status >= http.StatusMultipleChoices && status < http.StatusBadRequest
}

func invalidRequest(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidRequest, fmt.Sprintf(format, args...))
}
