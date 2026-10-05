package errors

import (
	"errors"
	"net/http"
)

// ToHTTPStatus extrai o código HTTP adequado a partir de qualquer erro.
func ToHTTPStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}

	var gErr *GatewayError
	if errors.As(err, &gErr) && gErr.HTTPStatus != 0 {
		return gErr.HTTPStatus
	}

	switch {
	case errors.Is(err, ErrModelNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrRateLimited):
		return http.StatusTooManyRequests
	case errors.Is(err, ErrUpstreamTimeout):
		return http.StatusGatewayTimeout
	case errors.Is(err, ErrInvalidPayload):
		return http.StatusBadRequest
	case errors.Is(err, ErrUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, ErrCircuitOpen):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// ToJSONRPCCode extrai o código JSON-RPC 2.0 adequado a partir de qualquer erro.
func ToJSONRPCCode(err error) int {
	if err == nil {
		return 0
	}

	var gErr *GatewayError
	if errors.As(err, &gErr) && gErr.JSONRPCCode != 0 {
		return gErr.JSONRPCCode
	}

	switch {
	case errors.Is(err, ErrInvalidPayload):
		return -32602 // Invalid params
	case errors.Is(err, ErrModelNotFound):
		return -32004
	case errors.Is(err, ErrRateLimited):
		return -32005
	case errors.Is(err, ErrUpstreamTimeout):
		return -32008
	default:
		return -32603 // Internal error
	}
}
