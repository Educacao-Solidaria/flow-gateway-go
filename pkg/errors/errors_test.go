package errors_test

import (
	"errors"
	"net/http"
	"testing"

	gwErr "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

func TestGatewayError_UnwrapAndIs(t *testing.T) {
	wrapped := gwErr.NewRateLimitError(map[string]interface{}{"retry_after": 60})

	if !errors.Is(wrapped, gwErr.ErrRateLimited) {
		t.Fatal("errors.Is falhou ao identificar ErrRateLimited")
	}

	if wrapped.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("status HTTP incorreto: %d", wrapped.HTTPStatus)
	}

	if wrapped.JSONRPCCode != -32005 {
		t.Fatalf("código JSON-RPC incorreto: %d", wrapped.JSONRPCCode)
	}
}

func TestToHTTPStatus_Mapping(t *testing.T) {
	tests := []struct {
		err      error
		expected int
	}{
		{nil, http.StatusOK},
		{gwErr.ErrModelNotFound, http.StatusNotFound},
		{gwErr.ErrUpstreamTimeout, http.StatusGatewayTimeout},
		{gwErr.ErrUnauthorized, http.StatusUnauthorized},
		{errors.New("erro desconhecido"), http.StatusInternalServerError},
	}

	for _, tt := range tests {
		status := gwErr.ToHTTPStatus(tt.err)
		if status != tt.expected {
			t.Errorf("para erro %v esperava %d, obteve %d", tt.err, tt.expected, status)
		}
	}
}
