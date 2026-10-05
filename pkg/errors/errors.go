package errors

import (
	"errors"
	"fmt"
	"net/http"
)

// Erros sentinela comuns de domínio do Gateway.
var (
	ErrModelNotFound   = errors.New("modelo nao suportado ou inexistente")
	ErrRateLimited     = errors.New("limite de taxa de requisicoes excedido")
	ErrUpstreamTimeout = errors.New("timeout na comunicacao com o provedor upstream")
	ErrInvalidPayload  = errors.New("payload de requisicao malformado ou invalido")
	ErrUnauthorized    = errors.New("autenticacao ausente ou token invalido")
	ErrCircuitOpen     = errors.New("circuito temporariamente aberto por falhas consecutivas")
)

// GatewayError é um erro de domínio enriquecido com metadados para HTTP e JSON-RPC.
type GatewayError struct {
	Err         error                  `json:"-"`
	Code        string                 `json:"code"`
	Message     string                 `json:"message"`
	HTTPStatus  int                    `json:"http_status"`
	JSONRPCCode int                    `json:"jsonrpc_code"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

func (e *GatewayError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *GatewayError) Unwrap() error {
	return e.Err
}

// New cria um GatewayError com código e status padrão.
func New(code string, msg string, httpStatus int, jsonrpcCode int) *GatewayError {
	return &GatewayError{
		Code:        code,
		Message:     msg,
		HTTPStatus:  httpStatus,
		JSONRPCCode: jsonrpcCode,
		Details:     make(map[string]interface{}),
	}
}

// Wrap encapsula um erro existente mantendo a causa raiz acessível via errors.Is.
func Wrap(baseErr error, code string, msg string, httpStatus int, jsonrpcCode int) *GatewayError {
	return &GatewayError{
		Err:         baseErr,
		Code:        code,
		Message:     msg,
		HTTPStatus:  httpStatus,
		JSONRPCCode: jsonrpcCode,
		Details:     make(map[string]interface{}),
	}
}

// Helpers pré-configurados
func NewRateLimitError(details map[string]interface{}) *GatewayError {
	err := Wrap(ErrRateLimited, "RATE_LIMIT_EXCEEDED", "taxa de requisicoes excedida", http.StatusTooManyRequests, -32005)
	err.Details = details
	return err
}

func NewModelNotFoundError(model string) *GatewayError {
	err := Wrap(ErrModelNotFound, "MODEL_NOT_FOUND", fmt.Sprintf("modelo '%s' nao encontrado", model), http.StatusNotFound, -32004)
	err.Details = map[string]interface{}{"model": model}
	return err
}
