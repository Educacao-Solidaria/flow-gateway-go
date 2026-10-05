package jsonrpc

import (
	"encoding/json"
	"errors"
	"fmt"
)

const Version = "2.0"

// Códigos canônicos da especificação JSON-RPC 2.0.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

var (
	ErrInvalidVersion = errors.New("campo jsonrpc deve ser '2.0'")
	ErrEmptyMethod    = errors.New("campo method nao pode ser vazio")
)

// ID representa um identificador JSON-RPC (string, number ou null).
type ID struct {
	raw interface{}
}

// NewIntID cria um identificador numérico.
func NewIntID(id int64) ID {
	return ID{raw: id}
}

// NewStringID cria um identificador alfanumérico.
func NewStringID(id string) ID {
	return ID{raw: id}
}

func (id ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.raw)
}

func (id *ID) UnmarshalJSON(data []byte) error {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	id.raw = v
	return nil
}

func (id ID) Value() interface{} {
	return id.raw
}

// Error representa um objeto de erro na resposta JSON-RPC.
type Error struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("jsonrpc error: code=%d message=%s", e.Code, e.Message)
}

// Request modela uma chamada JSON-RPC 2.0.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *ID             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification indica se a mensagem é uma notificação unidirecional (sem id).
func (r *Request) IsNotification() bool {
	return r.ID == nil || r.ID.raw == nil
}

// Validate verifica a conformidade com a especificação JSON-RPC 2.0.
func (r *Request) Validate() error {
	if r.JSONRPC != Version {
		return ErrInvalidVersion
	}
	if r.Method == "" {
		return ErrEmptyMethod
	}
	return nil
}

// Response modela a resposta correspondente a uma requisição JSON-RPC 2.0.
type Response struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      *ID         `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   *Error      `json:"error,omitempty"`
}

// NewSuccessResponse constrói uma resposta de sucesso padrão.
func NewSuccessResponse(id *ID, result interface{}) *Response {
	return &Response{
		JSONRPC: Version,
		ID:      id,
		Result:  result,
	}
}

// NewErrorResponse constrói uma resposta de erro padrão.
func NewErrorResponse(id *ID, code int, message string, data interface{}) *Response {
	return &Response{
		JSONRPC: Version,
		ID:      id,
		Error: &Error{
			Code:    code,
			Message: message,
			Data:    data,
		},
	}
}

// ParseRequest analisa bytes brutos em uma estrutura Request validada.
func ParseRequest(payload []byte) (*Request, error) {
	var req Request
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("falha de deserializacao json-rpc: %w", err)
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	return &req, nil
}
