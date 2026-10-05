package mcp

import "encoding/json"

const (
	JSONRPCVersion = "2.0"

	// Standard JSON-RPC 2.0 error codes
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603

	// MCP specific error codes
	CodeToolNotFound     = -32000
	CodeToolExecFailed   = -32001
	CodeResourceNotFound = -32002
)

// Request representa uma chamada JSON-RPC 2.0 recebida pelo servidor MCP.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response representa uma resposta JSON-RPC 2.0 emitida pelo servidor MCP.
type Response struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *Error      `json:"error,omitempty"`
}

// Error detalha falhas no processamento JSON-RPC 2.0.
type Error struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func (e *Error) Error() string {
	return e.Message
}

// NewError cria uma struct de erro padronizada.
func NewError(code int, msg string, data ...interface{}) *Error {
	var d interface{}
	if len(data) > 0 {
		d = data[0]
	}
	return &Error{
		Code:    code,
		Message: msg,
		Data:    d,
	}
}
