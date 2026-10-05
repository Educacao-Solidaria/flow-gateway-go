package jsonrpc_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/jsonrpc"
)

func TestParseRequest_Success(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","id":42,"method":"tools/call","params":{"name":"query_cache"}}`)
	req, err := jsonrpc.ParseRequest(raw)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}

	if req.JSONRPC != "2.0" {
		t.Fatalf("esperava 2.0, obteve %s", req.JSONRPC)
	}
	if req.Method != "tools/call" {
		t.Fatalf("metodo incorreto: %s", req.Method)
	}
	if req.IsNotification() {
		t.Fatal("requisicao com id nao deve ser notificacao")
	}
}

func TestParseRequest_Notification(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","method":"notifications/progress"}`)
	req, err := jsonrpc.ParseRequest(raw)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}

	if !req.IsNotification() {
		t.Fatal("requisicao sem id deve ser identificada como notificacao")
	}
}

func TestParseRequest_ValidationErrors(t *testing.T) {
	// Versão inválida
	_, err := jsonrpc.ParseRequest([]byte(`{"jsonrpc":"1.0","id":1,"method":"ping"}`))
	if !errors.Is(err, jsonrpc.ErrInvalidVersion) {
		t.Fatalf("esperava ErrInvalidVersion, obteve: %v", err)
	}

	// Método vazio
	_, err = jsonrpc.ParseRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":""}`))
	if !errors.Is(err, jsonrpc.ErrEmptyMethod) {
		t.Fatalf("esperava ErrEmptyMethod, obteve: %v", err)
	}
}

func TestResponse_SuccessAndErrorSerialization(t *testing.T) {
	id := jsonrpc.NewIntID(10)
	respSuccess := jsonrpc.NewSuccessResponse(&id, map[string]string{"status": "ok"})

	data, err := json.Marshal(respSuccess)
	if err != nil {
		t.Fatalf("erro ao serializar sucesso: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("payload vazio")
	}

	strID := jsonrpc.NewStringID("req-abc")
	respErr := jsonrpc.NewErrorResponse(&strID, jsonrpc.CodeMethodNotFound, "metodo desconhecido", nil)
	errData, err := json.Marshal(respErr)
	if err != nil {
		t.Fatalf("erro ao serializar erro: %v", err)
	}
	if len(errData) == 0 {
		t.Fatal("payload vazio")
	}
}
