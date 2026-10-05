package mcp_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/mcp"
)

// MockTool simula uma ferramenta de teste.
type MockTool struct {
	name string
}

func (m *MockTool) Name() string        { return m.name }
func (m *MockTool) Description() string { return "Mock description" }
func (m *MockTool) InputSchema() map[string]interface{} {
	return map[string]interface{}{"type": "object"}
}
func (m *MockTool) Execute(ctx context.Context, args json.RawMessage) (interface{}, error) {
	return map[string]string{"status": "ok"}, nil
}

func TestRegistry_RegisterAndGetTool(t *testing.T) {
	reg := mcp.NewInMemoryRegistry()
	tool := &MockTool{name: "query_cache"}

	err := reg.RegisterTool(tool)
	if err != nil {
		t.Fatalf("erro inesperado ao registrar tool: %v", err)
	}

	// Duplicado deve falhar
	errDup := reg.RegisterTool(tool)
	if errDup == nil {
		t.Fatal("esperava erro ao registrar tool duplicada")
	}

	found, ok := reg.GetTool("query_cache")
	if !ok {
		t.Fatal("tool registrada nao foi encontrada")
	}
	if found.Name() != "query_cache" {
		t.Fatalf("esperava 'query_cache', obteve '%s'", found.Name())
	}

	list := reg.ListTools()
	if len(list) != 1 {
		t.Fatalf("esperava 1 tool na lista, obteve %d", len(list))
	}
}

func TestProtocol_JSONRPCSerialization(t *testing.T) {
	req := mcp.Request{
		JSONRPC: mcp.JSONRPCVersion,
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"dispatch_llm"}`),
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("falha ao serializar request: %v", err)
	}

	var parsed mcp.Request
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("falha ao desserializar request: %v", err)
	}

	if parsed.Method != "tools/call" {
		t.Fatalf("metodo incorreto: %s", parsed.Method)
	}
}
