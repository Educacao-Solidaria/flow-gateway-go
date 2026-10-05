package contracts_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"strings"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/contracts"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

func TestPhase1Contracts_MCPDispatchSuccess(t *testing.T) {
	harness, err := contracts.NewPhase1Harness()
	if err != nil {
		t.Fatalf("erro ao instanciar harness: %v", err)
	}

	args := json.RawMessage(`{"prompt":"Teste de integracao Fase 1"}`)
	result, err := harness.DispatchMCPCall(context.Background(), "echo_prompt", args)
	if err != nil {
		t.Fatalf("erro ao despachar tool: %v", err)
	}

	resMap, ok := result.(map[string]string)
	if !ok {
		t.Fatalf("esperava map[string]string, obteve %T", result)
	}

	if resMap["echo"] != "Teste de integracao Fase 1" {
		t.Fatalf("eco incorreto: %s", resMap["echo"])
	}
	if resMap["role"] != string(domain.RoleAssistant) {
		t.Fatalf("role incorreta: %s", resMap["role"])
	}
}

func TestPhase1Contracts_ToolNotFound(t *testing.T) {
	harness, err := contracts.NewPhase1Harness()
	if err != nil {
		t.Fatalf("erro ao instanciar harness: %v", err)
	}

	_, err = harness.DispatchMCPCall(context.Background(), "ferramenta_inexistente", nil)
	if err == nil {
		t.Fatal("esperava erro para ferramenta inexistente")
	}

	var gatewayErr *errors.GatewayError
	if !stderrors.As(err, &gatewayErr) {
		t.Fatalf("esperava GatewayError, obteve %T", err)
	}
	if gatewayErr.Code != "TOOL_NOT_FOUND" {
		t.Fatalf("codigo incorreto: %s", gatewayErr.Code)
	}
}

func TestPhase1Contracts_SSEEventFormatting(t *testing.T) {
	chunk := domain.StreamChunk{
		ID:           "chk-phase1",
		Model:        domain.ModelDeepSeekV3,
		DeltaContent: "palavra de streaming",
		IsLast:       true,
		FinishReason: "stop",
	}

	formatted, err := contracts.ValidateDomainSSE(chunk)
	if err != nil {
		t.Fatalf("erro ao formatar evento SSE: %v", err)
	}

	str := string(formatted)
	if !strings.HasPrefix(str, "event: completion\n") {
		t.Fatalf("prefixo incorreto: %s", str)
	}
	if !strings.Contains(str, `"delta_content":"palavra de streaming"`) {
		t.Fatalf("delta ausente no SSE: %s", str)
	}
	if !strings.HasSuffix(str, "\n\n") {
		t.Fatal("delimitador final duplo ausente")
	}
}
