package contracts

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/mcp"
)

// Phase1Harness agrega os componentes e contratos integrados na Fase 1 do Flow Gateway.
type Phase1Harness struct {
	Registry mcp.Registry
	Config   *config.Config
}

// MockEchoTool é uma ferramenta de teste para validação de ciclo de vida MCP.
type MockEchoTool struct{}

func (m *MockEchoTool) Name() string { return "echo_prompt" }
func (m *MockEchoTool) Description() string {
	return "Ecoa o prompt validando conformidade de domínio da Fase 1"
}
func (m *MockEchoTool) InputSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"prompt": map[string]interface{}{"type": "string"},
		},
		"required": []string{"prompt"},
	}
}
func (m *MockEchoTool) Execute(ctx context.Context, args json.RawMessage) (interface{}, error) {
	var input struct {
		Prompt string `json:"prompt"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return nil, errors.Wrap(errors.ErrInvalidPayload, "INVALID_PAYLOAD", err.Error(), 400, -32602)
	}
	if strings.TrimSpace(input.Prompt) == "" {
		return nil, errors.Wrap(errors.ErrInvalidPayload, "INVALID_PAYLOAD", "prompt vazio", 400, -32602)
	}

	return map[string]string{
		"echo": input.Prompt,
		"role": string(domain.RoleAssistant),
	}, nil
}

// NewPhase1Harness inicializa os contratos da Fase 1 prontos para validação.
func NewPhase1Harness() (*Phase1Harness, error) {
	reg := mcp.NewInMemoryRegistry()
	if err := reg.RegisterTool(&MockEchoTool{}); err != nil {
		return nil, fmt.Errorf("falha ao registrar echo tool: %w", err)
	}

	cfg := &config.Config{
		Env: "testing",
		Server: config.ServerConfig{
			Addr: ":8080",
		},
	}

	return &Phase1Harness{
		Registry: reg,
		Config:   cfg,
	}, nil
}

// DispatchMCPCall executa uma chamada MCP validando integridade de request e tool.
func (h *Phase1Harness) DispatchMCPCall(ctx context.Context, toolName string, args json.RawMessage) (interface{}, error) {
	tool, ok := h.Registry.GetTool(toolName)
	if !ok {
		return nil, errors.Wrap(errors.ErrModelNotFound, "TOOL_NOT_FOUND", fmt.Sprintf("tool '%s' nao encontrada", toolName), 404, -32601)
	}

	return tool.Execute(ctx, args)
}

// ValidateDomainSSE formata e valida a integridade de um evento SSE.
func ValidateDomainSSE(chunk domain.StreamChunk) ([]byte, error) {
	evt := domain.SSEEvent{
		Event: "completion",
		Data:  chunk,
	}
	return evt.Format()
}
