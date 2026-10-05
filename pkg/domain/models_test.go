package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
)

func TestChatCompletionRequest_Serialization(t *testing.T) {
	req := domain.ChatCompletionRequest{
		Model: domain.ModelDeepSeekV3,
		Messages: []domain.ChatMessage{
			{Role: domain.RoleSystem, Content: "Voce e um assistente util."},
			{Role: domain.RoleUser, Content: "Ola mundo"},
		},
		Temperature: 0.7,
		Stream:      true,
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("erro ao serializar request: %v", err)
	}

	var parsed domain.ChatCompletionRequest
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("erro ao desserializar request: %v", err)
	}

	if parsed.Model != domain.ModelDeepSeekV3 {
		t.Fatalf("modelo incorreto: %s", parsed.Model)
	}
	if len(parsed.Messages) != 2 {
		t.Fatalf("esperava 2 mensagens, obteve %d", len(parsed.Messages))
	}
}

func TestSSEEvent_Format(t *testing.T) {
	chunk := domain.StreamChunk{
		ID:           "chk-123",
		Model:        domain.ModelDeepSeekV3,
		DeltaContent: "Ola",
		IsLast:       false,
	}

	event := domain.SSEEvent{
		Event: "message",
		Data:  chunk,
	}

	formatted, err := event.Format()
	if err != nil {
		t.Fatalf("erro ao formatar SSE: %v", err)
	}

	str := string(formatted)
	if !strings.HasPrefix(str, "event: message\n") {
		t.Fatalf("prefixo de evento invalido: %s", str)
	}
	if !strings.Contains(str, `"delta_content":"Ola"`) {
		t.Fatalf("conteudo nao encontrado: %s", str)
	}
	if !strings.HasSuffix(str, "\n\n") {
		t.Fatalf("deve terminar com delimitador duplo de nova linha")
	}
}
