package openrouter_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/openrouter"
)

func TestMockClient_CreateChatCompletion_Success(t *testing.T) {
	client := openrouter.NewMockClient("Resposta de teste para LLM")
	req := &domain.ChatCompletionRequest{
		Model: domain.ModelDeepSeekV3,
		Messages: []domain.ChatMessage{
			{Role: domain.RoleUser, Content: "Ola gateway"},
		},
	}

	resp, err := client.CreateChatCompletion(context.Background(), req)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}

	if resp.Model != domain.ModelDeepSeekV3 {
		t.Fatalf("esperava model %s, obteve %s", domain.ModelDeepSeekV3, resp.Model)
	}
	if len(resp.Choices) != 1 {
		t.Fatalf("esperava 1 choice, obteve %d", len(resp.Choices))
	}
	if resp.Choices[0].Message.Content != "Resposta de teste para LLM" {
		t.Fatalf("conteudo inesperado: %s", resp.Choices[0].Message.Content)
	}
	if client.GetRequestCount() != 1 {
		t.Fatalf("esperava requestCount 1, obteve %d", client.GetRequestCount())
	}
}

func TestMockClient_StreamChatCompletion_Success(t *testing.T) {
	client := openrouter.NewMockClient("palavra1 palavra2 palavra3")
	req := &domain.ChatCompletionRequest{
		Model: domain.ModelQwen25,
		Messages: []domain.ChatMessage{
			{Role: domain.RoleUser, Content: "Stream test"},
		},
		Stream: true,
	}

	chunks, errs, err := client.StreamChatCompletion(context.Background(), req)
	if err != nil {
		t.Fatalf("erro ao iniciar stream: %v", err)
	}

	var collected []string
	for chunk := range chunks {
		collected = append(collected, strings.TrimSpace(chunk.DeltaContent))
		if chunk.IsLast && chunk.FinishReason != "stop" {
			t.Fatalf("esperava finishReason stop no ultimo chunk")
		}
	}

	select {
	case streamErr := <-errs:
		if streamErr != nil {
			t.Fatalf("erro no canal de streaming: %v", streamErr)
		}
	default:
	}

	if len(collected) != 3 {
		t.Fatalf("esperava 3 chunks, obteve %d (%v)", len(collected), collected)
	}
}

func TestMockClient_ValidationErrors(t *testing.T) {
	client := openrouter.NewMockClient("")

	// Modelo vazio
	_, err := client.CreateChatCompletion(context.Background(), &domain.ChatCompletionRequest{})
	if !errors.Is(err, openrouter.ErrEmptyModel) {
		t.Fatalf("esperava ErrEmptyModel, obteve: %v", err)
	}

	// Sem mensagens
	_, err = client.CreateChatCompletion(context.Background(), &domain.ChatCompletionRequest{
		Model: domain.ModelDeepSeekR1,
	})
	if !errors.Is(err, openrouter.ErrEmptyMessages) {
		t.Fatalf("esperava ErrEmptyMessages, obteve: %v", err)
	}
}

func TestMockClient_ContextCancellation(t *testing.T) {
	client := openrouter.NewMockClient("mensagem longa com varias palavras para streaming demorado")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancela imediatamente

	req := &domain.ChatCompletionRequest{
		Model: domain.ModelDeepSeekV3,
		Messages: []domain.ChatMessage{
			{Role: domain.RoleUser, Content: "teste"},
		},
	}

	_, err := client.CreateChatCompletion(ctx, req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("esperava context.Canceled, obteve: %v", err)
	}
}

func TestMockClient_FailureSimulation(t *testing.T) {
	client := openrouter.NewMockClient("")
	client.SetFailRequests(true)

	req := &domain.ChatCompletionRequest{
		Model: domain.ModelDeepSeekV3,
		Messages: []domain.ChatMessage{
			{Role: domain.RoleUser, Content: "teste"},
		},
	}

	_, err := client.CreateChatCompletion(context.Background(), req)
	if err == nil {
		t.Fatal("esperava erro com SetFailRequests ativo")
	}

	_, _, err = client.StreamChatCompletion(context.Background(), req)
	if err == nil {
		t.Fatal("esperava erro no streaming com SetFailRequests ativo")
	}
}
