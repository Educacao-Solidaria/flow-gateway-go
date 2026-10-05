package openrouter

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
)

var (
	ErrEmptyModel    = errors.New("modelo nao pode ser vazio")
	ErrEmptyMessages = errors.New("a requisicao deve conter pelo menos uma mensagem")
	ErrClientClosed  = errors.New("cliente openrouter encerrado")
)

// Client define o contrato canônico para interação com o upstream OpenRouter.
type Client interface {
	CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error)
	StreamChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error)
}

// MockClient fornece uma implementação simulada concorrente e determinística para testes.
type MockClient struct {
	mu           sync.RWMutex
	customReply  string
	failRequests bool
	requestCount int
}

// NewMockClient instancia um mock client configurável.
func NewMockClient(defaultReply string) *MockClient {
	if defaultReply == "" {
		defaultReply = "Resposta simulada do modelo upstream OpenRouter."
	}
	return &MockClient{
		customReply: defaultReply,
	}
}

// SetFailRequests força o mock a retornar erro em todas as requisições subsequentes.
func (m *MockClient) SetFailRequests(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failRequests = fail
}

// GetRequestCount retorna a contagem de requisições recebidas.
func (m *MockClient) GetRequestCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.requestCount
}

// CreateChatCompletion simula uma chamada síncrona retornando ChatCompletionResponse completo.
func (m *MockClient) CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.requestCount++
	fail := m.failRequests
	reply := m.customReply
	m.mu.Unlock()

	if fail {
		return nil, errors.New("falha simulada ao chamar upstream openrouter")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	content := reply
	return &domain.ChatCompletionResponse{
		ID:      "mock-chatcmpl-001",
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   req.Model,
		Choices: []domain.Choice{
			{
				Index: 0,
				Message: &domain.ChatMessage{
					Role:    domain.RoleAssistant,
					Content: content,
				},
				FinishReason: "stop",
			},
		},
		Usage: &domain.Usage{
			PromptTokens:     10,
			CompletionTokens: len(strings.Fields(content)),
			TotalTokens:      10 + len(strings.Fields(content)),
		},
	}, nil
}

// StreamChatCompletion simula o streaming SSE gerando chunks parciais via canal assíncrono.
func (m *MockClient) StreamChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	if err := validateRequest(req); err != nil {
		return nil, nil, err
	}

	m.mu.Lock()
	m.requestCount++
	fail := m.failRequests
	reply := m.customReply
	m.mu.Unlock()

	if fail {
		return nil, nil, errors.New("falha simulada ao iniciar streaming openrouter")
	}

	chunkChan := make(chan domain.StreamChunk, 10)
	errChan := make(chan error, 1)

	go func() {
		defer close(chunkChan)
		defer close(errChan)

		words := strings.Fields(reply)
		for i, word := range words {
			isLast := i == len(words)-1
			chunk := domain.StreamChunk{
				ID:           "chunk-mock",
				Model:        req.Model,
				DeltaContent: word + " ",
				IsLast:       isLast,
			}
			if isLast {
				chunk.FinishReason = "stop"
			}

			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			case chunkChan <- chunk:
			}
		}
	}()

	return chunkChan, errChan, nil
}

func validateRequest(req *domain.ChatCompletionRequest) error {
	if req == nil {
		return errors.New("requisicao nula")
	}
	if strings.TrimSpace(req.Model) == "" {
		return ErrEmptyModel
	}
	if len(req.Messages) == 0 {
		return ErrEmptyMessages
	}
	return nil
}
