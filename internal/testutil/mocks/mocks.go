// Package mocks traz mocks testify (expectativa de chamada e argumentos) dos
// contratos de cliente do gateway. Complementam os fakes com estado de cada
// pacote (cache.InMemoryClient, rag.InMemoryClient, openrouter.MockClient):
// use o fake para comportamento e o mock para afirmar como o cliente foi chamado.
package mocks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/rag"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/openrouter"
)

var (
	_ openrouter.Client = (*OpenRouter)(nil)
	_ cache.Client      = (*Cache)(nil)
	_ rag.Client        = (*RAG)(nil)
)

// expect cria o mock e confere as expectativas no fim do teste.
func expect[T interface{ AssertExpectations(mock.TestingT) bool }](t testing.TB, m T) T {
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

// get devolve o valor de retorno i como T, ou o zero de T se for nil.
func get[T any](args mock.Arguments, i int) T {
	v, _ := args.Get(i).(T)
	return v
}

// OpenRouter é o mock de openrouter.Client.
type OpenRouter struct{ mock.Mock }

// NewOpenRouter cria o mock e registra AssertExpectations no t.Cleanup.
func NewOpenRouter(t testing.TB) *OpenRouter { return expect(t, &OpenRouter{}) }

func (m *OpenRouter) CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	args := m.Called(ctx, req)
	return get[*domain.ChatCompletionResponse](args, 0), args.Error(1)
}

// StreamChatCompletion aceita nos retornos tanto chan quanto <-chan.
func (m *OpenRouter) StreamChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	args := m.Called(ctx, req)
	return recvChan[domain.StreamChunk](args.Get(0)), recvChan[error](args.Get(1)), args.Error(2)
}

func recvChan[T any](v any) <-chan T {
	if c, ok := v.(chan T); ok {
		return c
	}
	c, _ := v.(<-chan T)
	return c
}

// Cache é o mock de cache.Client.
type Cache struct{ mock.Mock }

// NewCache cria o mock e registra AssertExpectations no t.Cleanup.
func NewCache(t testing.TB) *Cache { return expect(t, &Cache{}) }

func (m *Cache) Lookup(ctx context.Context, prompt string, embedding []float32, threshold float32) (*cache.LookupResult, error) {
	args := m.Called(ctx, prompt, embedding, threshold)
	return get[*cache.LookupResult](args, 0), args.Error(1)
}

func (m *Cache) Store(ctx context.Context, prompt, response string, embedding []float32, ttlSeconds int) error {
	return m.Called(ctx, prompt, response, embedding, ttlSeconds).Error(0)
}

func (m *Cache) Stats(ctx context.Context) (*cache.StatsResult, error) {
	args := m.Called(ctx)
	return get[*cache.StatsResult](args, 0), args.Error(1)
}

func (m *Cache) Close() error { return m.Called().Error(0) }

// RAG é o mock de rag.Client.
type RAG struct{ mock.Mock }

// NewRAG cria o mock e registra AssertExpectations no t.Cleanup.
func NewRAG(t testing.TB) *RAG { return expect(t, &RAG{}) }

func (m *RAG) HybridSearch(ctx context.Context, query string, topK int, alpha float32) (*rag.SearchResult, error) {
	args := m.Called(ctx, query, topK, alpha)
	return get[*rag.SearchResult](args, 0), args.Error(1)
}

func (m *RAG) IndexDocument(ctx context.Context, title, content, uri string) (*rag.IndexResult, error) {
	args := m.Called(ctx, title, content, uri)
	return get[*rag.IndexResult](args, 0), args.Error(1)
}

func (m *RAG) Close() error { return m.Called().Error(0) }
