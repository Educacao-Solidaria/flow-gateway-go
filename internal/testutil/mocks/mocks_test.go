package mocks

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/rag"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
)

func TestOpenRouterMock(t *testing.T) {
	m := NewOpenRouter(t)
	req := &domain.ChatCompletionRequest{Model: "openai/gpt-4o"}
	want := &domain.ChatCompletionResponse{ID: "gen-1"}
	m.On("CreateChatCompletion", mock.Anything, req).Return(want, nil).Once()

	chunks := make(chan domain.StreamChunk, 1) // chan bidirecional também serve
	chunks <- domain.StreamChunk{ID: "c1"}
	close(chunks)
	m.On("StreamChatCompletion", mock.Anything, req).Return(chunks, nil, nil).Once()

	got, err := m.CreateChatCompletion(context.Background(), req)
	require.NoError(t, err)
	assert.Same(t, want, got)

	out, errs, err := m.StreamChatCompletion(context.Background(), req)
	require.NoError(t, err)
	assert.Nil(t, errs)
	assert.Equal(t, "c1", (<-out).ID)
}

func TestOpenRouterMockNilResultWithError(t *testing.T) {
	m := NewOpenRouter(t)
	boom := errors.New("upstream 502")
	m.On("CreateChatCompletion", mock.Anything, mock.Anything).Return(nil, boom)

	got, err := m.CreateChatCompletion(context.Background(), &domain.ChatCompletionRequest{})
	assert.Nil(t, got)
	assert.ErrorIs(t, err, boom)
}

func TestCacheMock(t *testing.T) {
	m := NewCache(t)
	emb := []float32{0.1, 0.2}
	m.On("Lookup", mock.Anything, "oi", emb, float32(0.9)).Return(&cache.LookupResult{Hit: true}, nil)
	m.On("Store", mock.Anything, "oi", "olá", emb, 60).Return(nil)
	m.On("Stats", mock.Anything).Return(nil, errors.New("indisponível"))
	m.On("Close").Return(nil)

	res, err := m.Lookup(context.Background(), "oi", emb, 0.9)
	require.NoError(t, err)
	assert.True(t, res.Hit)
	require.NoError(t, m.Store(context.Background(), "oi", "olá", emb, 60))
	stats, err := m.Stats(context.Background())
	assert.Nil(t, stats)
	require.Error(t, err)
	require.NoError(t, m.Close())
}

func TestRAGMock(t *testing.T) {
	m := NewRAG(t)
	m.On("HybridSearch", mock.Anything, "go", 5, float32(0.5)).Return(&rag.SearchResult{TotalFound: 2}, nil)
	m.On("IndexDocument", mock.Anything, "t", "c", "u").Return(&rag.IndexResult{Success: true}, nil)
	m.On("Close").Return(nil)

	sr, err := m.HybridSearch(context.Background(), "go", 5, 0.5)
	require.NoError(t, err)
	assert.Equal(t, 2, sr.TotalFound)
	ir, err := m.IndexDocument(context.Background(), "t", "c", "u")
	require.NoError(t, err)
	assert.True(t, ir.Success)
	require.NoError(t, m.Close())
}

// spyT registra a falha sem derrubar o teste que o usa.
type spyT struct{ failed bool }

func (s *spyT) Logf(string, ...any)   {}
func (s *spyT) Errorf(string, ...any) { s.failed = true }
func (s *spyT) FailNow()              { s.failed = true }

func TestUnmetExpectationFailsTest(t *testing.T) {
	fake := &spyT{}
	m := &RAG{}
	m.On("Close").Return(nil)
	assert.False(t, m.AssertExpectations(fake))
	assert.True(t, fake.failed, "expectativa não cumprida deve reprovar")
}
