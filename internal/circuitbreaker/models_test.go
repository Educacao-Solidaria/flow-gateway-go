package circuitbreaker

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/resilience"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

func TestModelsIsolatesModelsAndCapsTheMap(t *testing.T) {
	m, err := NewModels(settings)
	require.NoError(t, err)
	for i := 0; i < settings.FailureThreshold; i++ {
		report, err := m.Allow("a")
		require.NoError(t, err)
		report(errDown)
	}
	_, err = m.Allow("a")
	assert.ErrorIs(t, err, gwerrors.ErrCircuitOpen)
	assert.Contains(t, err.Error(), "modelo a")
	_, err = m.Allow("b")
	assert.NoError(t, err, "cada modelo tem o seu disjuntor")
	assert.Nil(t, m.Breaker(strings.Repeat("x", maxModelLen+1)), "nome longo demais não vira chave")

	for i := len(m.breakers); i < maxModels; i++ {
		require.NotNil(t, m.Breaker(fmt.Sprint(i)))
	}
	assert.Nil(t, m.Breaker("inventado"), "o modelo vem da requisição: o mapa tem teto")
	report, err := m.Allow("inventado")
	require.NoError(t, err, "acima do teto, passa sem disjuntor")
	report(errDown)
}

// slow é um openrouter.Client em que "a" só termina pela janela do Fallback.
type slow struct{ calls []string }

func (s *slow) CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	s.calls = append(s.calls, req.Model)
	if req.Model == "a" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &domain.ChatCompletionResponse{Model: req.Model}, nil
}

func (s *slow) StreamChatCompletion(context.Context, *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	return nil, nil, nil
}

func TestModelsAsTheFallbackGate(t *testing.T) {
	m, err := NewModels(settings)
	require.NoError(t, err)
	up := &slow{}
	f, err := resilience.New(up, resilience.Options{AttemptTimeout: 10 * time.Millisecond, Gate: m, Fallbacks: map[string][]string{"a": {"b"}}})
	require.NoError(t, err)
	req := &domain.ChatCompletionRequest{Model: "a", Messages: []domain.ChatMessage{{Role: domain.RoleUser, Content: "oi"}}}
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 2*settings.FailureThreshold; i++ {
		_, err := f.CreateChatCompletion(gone, req)
		assert.ErrorIs(t, err, context.Canceled)
	}
	assert.Equal(t, Closed, m.Breaker("a").State(), "desistência do chamador não conta")

	for i := 0; i < settings.FailureThreshold+1; i++ {
		resp, err := f.CreateChatCompletion(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, "b", resp.Model)
	}
	assert.Equal(t, Open, m.Breaker("a").State(), "a janela vencida conta como timeout do upstream")
	assert.Equal(t, []string{"a", "b", "a", "b", "b"}, up.calls[2*settings.FailureThreshold:],
		"aberto o circuito, a cascata pula a sem esperar a janela")
}
