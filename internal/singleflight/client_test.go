package singleflight

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
)

// upstream é um openrouter.Client que segura cada chamada até release fechar.
type upstream struct {
	calls   atomic.Int32
	streams atomic.Int32
	entered chan struct{} // um sinal por chamada que chegou
	seen    chan []byte   // a requisição recebida, serializada depois da liberação
	release chan struct{}
}

func newUpstream() *upstream {
	return &upstream{entered: make(chan struct{}, 16), seen: make(chan []byte, 16), release: make(chan struct{})}
}

func (u *upstream) CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	u.calls.Add(1)
	u.entered <- struct{}{}
	select {
	case <-u.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	// Lida só depois da liberação: prova que a chamada usa a própria cópia.
	b, _ := json.Marshal(req)
	u.seen <- b
	return &domain.ChatCompletionResponse{ID: "gen-1", Model: req.Model, Choices: []domain.Choice{
		{Message: &domain.ChatMessage{Role: domain.RoleAssistant, Content: "eco: " + req.Messages[0].Content}},
	}}, nil
}

func (u *upstream) StreamChatCompletion(context.Context, *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	u.streams.Add(1)
	return nil, nil, nil
}

func request(tenant string) *domain.ChatCompletionRequest {
	return &domain.ChatCompletionRequest{
		Model: domain.ModelDeepSeekV3, TenantID: tenant,
		Messages: []domain.ChatMessage{{Role: domain.RoleUser, Content: "olá"}},
		Tools: []domain.Tool{{Type: "function", Function: domain.FunctionDefinition{
			Name: "busca", Parameters: map[string]interface{}{"maxLength": int64(9007199254740993)}, // > 2^53
		}}},
	}
}

func TestClientJoinsSameTenant(t *testing.T) {
	u := newUpstream()
	c := NewClient(u)
	out := make(chan *domain.ChatCompletionResponse, 2)
	call := func() {
		r, err := c.CreateChatCompletion(context.Background(), request("tenant-a"))
		assert.NoError(t, err)
		out <- r
	}
	go call()
	recv(t, u.entered)
	go call()
	waitFor(t, func() bool { return c.group.totalWaiters() == 2 })
	close(u.release)

	r1, r2 := recv(t, out), recv(t, out)
	require.NotNil(t, r1)
	require.NotNil(t, r2)
	assert.EqualValues(t, 1, u.calls.Load(), "uma chamada ao upstream para as duas")
	assert.Equal(t, r1, r2)
	r1.Choices[0].Message.Content = "alterado por um pós-processamento"
	assert.Equal(t, "eco: olá", r2.Choices[0].Message.Content, "cada chamador tem a própria cópia")

	want, err := json.Marshal(request("tenant-a"))
	require.NoError(t, err)
	assert.Equal(t, string(want), string(recv(t, u.seen)), "o upstream recebe a requisição intacta, inteiro grande inclusive")
}

func TestClientNeverJoinsAcrossTenants(t *testing.T) {
	u := newUpstream()
	c := NewClient(u)
	done := make(chan error, 2)
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		go func() {
			_, err := c.CreateChatCompletion(context.Background(), request(tenant))
			done <- err
		}()
	}
	recv(t, u.entered) // as duas chegam ao upstream sem uma esperar a outra
	recv(t, u.entered)
	close(u.release)
	require.NoError(t, recv(t, done))
	require.NoError(t, recv(t, done))
	assert.EqualValues(t, 2, u.calls.Load())
}

func TestClientPassesThroughWithoutTenantOrWithStream(t *testing.T) {
	u := newUpstream()
	c := NewClient(u)
	noTenant := request("")
	streaming := request("tenant-a")
	streaming.Stream = true
	done := make(chan error, 4)
	for _, req := range []*domain.ChatCompletionRequest{noTenant, noTenant, streaming, streaming} {
		go func() {
			_, err := c.CreateChatCompletion(context.Background(), req)
			done <- err
		}()
	}
	for i := 0; i < 4; i++ {
		recv(t, u.entered) // nenhuma esperou por outra
	}
	assert.Zero(t, c.group.inFlight())
	close(u.release)
	for i := 0; i < 4; i++ {
		require.NoError(t, recv(t, done))
	}

	_, _, err := c.StreamChatCompletion(context.Background(), streaming)
	require.NoError(t, err)
	assert.EqualValues(t, 1, u.streams.Load())
}

func TestClientLeaderGivingUpKeepsTheCall(t *testing.T) {
	u := newUpstream()
	c := NewClient(u)
	leaderReq := request("tenant-a")
	leaderCtx, giveUp := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := c.CreateChatCompletion(leaderCtx, leaderReq)
		leaderErr <- err
	}()
	recv(t, u.entered)
	joined := make(chan *domain.ChatCompletionResponse, 1)
	go func() {
		r, err := c.CreateChatCompletion(context.Background(), request("tenant-a"))
		assert.NoError(t, err)
		joined <- r
	}()
	waitFor(t, func() bool { return c.group.totalWaiters() == 2 })

	giveUp()
	assert.ErrorIs(t, recv(t, leaderErr), context.Canceled)
	leaderReq.Messages[0].Content = "mexido depois de desistir"
	close(u.release)
	r := recv(t, joined)
	require.NotNil(t, r)
	assert.Equal(t, "eco: olá", r.Choices[0].Message.Content)
	assert.EqualValues(t, 1, u.calls.Load())
}
