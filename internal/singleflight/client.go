package singleflight

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/openrouter"
)

// Client é um openrouter.Client que junta numa chamada só ao upstream as
// requisições idênticas em voo do mesmo tenant.
//
// Só entra na junção a requisição sem stream e com TenantID: sem tenant não há
// como garantir que a resposta vá só para quem a pediu, e um stream tem um
// consumidor só — compartilhá-lo é trabalho do muxer de SSE. O TenantID
// precisa vir da autenticação, nunca do corpo enviado pelo cliente.
// Temperatura > 0 não impede a junção: quem pediu a mesma coisa ao mesmo tempo
// recebe a mesma amostra.
type Client struct {
	next  openrouter.Client
	group Group[[]byte]
}

var _ openrouter.Client = (*Client)(nil)

// NewClient envolve next.
func NewClient(next openrouter.Client) *Client {
	return &Client{next: next}
}

// CreateChatCompletion junta a chamada com uma idêntica em voo do mesmo
// tenant. O upstream recebe uma cópia própria da requisição (quem desiste pode
// mexer na sua) e cada chamador recebe uma cópia própria da resposta (mexer na
// sua não muda a dos outros).
func (c *Client) CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	if req == nil || req.Stream || req.TenantID == "" {
		return c.next.CreateChatCompletion(ctx, req)
	}
	body, err := json.Marshal(req)
	if err != nil { // ex.: NaN na temperatura; o upstream dá o erro dele
		return c.next.CreateChatCompletion(ctx, req)
	}
	sum := sha256.Sum256(body)
	// O tenant entra cru na chave, além de dentro do hash: tenants diferentes
	// nunca caem na mesma chamada, nem por colisão.
	key := req.TenantID + "\x00" + string(sum[:])
	raw, err := c.group.Do(ctx, key, func(ctx context.Context) ([]byte, error) {
		var own domain.ChatCompletionRequest
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber() // números dos schemas das tools voltam idênticos, sem passar por float64
		if err := dec.Decode(&own); err != nil {
			return nil, fmt.Errorf("singleflight: copiando a requisição: %w", err)
		}
		resp, err := c.next.CreateChatCompletion(ctx, &own)
		if err != nil || resp == nil {
			return nil, err
		}
		return json.Marshal(resp)
	})
	if err != nil || raw == nil {
		return nil, err
	}
	var resp domain.ChatCompletionResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("singleflight: copiando a resposta: %w", err)
	}
	return &resp, nil
}

// StreamChatCompletion não junta nada: um stream tem um consumidor só.
func (c *Client) StreamChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	return c.next.StreamChatCompletion(ctx, req)
}
