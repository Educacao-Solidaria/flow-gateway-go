package resilience

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/quota"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/openrouter"
)

// QuotaError é o erro que traz a cota lida da resposta do upstream
// (quota.Parse dos cabeçalhos e do JSON de erro): o Retry não tenta de novo
// antes de Quota().Wait. É o encaixe para o cliente HTTP da OpenRouter.
type QuotaError interface {
	error
	Quota() quota.Quota
}

// RetryPolicy configura o Retry.
type RetryPolicy struct {
	MaxAttempts int           // tentativas no total, a primeira inclusa (>= 1)
	BaseDelay   time.Duration // teto da 1ª espera, que dobra a cada tentativa (> 0)
	MaxDelay    time.Duration // teto de toda espera, o backoff truncado (BaseDelay..24h, como o quota)
	MaxElapsed  time.Duration // teto do tempo total, esperas inclusas; 0 = só MaxAttempts e o ctx
}

// Retry é um openrouter.Client que repete o mesmo pedido, ao mesmo modelo, em
// falha efêmera (o retryable do Fallback, menos o circuito aberto), esperando
// um tempo sorteado entre 0 e o backoff exponencial truncado (full jitter: os
// clientes que falharam juntos não voltam juntos). Para trocar de modelo,
// componha com o Fallback: New(NewRetry(cliente, p), opts) repete dentro da
// janela de cada modelo e, esgotada ela, a cascata segue.
type Retry struct {
	next   openrouter.Client
	p      RetryPolicy
	now    func() time.Time
	jitter func(n int64) int64 // sorteia em [0, n); os testes trocam
}

var _ openrouter.Client = (*Retry)(nil)

// NewRetry envolve next com a política p.
func NewRetry(next openrouter.Client, p RetryPolicy) (*Retry, error) {
	if next == nil || p.MaxAttempts < 1 || p.BaseDelay <= 0 || p.MaxDelay < p.BaseDelay || p.MaxDelay > 24*time.Hour || p.MaxElapsed < 0 {
		return nil, errors.New("resilience: next é obrigatório, MaxAttempts >= 1, 0 < BaseDelay <= MaxDelay <= 24h e MaxElapsed >= 0")
	}
	return &Retry{next: next, p: p, now: time.Now, jitter: rand.Int64N}, nil
}

// CreateChatCompletion repete o pedido enquanto a falha for efêmera e a política deixar.
func (r *Retry) CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	begin := r.now()
	for attempt := 0; ; attempt++ {
		resp, err := r.next.CreateChatCompletion(ctx, req)
		if err == nil {
			return resp, nil
		}
		if err = r.pause(ctx, begin, attempt, err); err != nil {
			return nil, err
		}
	}
}

// StreamChatCompletion repete só enquanto nenhum pedaço foi entregue: falha na
// abertura ou erro antes do 1º pedaço. Depois dele, o stream é esse até o fim
// (a mesma regra do Fallback): repetir entregaria parte da resposta duas vezes.
func (r *Retry) StreamChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	begin := r.now()
	for attempt := 0; ; attempt++ {
		actx, cancel := context.WithCancel(ctx)
		chunks, errs, err := r.next.StreamChatCompletion(actx, req)
		if err == nil {
			first, got := domain.StreamChunk{}, false
			select {
			case first, got = <-chunks:
			case <-ctx.Done():
				err = ctx.Err()
			}
			if !got && err == nil { // fechou sem pedaço: o erro final decide (nil = stream vazio, sucesso)
				select {
				case err = <-errs:
				case <-ctx.Done():
					err = ctx.Err()
				}
			}
			if got || err == nil {
				out, outErr := forward(ctx, cancel, got, first, chunks, errs)
				return out, outErr, nil
			}
		}
		cancel()
		if err = r.pause(ctx, begin, attempt, err); err != nil {
			return nil, nil, err
		}
	}
}

// pause decide se vale outra tentativa depois de err e espera por ela: um
// sorteio entre 0 e o backoff da tentativa, nunca menos que a espera da cota, e
// só se a espera couber no prazo do chamador e no MaxElapsed. Devolve nil para
// tentar de novo; senão, o erro que vai para o chamador.
func (r *Retry) pause(ctx context.Context, begin time.Time, attempt int, err error) error {
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case attempt+1 >= r.p.MaxAttempts, !retryable(err), errors.Is(err, gwerrors.ErrCircuitOpen):
		return err // circuito aberto não fecha em milissegundos: trocar de modelo é com o Fallback
	}
	ceil := r.p.BaseDelay
	for i := 0; i < attempt && ceil < r.p.MaxDelay; i++ {
		ceil *= 2
	}
	d := time.Duration(r.jitter(int64(min(ceil, r.p.MaxDelay)) + 1))
	now := r.now()
	var qe QuotaError
	if errors.As(err, &qe) {
		d = max(d, qe.Quota().Wait(now)) // Retry-After, ou a cota zerada até o reset
	}
	if dl, ok := ctx.Deadline(); ok && now.Add(d).After(dl) || r.p.MaxElapsed > 0 && now.Add(d).Sub(begin) > r.p.MaxElapsed {
		return err // a espera não cabe: o erro de agora vale mais que um timeout depois
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
