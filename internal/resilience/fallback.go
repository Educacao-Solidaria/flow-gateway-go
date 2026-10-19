// Package resilience isola o chamador de upstream lento ou instável: cada
// tentativa tem janela própria e, em timeout ou erro retentável, o tempo que
// sobra vai para a próxima alternativa. O fallback nativo da OpenRouter
// (`models`) só reage a erro, sem prazo por tentativa; cada tentativa daqui
// leva um modelo só (o contrato nem tem `models`).
package resilience

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/openrouter"
)

// errAttemptTimeout é a causa (context.Cause) do ctx da tentativa que estourou a
// janela: quem está embaixo (circuit breaker, log) a separa da desistência do chamador.
var errAttemptTimeout = fmt.Errorf("%w: janela da tentativa esgotada", gwerrors.ErrUpstreamTimeout)

// Options configura o Fallback.
type Options struct {
	// AttemptTimeout (obrigatório) é a janela de cada tentativa; no stream, até o primeiro chunk.
	AttemptTimeout time.Duration
	// Budget limita a soma das janelas; 0 deixa só as janelas e o prazo do chamador.
	Budget time.Duration
	// Fallbacks lista, por modelo pedido, as alternativas em ordem. Modelo
	// fora do mapa tem uma tentativa só. Quem serviu fica no Model da resposta.
	Fallbacks map[string][]string
}

// Fallback é um openrouter.Client que dá uma janela a cada tentativa e, em
// timeout ou erro retentável, cai para a próxima alternativa do modelo.
type Fallback struct {
	next openrouter.Client
	opts Options
	// time.Now e time.AfterFunc; os testes trocam por um relógio manual.
	now   func() time.Time
	after func(d time.Duration, fn func()) (stop func() bool)
}

var _ openrouter.Client = (*Fallback)(nil)

// New envolve next; as cascatas são copiadas.
func New(next openrouter.Client, opts Options) (*Fallback, error) {
	if next == nil || opts.AttemptTimeout <= 0 || opts.Budget < 0 {
		return nil, errors.New("resilience: next é obrigatório, AttemptTimeout positivo e Budget não negativo")
	}
	chains := make(map[string][]string, len(opts.Fallbacks))
	for model, alts := range opts.Fallbacks {
		if model == "" || slices.Contains(alts, "") {
			return nil, fmt.Errorf("resilience: modelo vazio na cascata de %q", model)
		}
		chains[model] = slices.Clone(alts)
	}
	opts.Fallbacks = chains
	return &Fallback{next: next, opts: opts, now: time.Now,
		after: func(d time.Duration, fn func()) func() bool { return time.AfterFunc(d, fn).Stop }}, nil
}

// CreateChatCompletion tenta o modelo pedido e, em timeout ou erro retentável,
// as alternativas dele. A tentativa que estoura a janela é cancelada antes de
// a próxima começar: nada roda em paralelo.
func (f *Fallback) CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	if req == nil {
		return f.next.CreateChatCompletion(ctx, req)
	}
	var last error
	begin := f.now()
	for i, model := range append([]string{req.Model}, f.opts.Fallbacks[req.Model]...) {
		window, err := f.window(begin, i, last)
		if err != nil {
			return nil, err
		}
		actx, cancel := context.WithCancelCause(ctx)
		stop := f.after(window, func() { cancel(errAttemptTimeout) })
		own := *req // cópia rasa: o upstream só lê, e a do chamador fica intacta
		own.Model = model
		resp, err := f.next.CreateChatCompletion(actx, &own)
		stop()
		cancel(nil)
		if err == nil { // a que chega junto com o fim da janela vale: já foi paga
			return resp, nil
		}
		if last, err = classify(ctx, actx, model, window, err); err != nil {
			return nil, err
		}
	}
	return nil, last
}

// StreamChatCompletion cascateia só até o primeiro chunk. Depois, o stream é do
// modelo que respondeu, até o fim ou até o chamador desistir: trocar no meio
// entregaria metade de uma resposta e metade de outra (a OpenRouter também não
// troca de provedor depois de saída parcial).
func (f *Fallback) StreamChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	if req == nil {
		return f.next.StreamChatCompletion(ctx, req)
	}
	var last error
	begin := f.now()
	for i, model := range append([]string{req.Model}, f.opts.Fallbacks[req.Model]...) {
		window, err := f.window(begin, i, last)
		if err != nil {
			return nil, nil, err
		}
		actx, cancel := context.WithCancelCause(ctx)
		stop := f.after(window, func() { cancel(errAttemptTimeout) })
		own := *req
		own.Model = model
		chunks, errs, err := f.next.StreamChatCompletion(actx, &own)
		if err == nil {
			first, got := domain.StreamChunk{}, false
			select {
			case first, got = <-chunks:
			case <-actx.Done():
			}
			// O primeiro chunk (ou o fim do stream, vazio) dentro da janela fixa
			// o modelo; o erro final, se houver, vai para o chamador.
			if got || actx.Err() == nil {
				if stop() {
					out, outErr := forward(ctx, func() { cancel(nil) }, got, first, chunks, errs)
					return out, outErr, nil
				}
				cancel(errAttemptTimeout) // a janela venceu, e o AfterFunc pode não ter rodado ainda
			}
			err = actx.Err()
		}
		stop()
		cancel(nil)
		if last, err = classify(ctx, actx, model, window, err); err != nil {
			return nil, nil, err
		}
	}
	return nil, nil, last
}

// window é a janela da tentativa i: AttemptTimeout ou o que sobra do
// orçamento, o que for menor; erro quando o orçamento acabou.
func (f *Fallback) window(begin time.Time, i int, last error) (time.Duration, error) {
	w := f.opts.AttemptTimeout
	if budget := f.opts.Budget; budget > 0 {
		left := budget - f.now().Sub(begin)
		if left <= 0 {
			// A última falha vai só como texto: embrulhada, um GatewayError dela
			// (ex.: 502) mudaria o status desta resposta, que é de timeout (504).
			return 0, fmt.Errorf("%w: orçamento de %s esgotado após %d tentativa(s) (última: %s)",
				gwerrors.ErrUpstreamTimeout, budget, i, fmt.Sprint(last))
		}
		w = min(w, left)
	}
	return w, nil
}

// classify decide o que fazer com o erro de uma tentativa: stop não nil encerra
// a cascata; senão, last é o erro (já traduzido) e a próxima alternativa segue.
func classify(ctx, actx context.Context, model string, window time.Duration, err error) (last, stop error) {
	if ctx.Err() != nil { // o chamador desistiu ou o prazo dele venceu: outra tentativa não serve a ninguém
		return nil, ctx.Err()
	}
	if errors.Is(context.Cause(actx), errAttemptTimeout) {
		err = fmt.Errorf("%w: %s não respondeu em %s", gwerrors.ErrUpstreamTimeout, model, window)
	}
	if !retryable(err) {
		return nil, err
	}
	return err, nil
}

// retryable diz se vale tentar outro modelo depois de err: prazo (do transporte
// ou do upstream), circuito aberto, rate limit e 5xx. Erro do cliente (4xx) não
// entra — a mesma requisição falharia de novo, e cada tentativa pode cobrar o
// prompt —, nem erro desconhecido: na dúvida, não se paga outra tentativa.
func retryable(err error) bool {
	var ne net.Error
	var ge *gwerrors.GatewayError
	switch {
	case errors.Is(err, gwerrors.ErrUpstreamTimeout), errors.Is(err, gwerrors.ErrCircuitOpen), errors.Is(err, gwerrors.ErrRateLimited):
		return true
	case errors.As(err, &ne) && ne.Timeout():
		return true
	case errors.As(err, &ge):
		return ge.HTTPStatus == http.StatusRequestTimeout || ge.HTTPStatus == http.StatusTooManyRequests || ge.HTTPStatus >= 500
	}
	return false
}

// forward entrega first (se got) e o resto do stream escolhido; done libera o
// ctx da tentativa quando o stream acaba ou o chamador desiste.
func forward(ctx context.Context, done func(), got bool, first domain.StreamChunk, in <-chan domain.StreamChunk, inErr <-chan error) (<-chan domain.StreamChunk, <-chan error) {
	out, outErr := make(chan domain.StreamChunk), make(chan error, 1)
	go func() {
		defer close(out)
		defer close(outErr)
		defer done()
		// O upstream fecha `in` quando o ctx (filho do do chamador) acaba.
		for c, ok := first, got; ok; c, ok = <-in {
			select {
			case out <- c:
			case <-ctx.Done():
				outErr <- ctx.Err()
				return
			}
		}
		select { // fechado é sucesso; nil (fora do contrato) espera o ctx
		case err := <-inErr:
			if err != nil {
				outErr <- err
			}
		case <-ctx.Done():
			outErr <- ctx.Err()
		}
	}()
	return out, outErr
}
