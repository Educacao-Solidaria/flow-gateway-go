// Package circuitbreaker é um disjuntor de três estados para o upstream.
// Fechado: tudo passa; FailureThreshold falhas seguidas abrem. Aberto: nada passa
// até OpenTimeout vencer, o que acontece na primeira chamada depois do prazo (sem
// timer nem goroutine). Meio-aberto: até HalfOpenProbes sondas por vez; esse
// número de sucessos fecha, uma falha reabre.
package circuitbreaker

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

// State é o estado do disjuntor.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	if names := [...]string{"closed", "open", "half-open"}; s >= 0 && int(s) < len(names) {
		return names[s]
	}
	return "unknown"
}

// Settings configura o disjuntor; os três números são obrigatórios.
type Settings struct {
	FailureThreshold int           // falhas seguidas que abrem o circuito fechado
	OpenTimeout      time.Duration // quanto o aberto espera antes de deixar sondas passarem
	HalfOpenProbes   int           // sondas simultâneas no meio-aberto e sucessos que o fecham
	// OnStateChange, se não for nil, recebe cada transição (log, métrica). Roda
	// sob o lock e em ordem: precisa ser rápido e não pode chamar o disjuntor.
	OnStateChange func(name string, from, to State)
}

func (s Settings) validate() error {
	if s.FailureThreshold < 1 || s.OpenTimeout <= 0 || s.HalfOpenProbes < 1 {
		return errors.New("circuitbreaker: FailureThreshold e HalfOpenProbes precisam ser >= 1 e OpenTimeout positivo")
	}
	return nil
}

// Breaker é o disjuntor, seguro para uso concorrente.
type Breaker struct {
	name      string
	s         Settings
	now       func() time.Time // time.Now; os testes trocam por um relógio manual
	mu        sync.Mutex
	state     State
	gen       uint64 // muda a cada transição: resultado de outra época não conta
	failures  int    // seguidas, no fechado
	successes int    // seguidas, no meio-aberto
	probes    int    // sondas em voo, no meio-aberto
	openedAt  time.Time
}

// New cria um disjuntor fechado; name o identifica no OnStateChange.
func New(name string, s Settings) (*Breaker, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &Breaker{name: name, s: s, now: time.Now}, nil
}

// State devolve o estado atual (o aberto vencido já aparece meio-aberto).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.expire()
	return b.state
}

// Allow pede passagem: com o circuito aberto, ou o meio-aberto cheio de sondas,
// devolve gwerrors.ErrCircuitOpen. Senão, done recebe o resultado da chamada uma
// vez (as seguintes são ignoradas): context.Canceled quando o chamador acabou
// (desistiu ou o prazo dele venceu), que não conta; um prazo próprio que corta
// a chamada vai como a causa (ex.: ErrUpstreamTimeout), que conta.
func (b *Breaker) Allow() (done func(err error), err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.expire()
	if b.state == Open || b.state == HalfOpen && b.probes >= b.s.HalfOpenProbes {
		return nil, gwerrors.ErrCircuitOpen
	}
	if b.state == HalfOpen {
		b.probes++
	}
	gen := b.gen
	var once atomic.Bool
	return func(err error) {
		if once.CompareAndSwap(false, true) {
			b.record(gen, err)
		}
	}, nil
}

func (b *Breaker) record(gen uint64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if gen != b.gen {
		return // o estado mudou desde o Allow: o resultado é de outra época
	}
	if b.state == HalfOpen {
		b.probes--
	}
	switch outcome(err) {
	case success:
		b.failures = 0
		if b.state == HalfOpen {
			if b.successes++; b.successes >= b.s.HalfOpenProbes {
				b.set(Closed)
			}
		}
	case failure:
		if b.failures++; b.state == HalfOpen || b.failures >= b.s.FailureThreshold {
			b.set(Open)
		}
	}
}

// expire leva o aberto vencido ao meio-aberto; chamado sob o lock.
func (b *Breaker) expire() {
	if b.state == Open && b.now().Sub(b.openedAt) >= b.s.OpenTimeout {
		b.set(HalfOpen)
	}
}

// set muda o estado, zera os contadores e avisa o gancho (sob o lock).
func (b *Breaker) set(to State) {
	from := b.state
	b.state, b.gen = to, b.gen+1
	b.failures, b.successes, b.probes = 0, 0, 0
	if to == Open {
		b.openedAt = b.now()
	}
	if b.s.OnStateChange != nil {
		b.s.OnStateChange(b.name, from, to)
	}
}

type result int

const (
	ignored result = iota
	success
	failure
)

// outcome classifica o resultado de uma chamada. Sucesso: sem erro, ou erro do
// cliente (4xx menos 408 e 429), porque o upstream respondeu. Ignorado:
// context.Canceled, o chamador que acabou. Falha: o resto, inclusive timeout e
// erro desconhecido — DeadlineExceeded conta, porque o timeout de dial do net
// também casa com ele.
func outcome(err error) result {
	var ge *gwerrors.GatewayError
	switch {
	case err == nil:
		return success
	case errors.Is(err, context.Canceled):
		return ignored
	case errors.As(err, &ge) && ge.HTTPStatus >= 400 && ge.HTTPStatus < 500 &&
		ge.HTTPStatus != http.StatusRequestTimeout && ge.HTTPStatus != http.StatusTooManyRequests:
		return success
	}
	return failure
}
