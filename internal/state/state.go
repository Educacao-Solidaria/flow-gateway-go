// Package state guarda estado volátil em memória com prazo de validade (TTL):
// sessões de streaming, retomada de SSE, chaves de idempotência. O item vale
// até o prazo e sai de dois jeitos: Get nunca o devolve depois do prazo, mesmo
// entre uma limpeza e outra (expiração preguiçosa), e a limpeza periódica de
// Run, num time.Ticker, tira da memória os vencidos que ninguém mais lê.
package state

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// MaxKeyLen limita o tamanho da chave. Junto com o teto de itens, limita a
// memória quando a chave vem de fora (id de sessão, Last-Event-ID).
const MaxKeyLen = 256

// minCleanupEvery impede que um intervalo absurdo vire laço quente na trava.
const minCleanupEvery = time.Millisecond

// ErrFull é devolvido por Set para chave nova com o teto de itens atingido.
var ErrFull = errors.New("state: teto de itens atingido")

var (
	errKey = errors.New("state: chave vazia ou acima de MaxKeyLen bytes")
	errTTL = errors.New("state: ttl deve ser positivo")
)

// Config dimensiona o Store.
type Config struct {
	// MaxItems limita quantos itens ficam em memória. Um vencido conta até a
	// limpeza de Run tirá-lo (ou até Set na mesma chave, ou Delete), então
	// dimensione pela vazão de chaves novas × (TTL + CleanupEvery).
	MaxItems int
	// CleanupEvery espaça as limpezas de Run (mínimo 1ms).
	CleanupEvery time.Duration
}

type entry[V any] struct {
	val V
	exp int64 // instante (relógio do Store, ns) a partir do qual o item venceu
}

// Store é um mapa chave → valor com prazo, seguro para uso concorrente; crie
// com New (o valor zero não serve). Guarda V como recebido (cópia rasa): um
// ponteiro guardado continua compartilhado.
type Store[V any] struct {
	maxItems int
	every    time.Duration
	now      func() int64 // ns monotônicos desde o New; trocado nos testes

	mu    sync.RWMutex
	items map[string]entry[V]
}

// New valida cfg e cria um Store vazio. A limpeza periódica só roda com Run.
func New[V any](cfg Config) (*Store[V], error) {
	switch {
	case cfg.MaxItems < 1:
		return nil, errors.New("state: max_items deve ser >= 1")
	case cfg.CleanupEvery < minCleanupEvery:
		return nil, errors.New("state: cleanup_every deve ser >= 1ms")
	}
	base := time.Now()
	return &Store[V]{
		maxItems: cfg.MaxItems,
		every:    cfg.CleanupEvery,
		now:      func() int64 { return int64(time.Since(base)) },
		items:    map[string]entry[V]{},
	}, nil
}

// Set grava v em key por ttl, substituindo valor e prazo anteriores. Chave nova
// com o teto atingido devolve ErrFull; sobrescrever não conta como chave nova.
func (s *Store[V]) Set(key string, v V, ttl time.Duration) error {
	if key == "" || len(key) > MaxKeyLen {
		return errKey
	}
	if ttl <= 0 {
		return errTTL
	}
	now := s.now()
	exp := int64(math.MaxInt64) // ttl gigante satura em vez de estourar para o passado
	if int64(ttl) < math.MaxInt64-now {
		exp = now + int64(ttl)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[key]; !ok && len(s.items) >= s.maxItems {
		return ErrFull
	}
	s.items[key] = entry[V]{val: v, exp: exp}
	return nil
}

// Get devolve o valor de key se ele ainda estiver no prazo. Item vencido não
// volta nunca, mesmo antes de a limpeza tirá-lo da memória.
func (s *Store[V]) Get(key string) (V, bool) {
	s.mu.RLock()
	e, ok := s.items[key]
	s.mu.RUnlock()
	if !ok || s.now() >= e.exp {
		var zero V
		return zero, false
	}
	return e.val, true
}

// Delete apaga key; chave ausente não é erro.
func (s *Store[V]) Delete(key string) {
	s.mu.Lock()
	delete(s.items, key)
	s.mu.Unlock()
}

// Len conta os itens em memória, inclusive os vencidos que a limpeza ainda não
// tirou: é o número que o teto de Config.MaxItems confere.
func (s *Store[V]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

// Run tira os vencidos da memória a cada Config.CleanupEvery até ctx acabar e
// então devolve ctx.Err(). O lugar dele é o lifecycle.Supervisor —
// sup.Go("state.limpeza", st.Run) —, cujo Shutdown cancela ctx e espera a
// volta. Sem Run nada vence errado (Get confere o prazo), mas os vencidos ficam
// na memória, contando no teto, até serem sobrescritos ou apagados.
func (s *Store[V]) Run(ctx context.Context) error {
	t := time.NewTicker(s.every)
	defer t.Stop()
	return s.run(ctx, t.C)
}

func (s *Store[V]) run(ctx context.Context, tick <-chan time.Time) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick:
			s.sweep()
		}
	}
}

// sweep apaga os itens vencidos.
// ponytail: varre o mapa inteiro sob a trava, O(itens) por intervalo, com teto
// conhecido (MaxItems); se pesar, varrer em lotes soltando a trava entre eles
// ou indexar os itens por prazo (heap).
func (s *Store[V]) sweep() {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.items {
		if now >= e.exp {
			delete(s.items, k)
		}
	}
}
