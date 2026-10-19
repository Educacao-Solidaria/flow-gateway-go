package circuitbreaker

import (
	"fmt"
	"sync"
	"time"
)

// O modelo vem da requisição: sem teto, o mapa cresceria com nomes inventados
// (e longos). ponytail: acima de maxModels disjuntores, ou com nome maior que
// maxModelLen, o modelo passa sem disjuntor; se o catálogo passar disso, LRU.
const maxModels, maxModelLen = 1024, 256

// Models mantém um disjuntor por modelo e é o resilience.Gate do Fallback:
// circuito aberto pula o modelo sem gastar a janela da tentativa.
type Models struct {
	s        Settings
	mu       sync.Mutex
	breakers map[string]*Breaker
}

// NewModels cria o conjunto; cada modelo ganha o disjuntor na primeira chamada.
func NewModels(s Settings) (*Models, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &Models{s: s, breakers: map[string]*Breaker{}}, nil
}

// Breaker devolve o disjuntor do modelo, criando-o na primeira vez (serve a
// métrica e health); nil acima dos tetos.
func (m *Models) Breaker(model string) *Breaker {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.breakers[model]
	if !ok && len(m.breakers) < maxModels && len(model) <= maxModelLen {
		b = &Breaker{name: model, s: m.s, now: time.Now}
		m.breakers[model] = b
	}
	return b
}

// Allow pede passagem ao disjuntor do modelo; acima dos tetos, passa sem.
func (m *Models) Allow(model string) (report func(error), err error) {
	b := m.Breaker(model)
	if b == nil {
		return func(error) {}, nil
	}
	if report, err = b.Allow(); err != nil {
		return nil, fmt.Errorf("%w: modelo %s", err, model)
	}
	return report, nil
}
