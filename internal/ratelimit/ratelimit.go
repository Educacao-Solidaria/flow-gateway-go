// Package ratelimit limita a taxa de requisições por chave — IP real do
// cliente ou API key — com token bucket.
//
// Cada balde segue a forma GCRA do token bucket: em vez de guardar "tokens" e
// "última recarga" (dois valores que precisariam de trava para mudar juntos),
// guarda só o instante teórico em que o balde volta a ficar cheio (TAT). Cabe
// num atomic.Int64 e muda por um laço de CAS, sem trava; o sync.Map também lê
// sem trava as chaves estáveis, então só chave nova passa por trava (a dele).
// Taxa e rajada são as de um token bucket de capacidade Burst recarregado a
// Rate por segundo.
package ratelimit

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// sweepEvery espaça as limpezas de baldes ociosos.
// ponytail: a limpeza roda no caminho da requisição que cria chave nova, no
// máximo uma vez por sweepEvery e em O(chaves); se pesar, vira uma goroutine
// no lifecycle.Supervisor.
const sweepEvery = time.Second

// dead marca o balde que a limpeza tirou do mapa: quem ainda o segura busca outro.
const dead = math.MinInt64

// Config dimensiona o limitador.
type Config struct {
	// Rate é a recarga em requisições por segundo; Burst, quantas passam de uma
	// vez com o balde cheio.
	Rate  float64
	Burst int
	// MaxKeys limita quantas chaves ficam em memória: cada IP ou API key nova
	// cria um balde, e sem teto quem gira chaves esgota a memória. Com o teto
	// atingido, chave nova é recusada até uma limpeza liberar vaga (as
	// limpezas são espaçadas por sweepEvery).
	MaxKeys int
}

type bucket struct {
	tat atomic.Int64 // ns (relógio do Limiter) em que o balde volta a ficar cheio; dead = removido
}

// Limiter é um rate limiter por chave, seguro para uso concorrente.
type Limiter struct {
	interval  int64        // ns para recarregar uma requisição
	tolerance int64        // (Burst-1)*interval: quanto o TAT pode estar à frente de agora
	maxKeys   int64        // teto de chaves (Config.MaxKeys)
	now       func() int64 // ns monotônicos desde o New; trocado nos testes

	buckets   sync.Map // chave -> *bucket
	keys      atomic.Int64
	nextSweep atomic.Int64
}

// New valida cfg e cria o limitador.
func New(cfg Config) (*Limiter, error) {
	if math.IsNaN(cfg.Rate) || math.IsInf(cfg.Rate, 0) || cfg.Rate <= 0 {
		return nil, errors.New("ratelimit: rate deve ser positivo e finito")
	}
	interval := float64(time.Second) / cfg.Rate
	switch {
	case interval < 1:
		return nil, errors.New("ratelimit: rate acima de 1e9 por segundo")
	case cfg.Burst < 1:
		return nil, errors.New("ratelimit: burst deve ser >= 1")
	case cfg.MaxKeys < 1:
		return nil, errors.New("ratelimit: max_keys deve ser >= 1")
	case float64(cfg.Burst)*interval > math.MaxInt64/4:
		return nil, errors.New("ratelimit: rate baixo demais para essa rajada")
	}
	base := time.Now()
	return &Limiter{
		interval:  int64(interval),
		tolerance: int64(cfg.Burst-1) * int64(interval),
		maxKeys:   int64(cfg.MaxKeys),
		now:       func() int64 { return int64(time.Since(base)) },
	}, nil
}

// Allow consome uma requisição do balde de key. Recusada, retryAfter diz em
// quanto tempo a próxima passa.
func (l *Limiter) Allow(key string) (ok bool, retryAfter time.Duration) {
	now := l.now()
	for {
		b := l.bucket(key, now)
		if b == nil {
			return false, sweepEvery // teto de chaves: espera a próxima limpeza
		}
		tat := b.tat.Load()
		if tat == dead {
			l.buckets.CompareAndDelete(key, b) // a limpeza o tirou; busca outro
			continue
		}
		start := max(tat, now)
		if start-now > l.tolerance {
			return false, time.Duration(start - l.tolerance - now)
		}
		if b.tat.CompareAndSwap(tat, start+l.interval) {
			return true, 0
		}
	}
}

// Hold segura key por d: até lá, Allow(key) recusa, com o retryAfter dessa
// espera. É o controle preventivo que a cota informada pelo upstream alimenta
// (internal/quota): o gateway recusa sozinho em vez de insistir num 429. d <= 0
// não faz nada; a espera só cresce (um Hold menor não encurta o anterior); com
// o teto de chaves atingido, a chave nova fica sem espera.
func (l *Limiter) Hold(key string, d time.Duration) {
	if d <= 0 {
		return
	}
	now := l.now()
	// Allow recusa enquanto o TAT passa de agora+tolerance; o teto em d evita estourar o int64.
	until := now + int64(min(d, math.MaxInt64/4)) + l.tolerance
	for {
		b := l.bucket(key, now)
		if b == nil {
			return
		}
		tat := b.tat.Load()
		if tat == dead {
			l.buckets.CompareAndDelete(key, b) // a limpeza o tirou; busca outro
			continue
		}
		if tat >= until || b.tat.CompareAndSwap(tat, until) {
			return
		}
	}
}

// bucket devolve o balde de key, criando um cheio se preciso; nil com o teto de
// chaves atingido.
func (l *Limiter) bucket(key string, now int64) *bucket {
	if b, ok := l.buckets.Load(key); ok {
		return b.(*bucket)
	}
	l.sweep(now)
	// ponytail: checar e inserir não é atômico, então o teto pode passar por até
	// o número de chaves novas inseridas ao mesmo tempo.
	if l.keys.Load() >= l.maxKeys {
		return nil
	}
	b := &bucket{}
	b.tat.Store(now)
	actual, loaded := l.buckets.LoadOrStore(key, b)
	if !loaded {
		l.keys.Add(1)
	}
	return actual.(*bucket)
}

// sweep tira do mapa os baldes cheios (TAT <= agora). Recriar um balde cheio dá
// no mesmo que mantê-lo, então a limpeza não muda decisão nenhuma; o CAS para
// dead garante que ninguém consuma de um balde já removido.
func (l *Limiter) sweep(now int64) {
	next := l.nextSweep.Load()
	if now < next || !l.nextSweep.CompareAndSwap(next, now+int64(sweepEvery)) {
		return
	}
	l.buckets.Range(func(k, v any) bool {
		b := v.(*bucket)
		if tat := b.tat.Load(); tat != dead && tat <= now && b.tat.CompareAndSwap(tat, dead) {
			l.buckets.CompareAndDelete(k, b)
			l.keys.Add(-1)
		}
		return true
	})
}

// Len devolve quantas chaves estão em memória.
func (l *Limiter) Len() int {
	return int(l.keys.Load())
}
