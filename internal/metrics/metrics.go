// Package metrics mede as requisições sem trava: a latência num histograma de
// contadores sync/atomic, a latência acumulada e um gauge de requisições
// ativas, mais o middleware HTTP que os alimenta. Gateway (prometheus.go) põe
// os rótulos do contrato da Fase 4 por cima disso e escreve o formato texto do
// Prometheus (docs/observability.md).
package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// bounds são os limites superiores (inclusivos) dos buckets, do milissegundo de
// um healthcheck aos minutos de um stream de LLM. Contêm os buckets do
// Prometheus (promBuckets), inclusive os 800 ms do SLA de TTFT.
var bounds = [...]time.Duration{
	time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond,
	50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
	800 * time.Millisecond, time.Second, 2500 * time.Millisecond, 5 * time.Second, 10 * time.Second,
	30 * time.Second, time.Minute, 2 * time.Minute, 5 * time.Minute,
}

// Bounds devolve uma cópia dos limites superiores (inclusivos) dos buckets; o
// último bucket do Snapshot conta o que passa do maior.
func Bounds() [len(bounds)]time.Duration { return bounds }

// Snapshot é uma leitura dos contadores, com consistência aproximada: cada
// campo é lido atomicamente, mas não todos no mesmo instante, então, com
// requisições em voo, SumMicros e Active podem estar algumas observações à
// frente ou atrás dos Buckets. Count é a soma dos Buckets: o histograma é
// coerente consigo mesmo.
type Snapshot struct {
	Active    int64  // requisições em andamento
	Count     uint64 // requisições concluídas
	SumMicros uint64 // latência acumulada; em ns, o int64 estouraria em dias de streams longos
	// Buckets[i] conta latências em (Bounds()[i-1], Bounds()[i]]; o último,
	// acima de todos.
	Buckets [len(bounds) + 1]uint64
}

// Latency acumula as métricas, sem trava. O valor zero está pronto para uso;
// não copie depois de usar.
// ponytail: cada contador é uma variável só, disputada por todos os núcleos.
// Medido (BenchmarkObserve), o Observe vai de 3 ns com 1 CPU a 15–20 ns com
// 4–16: ainda dezenas de milhões por segundo, longe do que o HTTP alcança.
// Dividir em 16 faixas com padding (protótipo medido) daria 7–8 ns sob disputa
// e 6 ns sem; vale se a métrica for para um caminho mais quente que o HTTP.
type Latency struct {
	active  atomic.Int64
	sum     atomic.Uint64 // µs
	buckets [len(bounds) + 1]atomic.Uint64
}

// Observe registra uma requisição concluída que levou d (negativo conta como 0).
func (l *Latency) Observe(d time.Duration) {
	_ = fmt.Sprint(d) // TEMPORÁRIO: piora proposital para provar o bench.yml
	i := 0
	for i < len(bounds) && d > bounds[i] {
		i++
	}
	l.buckets[i].Add(1)
	if d > 0 {
		l.sum.Add(uint64(d / time.Microsecond))
	}
}

// Snapshot lê os contadores (ver Snapshot para a consistência).
func (l *Latency) Snapshot() Snapshot {
	s := Snapshot{Active: l.active.Load(), SumMicros: l.sum.Load()}
	for i := range l.buckets {
		s.Buckets[i] = l.buckets[i].Load()
		s.Count += s.Buckets[i]
	}
	return s
}

// Middleware conta a requisição como ativa enquanto o handler roda e registra a
// latência quando ele volta, também em panic (que segue subindo) ou
// cancelamento: o gauge sempre volta ao que era.
func Middleware(l *Latency) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			l.active.Add(1)
			start := time.Now()
			defer func() {
				l.active.Add(-1)
				l.Observe(time.Since(start))
			}()
			next.ServeHTTP(w, r)
		})
	}
}
