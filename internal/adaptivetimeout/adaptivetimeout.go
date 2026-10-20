// Package adaptivetimeout dá a janela de cada tentativa pela latência recente
// do modelo: clamp(p95 × Factor, Min, Max). O p95 sai do histograma do
// internal/metrics (PR 49), interpolado dentro do bucket, sobre duas janelas de
// tempo que se alternam (a atual e a anterior): a latência de agora pesa e a de
// horas atrás some. Com menos de MinSamples observações nas duas, vale Default
// (partida a frio). Stream e resposta inteira têm séries separadas, porque a
// janela do stream vai só até o primeiro pedaço.
//
// Uso: resilience.Options{Timeouts: t}. O Fallback pede a janela antes de cada
// tentativa e ensina com o resultado (ver resilience.Timeouts); a janela nunca
// passa do prazo do chamador, porque o ctx da tentativa deriva do dele.
package adaptivetimeout

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/metrics"
)

// Options configura os Timeouts.
type Options struct {
	Default    time.Duration // janela na partida a frio
	Min, Max   time.Duration // a janela fica em [Min, Max]; 0 < Min <= Default <= Max
	Factor     float64       // folga sobre o p95, finita e >= 1 (ex.: 1.5)
	MinSamples uint64        // observações para confiar no p95; padrão 20
	Window     time.Duration // duração de cada janela de observação; padrão 5 min
	MaxModels  int           // teto de séries (modelo × stream); padrão 512
}

// Timeouts implementa resilience.Timeouts; seguro para uso concorrente.
type Timeouts struct {
	opts   Options
	now    func() time.Time // o teste troca
	mu     sync.Mutex
	series map[key]*series
}

type key struct {
	model  string
	stream bool
}

// series são as duas janelas de um modelo: cur recebe, prev só é lida.
type series struct {
	cur, prev *metrics.Latency
	since     time.Time // início de cur
}

const maxModelLen = 256 // como no circuitbreaker.Models

// New valida o.
func New(o Options) (*Timeouts, error) {
	if o.Min <= 0 || o.Min > o.Default || o.Default > o.Max || math.IsNaN(o.Factor) || o.Factor < 1 || math.IsInf(o.Factor, 1) {
		return nil, errors.New("adaptivetimeout: é preciso 0 < Min <= Default <= Max e Factor finito >= 1")
	}
	if o.MinSamples == 0 {
		o.MinSamples = 20
	}
	if o.Window <= 0 {
		o.Window = 5 * time.Minute
	}
	if o.MaxModels <= 0 {
		o.MaxModels = 512
	}
	return &Timeouts{opts: o, now: time.Now, series: map[key]*series{}}, nil
}

// Timeout devolve a janela da próxima tentativa no modelo.
func (t *Timeouts) Timeout(model string, stream bool) time.Duration {
	t.mu.Lock()
	var snap metrics.Snapshot
	if s := t.get(key{model, stream}, false); s != nil {
		snap = s.cur.Snapshot()
		prev := s.prev.Snapshot()
		for i := range snap.Buckets {
			snap.Buckets[i] += prev.Buckets[i]
		}
		snap.Count += prev.Count
	}
	t.mu.Unlock()
	if snap.Count < t.opts.MinSamples {
		return t.opts.Default
	}
	// Em float e comparando antes de converter: Factor grande (ou Max perto do
	// teto do int64) não estoura a conversão.
	switch w := float64(p95(snap)) * t.opts.Factor; {
	case w >= float64(t.opts.Max):
		return t.opts.Max
	case w <= float64(t.opts.Min):
		return t.opts.Min
	default:
		return time.Duration(w)
	}
}

// Observe registra quanto uma tentativa no modelo levou. Modelo vazio, acima
// de 256 bytes ou além de MaxModels séries não aprende: fica no Default.
func (t *Timeouts) Observe(model string, stream bool, took time.Duration) {
	if model == "" || len(model) > maxModelLen {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if s := t.get(key{model, stream}, true); s != nil {
		s.cur.Observe(took)
	}
}

// get acha a série (ou a cria, com create) e a gira quando a janela atual
// venceu. Chamada com t.mu.
// ponytail: sem despejo, a série de um modelo que saiu de uso ocupa vaga do
// MaxModels até o restart; se o teto encher, despejar a parada há 2 janelas.
func (t *Timeouts) get(k key, create bool) *series {
	now := t.now()
	s := t.series[k]
	switch {
	case s == nil && (!create || len(t.series) >= t.opts.MaxModels):
		return nil
	case s == nil:
		s = &series{cur: new(metrics.Latency), prev: new(metrics.Latency), since: now}
		t.series[k] = s
	case now.Sub(s.since) >= 2*t.opts.Window: // parada: as duas janelas são velhas
		s.cur, s.prev, s.since = new(metrics.Latency), new(metrics.Latency), now
	case now.Sub(s.since) >= t.opts.Window:
		s.cur, s.prev, s.since = new(metrics.Latency), s.cur, now
	}
	return s
}

// p95 interpola dentro do bucket em que cai o 95º percentil, como o
// histogram_quantile do Prometheus; acima do último limite (5 min), vale ele.
func p95(s metrics.Snapshot) time.Duration {
	bounds := metrics.Bounds()
	rank := 0.95 * float64(s.Count)
	var cum, lo float64
	for i, b := range bounds {
		n, hi := float64(s.Buckets[i]), float64(b)
		if n > 0 && cum+n >= rank {
			return time.Duration(lo + (hi-lo)*(rank-cum)/n)
		}
		cum, lo = cum+n, hi
	}
	return bounds[len(bounds)-1]
}
