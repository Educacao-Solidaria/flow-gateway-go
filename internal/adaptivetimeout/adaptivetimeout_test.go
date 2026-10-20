package adaptivetimeout

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/resilience"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

func base() Options {
	return Options{Default: time.Second, Min: 10 * time.Millisecond, Max: 10 * time.Second, Factor: 2, MinSamples: 10, Window: time.Minute, MaxModels: 2}
}

// clocked cria os Timeouts com um relógio manual.
func clocked(t *testing.T, o Options) (*Timeouts, *time.Time) {
	t.Helper()
	ts, err := New(o)
	require.NoError(t, err)
	now := time.Unix(1_700_000_000, 0)
	ts.now = func() time.Time { return now }
	return ts, &now
}

func observe(ts *Timeouts, model string, n int, took time.Duration) {
	for i := 0; i < n; i++ {
		ts.Observe(model, false, took)
	}
}

// near tolera o arredondamento do float da interpolação (1 µs).
func near(t *testing.T, want, got time.Duration, msg string) {
	t.Helper()
	assert.InDelta(t, float64(want), float64(got), float64(time.Microsecond), msg)
}

func TestNewValidatesAndFillsDefaults(t *testing.T) {
	for name, mut := range map[string]func(*Options){
		"Min zero":      func(o *Options) { o.Min = 0 },
		"Min > Default": func(o *Options) { o.Min = 2 * time.Second },
		"Default > Max": func(o *Options) { o.Max = time.Millisecond * 500 },
		"Factor < 1":    func(o *Options) { o.Factor = 0.5 },
		"Factor NaN":    func(o *Options) { o.Factor = math.NaN() },
		"Factor +Inf":   func(o *Options) { o.Factor = math.Inf(1) },
	} {
		o := base()
		mut(&o)
		_, err := New(o)
		assert.Error(t, err, name)
	}
	ts, err := New(Options{Default: time.Second, Min: time.Second, Max: time.Second, Factor: 1})
	require.NoError(t, err)
	assert.Equal(t, Options{Default: time.Second, Min: time.Second, Max: time.Second, Factor: 1, MinSamples: 20, Window: 5 * time.Minute, MaxModels: 512}, ts.opts)
}

func TestColdStartThenP95TimesFactor(t *testing.T) {
	ts, _ := clocked(t, base())
	observe(ts, "m", 9, 80*time.Millisecond) // bucket (50 ms, 100 ms]
	assert.Equal(t, time.Second, ts.Timeout("m", false), "9 de 10: ainda a frio")
	observe(ts, "m", 1, 2*time.Second) // bucket (1 s, 2,5 s]
	// Posto 9,5 de 10: cai no bucket (1 s, 2,5 s], com 9 antes e 1 nele:
	// 1 s + 1,5 s × (9,5 − 9) / 1 = 1,75 s; × 2 = 3,5 s.
	near(t, 3500*time.Millisecond, ts.Timeout("m", false), "p95 × Factor")
	assert.Equal(t, time.Second, ts.Timeout("m", true), "o stream é outra série")
	assert.Len(t, ts.series, 1, "Timeout não cria série")
}

func TestClampAndOverflow(t *testing.T) {
	o := base()
	o.MaxModels = 8
	ts, _ := clocked(t, o)
	observe(ts, "rápido", 10, 0)
	observe(ts, "lento", 10, time.Hour) // acima do último bucket (5 min)
	assert.Equal(t, 10*time.Millisecond, ts.Timeout("rápido", false), "Min")
	assert.Equal(t, 10*time.Second, ts.Timeout("lento", false), "Max")
	ts.opts.Factor, ts.opts.Max = math.MaxFloat64, math.MaxInt64
	assert.Equal(t, time.Duration(math.MaxInt64), ts.Timeout("lento", false), "Factor enorme não estoura")
}

func TestWindowsRotateAndForget(t *testing.T) {
	ts, now := clocked(t, base())
	observe(ts, "m", 10, 2*time.Second)
	slow := ts.Timeout("m", false)
	near(t, 4850*time.Millisecond, slow, "(1 s + 1,5 s × 0,95) × 2")
	*now = now.Add(time.Minute) // a janela atual vira a anterior e ainda conta
	assert.Equal(t, slow, ts.Timeout("m", false))
	observe(ts, "m", 10, 80*time.Millisecond)
	near(t, 4700*time.Millisecond, ts.Timeout("m", false), "metade lenta ainda puxa o p95: (1 s + 1,5 s × 9/10) × 2")
	*now = now.Add(time.Minute) // a lenta sai; a rápida vira a anterior
	near(t, 195*time.Millisecond, ts.Timeout("m", false), "(50 ms + 50 ms × 0,95) × 2")
	observe(ts, "m", 10, 80*time.Millisecond)
	*now = now.Add(2 * time.Minute) // parada por duas janelas: as duas são velhas, volta a frio
	assert.Equal(t, time.Second, ts.Timeout("m", false))
}

func TestModelLimits(t *testing.T) {
	ts, _ := clocked(t, base()) // MaxModels: 2
	long := strings.Repeat("x", 257)
	for _, m := range []string{"", long, "a", "b", "c"} {
		observe(ts, m, 10, 0)
	}
	assert.Equal(t, 10*time.Millisecond, ts.Timeout("a", false))
	assert.Equal(t, 10*time.Millisecond, ts.Timeout("b", false))
	for _, m := range []string{"", long, "c"} {
		assert.Equal(t, time.Second, ts.Timeout(m, false), "não aprende: %.10q", m)
	}
	assert.Len(t, ts.series, 2)
}

func TestConcurrentObserveAndTimeout(t *testing.T) {
	ts, err := New(base())
	require.NoError(t, err)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				ts.Observe("m", i%2 == 0, time.Duration(i)*time.Millisecond)
				_ = ts.Timeout("m", i%2 == 1)
			}
		}()
	}
	wg.Wait()
	s := ts.series[key{"m", true}]
	assert.Equal(t, uint64(800), s.cur.Snapshot().Count+s.prev.Snapshot().Count, "nenhuma observação perdida")
}

// upstream é um openrouter.Client mínimo: create responde na hora ou trava até o ctx.
type upstream struct{ slow *bool }

func (u upstream) CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	if *u.slow {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &domain.ChatCompletionResponse{Model: req.Model}, nil
}

func (u upstream) StreamChatCompletion(context.Context, *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	panic("não usado")
}

// TestFallbackUsesAndTeachesTheWindow liga os Timeouts no Fallback de verdade:
// três respostas rápidas levam a janela ao Min, e a tentativa que trava é
// cortada nele (e não no Default de 1 min).
func TestFallbackUsesAndTeachesTheWindow(t *testing.T) {
	ts, err := New(Options{Default: time.Minute, Min: 20 * time.Millisecond, Max: time.Minute, Factor: 2, MinSamples: 3})
	require.NoError(t, err)
	slow := false
	f, err := resilience.New(upstream{&slow}, resilience.Options{Timeouts: ts})
	require.NoError(t, err)
	req := &domain.ChatCompletionRequest{Model: "m", Messages: []domain.ChatMessage{{Role: domain.RoleUser, Content: "oi"}}}
	for i := 0; i < 3; i++ {
		_, err := f.CreateChatCompletion(context.Background(), req)
		require.NoError(t, err)
	}
	assert.Equal(t, 20*time.Millisecond, ts.Timeout("m", false))
	slow = true
	_, err = f.CreateChatCompletion(context.Background(), req)
	assert.ErrorIs(t, err, gwerrors.ErrUpstreamTimeout)
	assert.ErrorContains(t, err, "m não respondeu em 20ms")
	assert.Equal(t, uint64(4), ts.series[key{"m", false}].cur.Snapshot().Count, "o corte também ensina")
}
