package state

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/lifecycle"
)

// newTestStore devolve um Store com relógio manual (ns desde o New).
func newTestStore(t *testing.T, maxItems int) (*Store[string], *atomic.Int64) {
	t.Helper()
	s, err := New[string](Config{MaxItems: maxItems, CleanupEvery: time.Millisecond})
	require.NoError(t, err)
	clock := new(atomic.Int64)
	s.now = clock.Load
	return s, clock
}

func TestNewValidatesConfig(t *testing.T) {
	for _, cfg := range []Config{
		{MaxItems: 0, CleanupEvery: time.Second},
		{MaxItems: -1, CleanupEvery: time.Second},
		{MaxItems: 1},
		{MaxItems: 1, CleanupEvery: time.Millisecond - 1},
		{MaxItems: 1, CleanupEvery: -time.Second},
	} {
		_, err := New[int](cfg)
		assert.Error(t, err, "%+v", cfg)
	}
	_, err := New[int](Config{MaxItems: 1, CleanupEvery: time.Millisecond})
	assert.NoError(t, err)
}

func TestGetNeverReturnsExpiredItem(t *testing.T) {
	s, clock := newTestStore(t, 10)
	require.NoError(t, s.Set("a", "1", time.Second))
	clock.Store(int64(time.Second) - 1)
	v, ok := s.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "1", v)

	clock.Store(int64(time.Second)) // venceu, e nenhuma limpeza rodou
	v, ok = s.Get("a")
	assert.False(t, ok)
	assert.Empty(t, v)
	assert.Equal(t, 1, s.Len(), "a memória só é liberada pela limpeza")
	_, ok = s.Get("ausente")
	assert.False(t, ok)
}

func TestSetOverwritesValueAndDeadline(t *testing.T) {
	s, clock := newTestStore(t, 10)
	require.NoError(t, s.Set("a", "1", time.Second))
	clock.Store(int64(900 * time.Millisecond))
	require.NoError(t, s.Set("a", "2", time.Second)) // renova: vence em 1,9s
	clock.Store(int64(1500 * time.Millisecond))
	v, ok := s.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "2", v)

	s.Delete("a")
	s.Delete("a") // ausente não é erro
	_, ok = s.Get("a")
	assert.False(t, ok)
	assert.Zero(t, s.Len())
}

func TestSetValidatesInput(t *testing.T) {
	s, _ := newTestStore(t, 10)
	assert.ErrorIs(t, s.Set("", "v", time.Second), errKey)
	assert.ErrorIs(t, s.Set(strings.Repeat("k", MaxKeyLen+1), "v", time.Second), errKey)
	assert.NoError(t, s.Set(strings.Repeat("k", MaxKeyLen), "v", time.Second))
	for _, ttl := range []time.Duration{0, -1, math.MinInt64} {
		assert.ErrorIs(t, s.Set("a", "v", ttl), errTTL, "ttl %d", ttl)
	}
	assert.Equal(t, 1, s.Len())
}

func TestHugeTTLSaturates(t *testing.T) {
	s, clock := newTestStore(t, 10)
	clock.Store(int64(time.Hour))
	require.NoError(t, s.Set("a", "v", math.MaxInt64)) // somado ao relógio, estouraria para o passado
	clock.Store(math.MaxInt64 - 1)
	_, ok := s.Get("a")
	assert.True(t, ok)
}

func TestMaxItems(t *testing.T) {
	s, clock := newTestStore(t, 2)
	require.NoError(t, s.Set("a", "1", time.Second))
	require.NoError(t, s.Set("b", "1", 2*time.Second))
	assert.ErrorIs(t, s.Set("c", "1", time.Second), ErrFull)
	assert.NoError(t, s.Set("a", "2", time.Second), "sobrescrever não cresce")

	clock.Store(int64(time.Second)) // a venceu, mas ocupa a vaga até a limpeza
	assert.ErrorIs(t, s.Set("c", "1", time.Second), ErrFull)
	s.sweep()
	assert.Equal(t, 1, s.Len(), "a limpeza tira só o vencido")
	assert.NoError(t, s.Set("c", "1", time.Second))
	_, ok := s.Get("b")
	assert.True(t, ok)

	s.Delete("b") // apagar também libera vaga
	assert.NoError(t, s.Set("d", "1", time.Second))
}

// waitDone espera o laço voltar; o prazo só existe para o teste falhar em vez de travar.
func waitDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("o laço de limpeza não voltou depois do cancelamento")
		return nil
	}
}

func TestRunSweepsOnTickAndStopsOnCancel(t *testing.T) {
	s, clock := newTestStore(t, 10)
	require.NoError(t, s.Set("velho", "v", time.Second))
	require.NoError(t, s.Set("novo", "v", time.Hour))
	clock.Store(int64(time.Second))

	tick := make(chan time.Time) // sem buffer: o 2º envio só entra depois da 1ª limpeza
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.run(ctx, tick) }()
	tick <- time.Time{}
	tick <- time.Time{}
	assert.Equal(t, 1, s.Len())
	_, ok := s.Get("novo")
	assert.True(t, ok)

	cancel()
	assert.ErrorIs(t, waitDone(t, done), context.Canceled)
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, s.Run(ctx), context.Canceled, "contexto já cancelado volta na hora")
}

func TestRunUnderSupervisorCleansAndExits(t *testing.T) {
	s, clock := newTestStore(t, 10)
	require.NoError(t, s.Set("a", "v", time.Second))
	clock.Store(int64(time.Second))

	var logs bytes.Buffer
	sup := lifecycle.New(slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, sup.Go("state.limpeza", s.Run))
	require.Eventually(t, func() bool { return s.Len() == 0 }, 10*time.Second, time.Millisecond, "o ticker real limpa")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, sup.Shutdown(ctx), "a goroutine de limpeza voltou: sem vazamento")
	assert.Empty(t, sup.Active())
	assert.Empty(t, logs.String(), "o cancelamento do Shutdown não é falha")
}

func TestConcurrentUse(t *testing.T) {
	s, err := New[int](Config{MaxItems: 64, CleanupEvery: time.Millisecond})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := strconv.Itoa((g*500 + i) % 100) // 100 chaves para 64 vagas: ErrFull acontece
				if err := s.Set(k, i, time.Duration(1+i%3)*time.Millisecond); err != nil && !errors.Is(err, ErrFull) {
					t.Error(err)
				}
				s.Get(k)
				if i%7 == 0 {
					s.Delete(k)
				}
			}
		}(g)
	}
	wg.Wait()
	cancel()
	assert.ErrorIs(t, waitDone(t, done), context.Canceled)
	assert.LessOrEqual(t, s.Len(), 64)
}
