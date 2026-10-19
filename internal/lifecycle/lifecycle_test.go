package lifecycle

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil"
)

// waitIdle espera todas as goroutines saírem do rastreamento (o log da falha
// é escrito antes da saída). O prazo longo só existe para não travar a suíte.
func waitIdle(t *testing.T, s *Supervisor) {
	t.Helper()
	require.Eventually(t, func() bool { return len(s.Active()) == 0 }, 10*time.Second, time.Millisecond)
}

// untilCanceled é a goroutine bem-comportada: avisa que começou e volta quando
// o supervisor cancela.
func untilCanceled(started chan<- struct{}) func(context.Context) error {
	return func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
}

func TestSupervisorTracksAndShutsDown(t *testing.T) {
	log, buf := testutil.NewLogger(t)
	s := New(log)
	started := make(chan struct{}, 3)
	for _, name := range []string{"ttl-cleaner", "workerpool", "metrics"} {
		require.NoError(t, s.Go(name, untilCanceled(started)))
	}
	for i := 0; i < 3; i++ {
		<-started
	}
	assert.Equal(t, []string{"metrics", "ttl-cleaner", "workerpool"}, s.Active())

	require.NoError(t, s.Shutdown(context.Background()))
	assert.Empty(t, s.Active())
	assert.ErrorIs(t, s.Go("tarde", untilCanceled(started)), ErrClosed)
	assert.NoError(t, s.Shutdown(context.Background()), "Shutdown repetido")
	assert.Empty(t, buf.Entries(t), "context.Canceled no encerramento não é falha")
}

func TestSupervisorShutdownDeadlineReportsStragglers(t *testing.T) {
	s := New(nil)
	started, release := make(chan struct{}, 2), make(chan struct{})
	require.NoError(t, s.Go("teimosa", func(context.Context) error {
		started <- struct{}{}
		<-release // ignora o cancelamento
		return nil
	}))
	require.NoError(t, s.Go("boa", untilCanceled(started)))
	<-started
	<-started

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.Shutdown(expired)
	require.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "teimosa")
	require.Eventually(t, func() bool { return len(s.Active()) == 1 }, 10*time.Second, time.Millisecond,
		"a bem-comportada sai; só a teimosa fica")
	assert.Equal(t, []string{"teimosa"}, s.Active())

	close(release)
	assert.NoError(t, s.Shutdown(context.Background()), "a segunda chamada espera de novo")
	assert.Empty(t, s.Active())
}

func TestSupervisorLogsFailuresAndPanics(t *testing.T) {
	log, buf := testutil.NewLogger(t)
	s := New(log)
	require.NoError(t, s.Go("cache-sync", func(context.Context) error { return errors.New("conexão recusada") }))
	require.NoError(t, s.Go("indexador", func(context.Context) error { panic("estado inválido") }))
	require.NoError(t, s.Go("cancelada-por-fora", func(context.Context) error { return context.Canceled }))
	waitIdle(t, s) // antes do Shutdown: o Canceled acima não vem do encerramento
	require.NoError(t, s.Shutdown(context.Background()))

	byGoroutine := map[string]map[string]any{}
	for _, e := range buf.Entries(t) {
		byGoroutine[e["goroutine"].(string)] = e
	}
	require.Len(t, byGoroutine, 3, "Canceled sem Shutdown em curso também é falha")
	assert.Equal(t, "goroutine supervisionada falhou", byGoroutine["cache-sync"]["msg"])
	assert.Equal(t, "conexão recusada", byGoroutine["cache-sync"]["err"])
	assert.Equal(t, "panic em goroutine supervisionada", byGoroutine["indexador"]["msg"])
	assert.Equal(t, "estado inválido", byGoroutine["indexador"]["panic"])
	assert.Contains(t, byGoroutine["indexador"]["stack"], "TestSupervisorLogsFailuresAndPanics")
	assert.Contains(t, byGoroutine, "cancelada-por-fora")
}

func TestSupervisorEdgeCases(t *testing.T) {
	s := New(nil)
	assert.Error(t, s.Go("sem-funcao", nil))
	assert.Empty(t, s.Active())

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NoError(t, s.Shutdown(expired), "sem goroutines, prazo vencido não é erro")

	// Goroutine que já tinha voltado sozinha antes do Shutdown não conta.
	s = New(nil)
	finished := make(chan struct{})
	require.NoError(t, s.Go("rapida", func(context.Context) error { close(finished); return nil }))
	<-finished
	assert.NoError(t, s.Shutdown(context.Background()))
}

// Go concorrendo com o Shutdown: toda goroutine aceita sai do rastreamento e o
// Shutdown só volta depois da última (rode com -race -count=N).
func TestSupervisorConcurrentGoAndShutdown(t *testing.T) {
	s := New(nil)
	var accepted, exited atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				err := s.Go("carga", func(ctx context.Context) error {
					defer exited.Add(1)
					<-ctx.Done()
					return ctx.Err()
				})
				if err == nil {
					accepted.Add(1)
				} else if !errors.Is(err, ErrClosed) {
					t.Errorf("erro inesperado: %v", err)
				}
			}
		}()
	}
	for accepted.Load() < 50 {
		runtime.Gosched() // encerra com as chamadas a Go em pleno andamento
	}
	require.NoError(t, s.Shutdown(context.Background()))
	wg.Wait()
	assert.Empty(t, s.Active())
	assert.Equal(t, accepted.Load(), exited.Load())
}
