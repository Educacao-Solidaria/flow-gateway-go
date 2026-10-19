package workerpool

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
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/pool"
)

func noop(context.Context) {}

// blocker devolve uma tarefa que avisa em started quando começa e só termina
// quando release fecha.
func blocker(started chan<- struct{}, release <-chan struct{}) pool.Task {
	return func(context.Context) {
		started <- struct{}{}
		<-release
	}
}

// waitForWaiter espera uma submissão segurar o RLock do pool: ela já passou da
// checagem do ctx e está à espera de vaga (sincroniza sem depender de sleep).
func waitForWaiter(p *Pool) {
	for p.mu.TryLock() {
		p.mu.Unlock()
		runtime.Gosched()
	}
}

func TestPoolRunsEveryAcceptedTask(t *testing.T) {
	p := New(pool.Config{Workers: 4, QueueCapacity: 100}, nil)
	var ran atomic.Int64
	for i := 0; i < 100; i++ {
		require.NoError(t, p.Submit(func(context.Context) { ran.Add(1) }))
	}
	p.Stop() // drena a fila antes de voltar
	assert.EqualValues(t, 100, ran.Load())
	assert.Equal(t, 0, p.RunningWorkers())
	p.Stop() // idempotente
}

func TestPoolBackpressure(t *testing.T) {
	p := New(pool.Config{Workers: 1, QueueCapacity: 1}, nil)
	started, release := make(chan struct{}, 1), make(chan struct{})
	require.NoError(t, p.Submit(blocker(started, release)))
	<-started
	assert.Equal(t, 1, p.RunningWorkers())

	var ranQueued atomic.Bool
	require.NoError(t, p.Submit(func(context.Context) { ranQueued.Store(true) })) // ocupa a única vaga

	assert.ErrorIs(t, p.Submit(noop), pool.ErrQueueFull)
	for _, timeout := range []time.Duration{0, -time.Second, 5 * time.Millisecond} {
		err := p.SubmitWithTimeout(noop, timeout)
		assert.ErrorIs(t, err, pool.ErrTaskTimeout, "timeout %v", timeout)
		assert.ErrorIs(t, err, pool.ErrQueueFull, "timeout %v", timeout)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.SubmitContext(canceled, noop)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, pool.ErrQueueFull, "ctx já encerrado não chega a esperar vaga")

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- p.SubmitContext(ctx, noop) }()
	waitForWaiter(p)
	cancel()
	err = <-errc
	assert.ErrorIs(t, err, pool.ErrQueueFull)
	assert.ErrorIs(t, err, context.Canceled)

	// Quem espera vaga sem prazo é acordado pelo encerramento, não fica preso.
	go func() { errc <- p.SubmitContext(context.Background(), noop) }()
	waitForWaiter(p)
	stopped := make(chan struct{})
	go func() { p.Stop(); close(stopped) }()
	assert.ErrorIs(t, <-errc, pool.ErrPoolClosed)

	close(release)
	<-stopped
	assert.True(t, ranQueued.Load(), "a tarefa aceita na fila roda antes do Stop voltar")
	for _, err := range []error{p.Submit(noop), p.SubmitWithTimeout(noop, time.Second), p.SubmitContext(context.Background(), noop)} {
		assert.ErrorIs(t, err, pool.ErrPoolClosed)
	}
}

func TestPoolRecoversTaskPanic(t *testing.T) {
	log, buf := testutil.NewLogger(t)
	p := New(pool.Config{Workers: 1, QueueCapacity: 2}, log)
	done := make(chan struct{})
	require.NoError(t, p.Submit(func(context.Context) { panic("boom") }))
	require.NoError(t, p.Submit(func(context.Context) { close(done) }))
	<-done // o único worker sobreviveu ao panic
	p.Stop()
	assert.Equal(t, 0, p.RunningWorkers(), "o panic não deixa o contador preso")

	entries := buf.Entries(t)
	require.Len(t, entries, 1)
	assert.Equal(t, "panic em tarefa do worker pool", entries[0]["msg"])
	assert.Equal(t, "boom", entries[0]["panic"])
	assert.Contains(t, entries[0]["stack"], "TestPoolRecoversTaskPanic")
}

func TestPoolShutdownDeadline(t *testing.T) {
	p := New(pool.Config{Workers: 1, QueueCapacity: 1}, nil)
	started := make(chan struct{})
	require.NoError(t, p.Submit(func(ctx context.Context) {
		close(started)
		<-ctx.Done() // só sai quando o prazo do Shutdown estoura
	}))
	<-started
	queuedErr := make(chan error, 1)
	require.NoError(t, p.Submit(func(ctx context.Context) { queuedErr <- ctx.Err() }))

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, p.Shutdown(expired), context.Canceled)
	assert.ErrorIs(t, <-queuedErr, context.Canceled, "a tarefa da fila é chamada, já cancelada")
	assert.NoError(t, p.Shutdown(context.Background()), "depois de drenado, Shutdown devolve nil")
}

func TestPoolDefaultsAndNilTask(t *testing.T) {
	p := New(pool.Config{Workers: 0, QueueCapacity: -1}, nil)
	started, release := make(chan struct{}, 4), make(chan struct{})
	for i := 0; i < 4; i++ {
		require.NoError(t, p.Submit(blocker(started, release)))
	}
	for i := 0; i < 4; i++ {
		<-started // os 4 workers padrão ocupados, fila vazia
	}
	for i := 0; i < 64; i++ {
		require.NoError(t, p.Submit(noop), "vaga %d da fila padrão", i)
	}
	assert.ErrorIs(t, p.Submit(noop), pool.ErrQueueFull)
	close(release)
	p.Stop()

	for _, err := range []error{p.Submit(nil), p.SubmitWithTimeout(nil, time.Second), p.SubmitContext(context.Background(), nil)} {
		assert.ErrorIs(t, err, pool.ErrNilTask)
	}
}

// Submissões das três variantes concorrendo com o Stop: sem "send on closed
// channel" e sem tarefa aceita perdida (rode com -race -count=N).
func TestPoolConcurrentSubmitAndShutdown(t *testing.T) {
	p := New(pool.Config{Workers: 4, QueueCapacity: 8}, nil)
	var accepted, ran atomic.Int64
	task := func(context.Context) { ran.Add(1) }
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				var err error
				switch i % 3 {
				case 0:
					err = p.Submit(task)
				case 1:
					err = p.SubmitWithTimeout(task, time.Millisecond)
				default:
					err = p.SubmitContext(context.Background(), task)
				}
				switch {
				case err == nil:
					accepted.Add(1)
				case !errors.Is(err, pool.ErrQueueFull) && !errors.Is(err, pool.ErrPoolClosed):
					t.Errorf("erro inesperado: %v", err)
				}
			}
		}()
	}
	for accepted.Load() < 100 {
		runtime.Gosched() // encerra com as submissões em pleno andamento
	}
	p.Stop()
	wg.Wait()
	assert.Equal(t, accepted.Load(), ran.Load(), "toda tarefa aceita roda exatamente uma vez")
}
