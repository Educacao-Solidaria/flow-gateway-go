package singleflight

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// totalWaiters soma quem espera em todas as chamadas em voo.
func (g *Group[V]) totalWaiters() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, c := range g.calls {
		n += c.waiters
	}
	return n
}

// inFlight conta as chamadas em voo.
func (g *Group[V]) inFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

// waitFor espera cond; o prazo largo só existe para não travar a suíte.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 10*time.Second, time.Millisecond)
}

// recv lê de ch ou falha (em vez de travar a suíte) se nada chegar.
func recv[T any](t *testing.T, ch <-chan T) (v T) {
	t.Helper()
	select {
	case v = <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("nada chegou no canal")
	}
	return v
}

type ctxKey struct{}

func TestGroupDeduplicatesConcurrentCalls(t *testing.T) {
	var g Group[string]
	var calls atomic.Int32
	release := make(chan struct{})
	fn := func(context.Context) (string, error) {
		calls.Add(1)
		<-release
		return "resposta", nil
	}
	const n = 8
	results := make(chan string, n)
	for i := 0; i < n; i++ {
		go func() {
			v, err := g.Do(context.Background(), "k", fn)
			assert.NoError(t, err)
			results <- v
		}()
	}
	waitFor(t, func() bool { return g.totalWaiters() == n })
	close(release)
	for i := 0; i < n; i++ {
		assert.Equal(t, "resposta", recv(t, results))
	}
	assert.EqualValues(t, 1, calls.Load())
	assert.Zero(t, g.inFlight(), "terminada, a chamada sai da tabela")
}

func TestGroupWaiterCancelDoesNotCancelOthers(t *testing.T) {
	var g Group[string]
	release := make(chan struct{})
	callCtx := make(chan context.Context, 1)
	fn := func(ctx context.Context) (string, error) {
		callCtx <- ctx
		select {
		case <-release:
			return "ok", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	quitter, quit := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "trace-1"))
	quitErr := make(chan error, 1)
	go func() {
		_, err := g.Do(quitter, "k", fn)
		quitErr <- err
	}()
	ctx := recv(t, callCtx)
	assert.Equal(t, "trace-1", ctx.Value(ctxKey{}), "a chamada leva os valores do primeiro chamador")
	stayed := make(chan string, 1)
	go func() {
		v, _ := g.Do(context.Background(), "k", fn)
		stayed <- v
	}()
	waitFor(t, func() bool { return g.totalWaiters() == 2 })

	quit()
	assert.ErrorIs(t, recv(t, quitErr), context.Canceled)
	assert.NoError(t, ctx.Err(), "quem desistiu não cancela a chamada de quem ficou")
	close(release)
	assert.Equal(t, "ok", recv(t, stayed))
}

func TestGroupLastWaiterCancelStopsCall(t *testing.T) {
	var g Group[string]
	started, callErr := make(chan struct{}), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := g.Do(ctx, "k", func(ctx context.Context) (string, error) {
			close(started)
			<-ctx.Done()
			callErr <- ctx.Err()
			return "", ctx.Err()
		})
		errc <- err
	}()
	recv(t, started)
	cancel()
	assert.ErrorIs(t, recv(t, errc), context.Canceled)
	assert.ErrorIs(t, recv(t, callErr), context.Canceled, "sem ninguém esperando, a chamada é cancelada")
	assert.Zero(t, g.inFlight(), "e sai da tabela")

	v, err := g.Do(context.Background(), "k", func(context.Context) (string, error) { return "nova", nil })
	require.NoError(t, err)
	assert.Equal(t, "nova", v, "um pedido novo começa do zero, não pega a chamada cancelada")
}

// Três chamadas seguidas na mesma chave: cada uma executa fn de novo (não é cache).
func TestGroupErrorsPanicsAndCanceledCtx(t *testing.T) {
	var g Group[string]
	boom := errors.New("upstream 502")
	_, err := g.Do(context.Background(), "k", func(context.Context) (string, error) { return "", boom })
	assert.ErrorIs(t, err, boom)

	_, err = g.Do(context.Background(), "k", func(context.Context) (string, error) { panic("estado inválido") })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "estado inválido")
	assert.Zero(t, g.inFlight())

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = g.Do(canceled, "k", func(context.Context) (string, error) {
		t.Error("ctx já cancelado não deve chamar fn")
		return "", nil
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, g.inFlight())
}
