package circuitbreaker

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

var (
	settings = Settings{FailureThreshold: 2, OpenTimeout: time.Minute, HalfOpenProbes: 2}
	errDown  = gwerrors.New("DOWN", "fora do ar", http.StatusBadGateway, -32603)
)

// newBreaker devolve o disjuntor com relógio manual (avance com clock.Add) e
// as transições anotadas.
func newBreaker(t *testing.T) (b *Breaker, clock *atomic.Int64, changes *[]string) {
	t.Helper()
	clock, changes = new(atomic.Int64), new([]string)
	s := settings
	s.OnStateChange = func(name string, from, to State) { *changes = append(*changes, name+":"+from.String()+">"+to.String()) }
	b, err := New("m", s)
	require.NoError(t, err)
	b.now = func() time.Time { return time.Unix(0, clock.Load()) }
	return b, clock, changes
}

// call faz uma chamada pelo disjuntor que termina com err.
func call(b *Breaker, err error) error {
	done, aerr := b.Allow()
	if aerr == nil {
		done(err)
	}
	return aerr
}

func TestNewValidates(t *testing.T) {
	for _, s := range []Settings{{OpenTimeout: 1, HalfOpenProbes: 1}, {FailureThreshold: 1, HalfOpenProbes: 1}, {FailureThreshold: 1, OpenTimeout: 1}} {
		_, err := New("m", s)
		assert.Error(t, err)
		_, err = NewModels(s)
		assert.Error(t, err)
	}
	assert.Equal(t, "unknown", State(7).String())
}

func TestLifecycle(t *testing.T) {
	b, clock, changes := newBreaker(t)
	for _, err := range []error{errDown, nil, errDown} { // o sucesso zera a contagem
		require.NoError(t, call(b, err))
	}
	assert.Equal(t, Closed, b.State())
	require.NoError(t, call(b, errDown))
	assert.ErrorIs(t, call(b, nil), gwerrors.ErrCircuitOpen)
	assert.Equal(t, http.StatusServiceUnavailable, gwerrors.ToHTTPStatus(call(b, nil)))

	clock.Add(int64(settings.OpenTimeout - 1))
	assert.Equal(t, Open, b.State())
	clock.Add(1) // recupera sozinho: o próximo pedido vira sonda
	d1, err1 := b.Allow()
	d2, err2 := b.Allow()
	_, err3 := b.Allow()
	require.NoError(t, errors.Join(err1, err2))
	assert.ErrorIs(t, err3, gwerrors.ErrCircuitOpen, "só HalfOpenProbes sondas por vez")
	d1(nil)
	d1(errDown) // a segunda chamada de done é ignorada
	d3, err := b.Allow()
	require.NoError(t, err, "a sonda que terminou libera a vaga")
	d2(nil)
	assert.Equal(t, Closed, b.State(), "HalfOpenProbes sucessos fecham")
	d3(errDown) // resultado de outra época (o meio-aberto acabou): não conta

	require.NoError(t, call(b, errDown))
	require.NoError(t, call(b, errDown))
	clock.Add(int64(settings.OpenTimeout))
	require.NoError(t, call(b, errDown), "a sonda falha")
	clock.Add(int64(settings.OpenTimeout - 1))
	assert.Equal(t, Open, b.State(), "reabre com prazo novo")
	assert.Equal(t, []string{"m:closed>open", "m:open>half-open", "m:half-open>closed", "m:closed>open", "m:open>half-open", "m:half-open>open"}, *changes)
}

func TestCallerCancelAndClientErrorsDoNotCount(t *testing.T) {
	b, clock, _ := newBreaker(t)
	badRequest := gwerrors.New("BAD_REQUEST", "x", http.StatusBadRequest, -32602)
	for _, err := range []error{context.Canceled, context.Canceled, errDown, badRequest, errDown} {
		require.NoError(t, call(b, err)) // o 400 zera a contagem: o upstream respondeu
	}
	assert.Equal(t, Closed, b.State())
	require.NoError(t, call(b, gwerrors.New("X", "x", http.StatusTooManyRequests, -32603)))
	assert.Equal(t, Open, b.State(), "429 conta como falha")

	clock.Add(int64(settings.OpenTimeout))
	for i := 0; i < 3; i++ {
		require.NoError(t, call(b, context.Canceled), "sonda cancelada libera a vaga")
	}
	assert.Equal(t, HalfOpen, b.State(), "e não fecha nem reabre")
	require.NoError(t, call(b, context.DeadlineExceeded))
	assert.Equal(t, Open, b.State(), "DeadlineExceeded conta: timeout de dial também casa com ele")
}

func TestConcurrentProbesRespectTheLimit(t *testing.T) {
	b, clock, _ := newBreaker(t)
	require.NoError(t, call(b, errDown))
	require.NoError(t, call(b, errDown))
	clock.Add(int64(settings.OpenTimeout))
	var passed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := b.Allow(); err == nil {
				passed.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, settings.HalfOpenProbes, passed.Load())
}
