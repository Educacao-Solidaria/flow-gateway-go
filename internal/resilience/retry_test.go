package resilience

import (
	"context"
	"errors"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/quota"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

var unavailable = gwerrors.New("UPSTREAM", "indisponível", http.StatusServiceUnavailable, 0)

// newRetry monta o Retry com o sorteio no teto (a espera é a maior possível) e
// anota, por pausa, o teto sorteado.
func newRetry(t *testing.T, up *fake, p RetryPolicy) (*Retry, *[]time.Duration) {
	t.Helper()
	r, err := NewRetry(up, p)
	require.NoError(t, err)
	var ceilings []time.Duration
	r.jitter = func(n int64) int64 { ceilings = append(ceilings, time.Duration(n-1)); return n - 1 }
	return r, &ceilings
}

// failing falha as n primeiras chamadas com err e depois responde.
func failing(n int, err error) func(context.Context, string) (*domain.ChatCompletionResponse, error) {
	return func(context.Context, string) (*domain.ChatCompletionResponse, error) {
		if n--; n >= 0 {
			return nil, err
		}
		return &domain.ChatCompletionResponse{ID: "ok"}, nil
	}
}

// quotaErr é um 429 com a cota lida da resposta, como o cliente HTTP devolverá.
type quotaErr struct{ wait time.Duration }

func (e quotaErr) Error() string      { return "429" }
func (e quotaErr) Unwrap() error      { return gwerrors.ErrRateLimited }
func (e quotaErr) Quota() quota.Quota { return quota.Quota{Remaining: -1, RetryAfter: e.wait} }

func TestNewRetryValidates(t *testing.T) {
	ms := time.Millisecond
	for _, p := range []RetryPolicy{{}, {MaxAttempts: 1}, {MaxAttempts: 1, BaseDelay: 2 * ms, MaxDelay: ms}, {MaxAttempts: 1, BaseDelay: ms, MaxDelay: ms, MaxElapsed: -1}, {MaxAttempts: 1, BaseDelay: ms, MaxDelay: math.MaxInt64}} {
		_, err := NewRetry(&fake{}, p)
		assert.Error(t, err, "%+v", p)
	}
	_, err := NewRetry(nil, RetryPolicy{MaxAttempts: 1, BaseDelay: ms, MaxDelay: ms})
	assert.Error(t, err)
}

func TestRetryBacksOffWithTruncatedFullJitter(t *testing.T) {
	up := &fake{create: failing(4, unavailable)}
	r, ceilings := newRetry(t, up, RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 3 * time.Millisecond})
	resp, err := r.CreateChatCompletion(context.Background(), ask("a"))
	require.NoError(t, err)
	assert.Equal(t, "ok", resp.ID)
	assert.Equal(t, []string{"a", "a", "a", "a", "a"}, up.calls, "o mesmo modelo")
	// O sorteio é em [0, teto]; o teto dobra a cada tentativa e trunca no MaxDelay.
	assert.Equal(t, []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 3 * time.Millisecond}, *ceilings)
}

func TestRetryOnlyEphemeralErrorsUpToTheLimit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		calls int
	}{
		{"4xx não repete", gwerrors.New("BAD", "x", http.StatusBadRequest, 0), 1},
		{"circuito aberto não repete", gwerrors.ErrCircuitOpen, 1},
		{"erro desconhecido não repete", errors.New("?"), 1},
		{"5xx até MaxAttempts", unavailable, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := &fake{create: failing(10, tc.err)}
			r, _ := newRetry(t, up, RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
			_, err := r.CreateChatCompletion(context.Background(), ask("a"))
			require.ErrorIs(t, err, tc.err)
			assert.Len(t, up.calls, tc.calls)
		})
	}
}

func TestRetryRespectsQuotaAndDeadlines(t *testing.T) {
	ms := time.Millisecond
	p := RetryPolicy{MaxAttempts: 3, BaseDelay: ms, MaxDelay: ms}

	// A cota pede 20 ms: a espera nunca é menor que isso.
	up := &fake{create: failing(1, quotaErr{20 * ms})}
	r, _ := newRetry(t, up, p)
	start := time.Now()
	_, err := r.CreateChatCompletion(context.Background(), ask("a"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 20*ms)

	// A espera não cabe no MaxElapsed nem no prazo do chamador: o 429 volta na hora.
	long := quotaErr{time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, tc := range []struct {
		ctx context.Context
		p   RetryPolicy
	}{{context.Background(), RetryPolicy{MaxAttempts: 3, BaseDelay: ms, MaxDelay: ms, MaxElapsed: time.Second}}, {ctx, p}} {
		up = &fake{create: failing(1, long)}
		r, _ = newRetry(t, up, tc.p)
		start = time.Now()
		_, err = r.CreateChatCompletion(tc.ctx, ask("a"))
		require.ErrorIs(t, err, gwerrors.ErrRateLimited)
		assert.Len(t, up.calls, 1)
		assert.Less(t, time.Since(start), 10*time.Second, "não esperou a cota")
	}

	// O chamador que desiste no meio da espera para tudo.
	ctx, cancel = context.WithCancel(context.Background())
	up = &fake{create: failing(10, unavailable)}
	r, _ = newRetry(t, up, RetryPolicy{MaxAttempts: 3, BaseDelay: time.Hour, MaxDelay: time.Hour})
	time.AfterFunc(20*ms, cancel)
	_, err = r.CreateChatCompletion(ctx, ask("a"))
	require.ErrorIs(t, err, context.Canceled)
	assert.Len(t, up.calls, 1)
}

func TestRetryStreamOnlyBeforeTheFirstChunk(t *testing.T) {
	exited := make(chan string, 8)
	failed := func(err error) (<-chan domain.StreamChunk, <-chan error, error) { // erro antes do 1º pedaço
		chunks, errs := make(chan domain.StreamChunk), make(chan error, 1)
		errs <- err
		close(errs)
		close(chunks)
		return chunks, errs, nil
	}
	n := 0
	up := &fake{stream: func(ctx context.Context, model string) (<-chan domain.StreamChunk, <-chan error, error) {
		switch n++; n {
		case 1:
			return nil, nil, unavailable // falha na abertura
		case 2:
			return failed(unavailable)
		}
		return words(ctx, model, false, exited, "x", "y")
	}}
	r, _ := newRetry(t, up, RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	chunks, errs, err := r.StreamChatCompletion(context.Background(), ask("a"))
	require.NoError(t, err)
	assert.Equal(t, []string{"a:x", "a:y"}, drain(t, chunks))
	require.NoError(t, recv(t, errs))
	assert.Len(t, up.calls, 3)

	// Depois do 1º pedaço, o erro vai para o chamador e não há outra tentativa.
	up = &fake{stream: func(ctx context.Context, model string) (<-chan domain.StreamChunk, <-chan error, error) {
		chunks, errs := make(chan domain.StreamChunk), make(chan error, 1)
		go func() {
			defer close(chunks)
			defer close(errs)
			chunks <- domain.StreamChunk{Model: model, DeltaContent: "x"}
			errs <- unavailable
		}()
		return chunks, errs, nil
	}}
	r, _ = newRetry(t, up, RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	chunks, errs, err = r.StreamChatCompletion(context.Background(), ask("a"))
	require.NoError(t, err)
	assert.Equal(t, []string{"a:x"}, drain(t, chunks))
	require.ErrorIs(t, recv(t, errs), unavailable)
	assert.Len(t, up.calls, 1)

	// Stream vazio que termina sem erro é sucesso; erro não retentável antes do 1º pedaço volta na abertura.
	for _, bad := range []error{nil, gwerrors.New("BAD", "x", http.StatusBadRequest, 0)} {
		up = &fake{stream: func(ctx context.Context, model string) (<-chan domain.StreamChunk, <-chan error, error) {
			if bad != nil {
				return failed(bad)
			}
			return words(ctx, model, false, exited)
		}}
		r, _ = newRetry(t, up, RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
		chunks, errs, err = r.StreamChatCompletion(context.Background(), ask("a"))
		assert.Len(t, up.calls, 1)
		if bad != nil {
			require.ErrorIs(t, err, bad)
			continue
		}
		require.NoError(t, err)
		assert.Empty(t, drain(t, chunks))
		require.NoError(t, recv(t, errs))
	}
}

func TestRetryComposesUnderFallback(t *testing.T) {
	up := &fake{create: func(_ context.Context, model string) (*domain.ChatCompletionResponse, error) {
		if model == "a" { // "a" fora: o Retry esgota dentro da janela e a cascata segue
			return nil, unavailable
		}
		return &domain.ChatCompletionResponse{Model: model}, nil
	}}
	r, _ := newRetry(t, up, RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	f, err := New(r, Options{AttemptTimeout: time.Minute, Fallbacks: chain})
	require.NoError(t, err)
	resp, err := f.CreateChatCompletion(context.Background(), ask("a"))
	require.NoError(t, err)
	assert.Equal(t, "b", resp.Model)
	assert.Equal(t, []string{"a", "a", "b"}, up.calls)
}
