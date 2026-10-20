package cachewriter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/lifecycle"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
)

type stored struct {
	prompt, response string
	emb              []float32
	ttl              int
}

// fakeCache implementa só o Store do cache.Client (o Writer não usa o resto).
type fakeCache struct {
	cache.Client
	fn func(ctx context.Context, s stored) error
}

func (f fakeCache) Store(ctx context.Context, prompt, response string, emb []float32, ttl int) error {
	return f.fn(ctx, stored{prompt, response, emb, ttl})
}

func recorder() (fakeCache, func() []stored) {
	var mu sync.Mutex
	var got []stored
	return fakeCache{fn: func(_ context.Context, s stored) error { mu.Lock(); got = append(got, s); mu.Unlock(); return nil }},
		func() []stored { mu.Lock(); defer mu.Unlock(); return slices.Clone(got) }
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// drain encerra o Writer como o Shutdown do supervisor faria: depois dele, a fila está vazia.
func drain(t *testing.T, w *Writer) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, w.Run(ctx))
}

func ck(text, finish string) domain.StreamChunk {
	return domain.StreamChunk{DeltaContent: text, FinishReason: finish, IsLast: finish != ""}
}

func TestTeeStoresOnlyResponsesThatEndedWell(t *testing.T) {
	boom := errors.New("upstream caiu")
	cases := []struct {
		name   string
		err    error
		chunks []domain.StreamChunk
		want   string // "" = não grava
	}{
		{"completa", nil, []domain.StreamChunk{ck("Olá ", ""), ck("mundo", "stop")}, "Olá mundo"},
		{"no teto exato", nil, []domain.StreamChunk{ck("123456", ""), ck("789012", "stop")}, "123456789012"},
		{"acima do teto", nil, []domain.StreamChunk{ck("123456", ""), ck("7890123", "stop")}, ""},
		{"erro no fim", boom, []domain.StreamChunk{ck("Olá", "stop")}, ""},
		{"cortada por length", nil, []domain.StreamChunk{ck("Olá", "length")}, ""},
		{"sem finish_reason", nil, []domain.StreamChunk{ck("Olá", "")}, ""},
		{"duas escolhas", nil, []domain.StreamChunk{ck("a", "stop"), ck("b", "stop")}, ""},
		{"pedaço depois do fim", nil, []domain.StreamChunk{ck("a", "stop"), ck("b", "")}, ""},
		{"vazia", nil, []domain.StreamChunk{ck("", "stop")}, ""},
		{"sem pedaços", nil, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc, got := recorder()
			w := New(fc, Options{MaxResponseBytes: 12, TTL: 1500 * time.Millisecond}, quiet)
			in, inErr := make(chan domain.StreamChunk, len(tc.chunks)), make(chan error, 1)
			for _, c := range tc.chunks {
				in <- c
			}
			if tc.err != nil {
				inErr <- tc.err
			}
			close(in)
			close(inErr)
			emb := []float32{0.5, -0.25}
			out, outErr := w.Tee(context.Background(), "p", emb, in, inErr)
			emb[0] = 9 // o Writer guardou a própria cópia
			var seen []domain.StreamChunk
			for c := range out {
				seen = append(seen, c)
			}
			assert.Equal(t, tc.chunks, seen, "o stream passa sem mudança")
			assert.Equal(t, tc.err, <-outErr)
			drain(t, w)
			if tc.want == "" {
				assert.Empty(t, got())
				return
			}
			assert.Equal(t, []stored{{"p", tc.want, []float32{0.5, -0.25}, 2}}, got())
			assert.Equal(t, Stats{Stored: 1}, w.Stats())
		})
	}
}

func TestTeeCanceledMidStreamIsNotStored(t *testing.T) {
	fc, got := recorder()
	w := New(fc, Options{}, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan domain.StreamChunk, 3) // o upstream ainda não fechou: o Tee tem de sair pelo ctx
	in <- ck("a", "")
	in <- ck("b", "")
	in <- ck("c", "stop")
	out, outErr := w.Tee(ctx, "p", nil, in, make(chan error))
	assert.Equal(t, "a", (<-out).DeltaContent)
	cancel() // o cliente desistiu: ninguém mais lê out
	select {
	case err := <-outErr:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("o Tee não saiu pelo ctx")
	}
	for range out {
	}
	drain(t, w)
	assert.Empty(t, got())
}

func TestStoreDropsWhenQueueIsFullAndAfterRun(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	fc := fakeCache{fn: func(context.Context, stored) error { started <- struct{}{}; <-release; return nil }}
	w := New(fc, Options{Workers: 1, QueueCapacity: 1}, quiet)
	require.True(t, w.Store("p", "a", nil))
	<-started // o único worker está ocupado com "a"
	assert.True(t, w.Store("p", "b", nil), "b ocupa a única vaga da fila")
	assert.False(t, w.Store("p", "c", nil), "fila cheia: descarta sem esperar")
	assert.False(t, w.Store("p", "", nil), "resposta vazia não vale gravar nem conta")
	close(release)
	drain(t, w)
	assert.False(t, w.Store("p", "d", nil), "depois do Run nada entra")
	assert.Equal(t, Stats{Stored: 2, Dropped: 2}, w.Stats())
}

func TestStoreTimeoutBoundsEachWrite(t *testing.T) {
	fc := fakeCache{fn: func(ctx context.Context, _ stored) error { <-ctx.Done(); return ctx.Err() }}
	w := New(fc, Options{StoreTimeout: 10 * time.Millisecond}, quiet)
	require.True(t, w.Store("p", "r", nil))
	drain(t, w) // volta porque o prazo da gravação solta o worker, não o do dreno (5 s)
	assert.Equal(t, Stats{Failed: 1}, w.Stats())
}

func TestFailuresAndDrainDeadlineUnderSupervisor(t *testing.T) {
	var buf bytes.Buffer // um handler só: o slog serializa as escritas
	log := slog.New(slog.NewTextHandler(&buf, nil))
	calls := make(chan string, 5)
	fc := fakeCache{fn: func(ctx context.Context, s stored) error {
		calls <- s.response
		switch s.response {
		case "falha":
			return errors.New("bad_request: dimensão errada")
		case "ok":
			return nil
		}
		<-ctx.Done() // "trava": só o prazo do dreno solta
		return ctx.Err()
	}}
	w := New(fc, Options{Workers: 1, StoreTimeout: time.Hour, DrainTimeout: 20 * time.Millisecond}, log)
	sup := lifecycle.New(log)
	require.NoError(t, sup.Go("cachewriter", w.Run))
	for _, r := range []string{"falha", "falha", "ok", "trava"} {
		require.True(t, w.Store("p", r, nil))
	}
	for range 4 {
		<-calls // a quarta é a "trava", já no worker
	}
	// Um Run que não esperasse o supervisor já teria drenado (20 ms); o certo só
	// sai pelo Shutdown, então esta espera não tem como fazer o teste falhar.
	time.Sleep(50 * time.Millisecond)
	require.True(t, w.Store("p", "falha", nil), "a fila só fecha no Shutdown do supervisor")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, sup.Shutdown(ctx), "o Run voltou no prazo do dreno")
	assert.Empty(t, sup.Active())
	require.NoError(t, w.pool.Shutdown(context.Background())) // espera a "trava" contar
	assert.Equal(t, Stats{Stored: 1, Failed: 4}, w.Stats())
	logs := buf.String()
	assert.Equal(t, 2, strings.Count(logs, "gravação no semantic-cache falhou"), "uma por sequência de falhas:\n%s", logs)
	assert.Equal(t, 1, strings.Count(logs, "gravação no semantic-cache voltou"))
	assert.Contains(t, logs, "bad_request: dimensão errada")
	assert.Contains(t, logs, "a fila não drenou em 20ms")
}

func TestTTLRoundsUpAndNonPositiveMeansNone(t *testing.T) {
	for ttl, want := range map[time.Duration]int{0: 0, -time.Second: 0, time.Millisecond: 1, 1500 * time.Millisecond: 2, time.Hour: 3600} {
		w := New(fakeCache{}, Options{TTL: ttl}, quiet)
		assert.Equal(t, want, w.ttl, ttl)
		drain(t, w)
	}
}
