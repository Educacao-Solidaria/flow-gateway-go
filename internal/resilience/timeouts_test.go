package resilience

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

// learner é um Timeouts de janela fixa que anota o que pediram e o que aprendeu.
type learner struct {
	window      time.Duration
	asked, seen []string
}

func (l *learner) Timeout(model string, stream bool) time.Duration {
	l.asked = append(l.asked, fmt.Sprintf("%s/%v", model, stream))
	return l.window
}

func (l *learner) Observe(model string, stream bool, took time.Duration) {
	l.seen = append(l.seen, fmt.Sprintf("%s/%v=%s", model, stream, took))
}

// timed põe em f um relógio manual (clock) e o disparo manual das janelas,
// anotando a janela de cada tentativa.
func timed(f *Fallback, clock *time.Time) (fire chan func(), windows *[]time.Duration) {
	f.now = func() time.Time { return *clock }
	fire = manual(f)
	inner, ws := f.after, &[]time.Duration{}
	f.after = func(d time.Duration, fn func()) func() bool { *ws = append(*ws, d); return inner(d, fn) }
	return fire, ws
}

func TestNewAcceptsTimeoutsInsteadOfAttemptTimeout(t *testing.T) {
	_, err := New(&fake{}, Options{Timeouts: &learner{}})
	assert.NoError(t, err)
	_, err = New(&fake{}, Options{AttemptTimeout: -time.Second, Timeouts: &learner{}})
	assert.Error(t, err)
}

func TestTimeoutsGiveTheWindowAndLearnFromIt(t *testing.T) {
	l, clock := &learner{window: 30 * time.Millisecond}, time.Unix(0, 0)
	var fire chan func()
	up := &fake{create: func(ctx context.Context, model string) (*domain.ChatCompletionResponse, error) {
		switch model {
		case "a": // a janela vence: ensina a janela inteira
			clock = clock.Add(30 * time.Millisecond)
			recv(t, fire)()
			return hang(ctx)
		case "b": // erro rápido não diz nada da latência
			return nil, gwerrors.New("DOWN", "x", http.StatusServiceUnavailable, -32603)
		}
		clock = clock.Add(7 * time.Millisecond)
		return &domain.ChatCompletionResponse{Model: model}, nil
	}}
	f := newFallback(t, up, Options{Fallbacks: chain, Timeouts: l})
	fire, windows := timed(f, &clock)
	resp, err := f.CreateChatCompletion(context.Background(), ask("a"))
	require.NoError(t, err)
	assert.Equal(t, "c", resp.Model)
	assert.Equal(t, []string{"a/false", "b/false", "c/false"}, l.asked)
	assert.Equal(t, []time.Duration{30 * time.Millisecond, 30 * time.Millisecond, 30 * time.Millisecond}, *windows, "a janela é a do Timeouts, não o AttemptTimeout de 1 min")
	assert.Equal(t, []string{"a/false=30ms", "c/false=7ms"}, l.seen)

	l.seen, l.asked = nil, nil
	up.stream = func(ctx context.Context, model string) (<-chan domain.StreamChunk, <-chan error, error) {
		exited := make(chan string, 1)
		if model == "a" { // nenhum pedaço dentro da janela
			recv(t, fire)()
			return words(ctx, model, true, exited)
		}
		clock = clock.Add(5 * time.Millisecond)
		return words(ctx, model, false, exited, "x")
	}
	f = newFallback(t, up, Options{Fallbacks: chain, Timeouts: l}) // sem disparos pendentes da parte de cima
	fire, _ = timed(f, &clock)
	chunks, errs, err := f.StreamChatCompletion(context.Background(), ask("a"))
	require.NoError(t, err)
	assert.Equal(t, []string{"b:x"}, drain(t, chunks))
	assert.NoError(t, recv(t, errs))
	assert.Equal(t, []string{"a/true", "b/true"}, l.asked, "o stream pede a série dele")
	assert.Equal(t, []string{"a/true=30ms", "b/true=5ms"}, l.seen, "no stream, até o 1º pedaço")
}

func TestTimeoutsDoNotLearnFromCutWindowsOrCallerGivingUp(t *testing.T) {
	l, clock := &learner{window: 30 * time.Millisecond}, time.Unix(0, 0)
	var fire chan func()
	up := &fake{create: func(ctx context.Context, model string) (*domain.ChatCompletionResponse, error) {
		clock = clock.Add(30 * time.Millisecond)
		recv(t, fire)()
		return hang(ctx)
	}}
	f := newFallback(t, up, Options{Fallbacks: chain, Budget: 40 * time.Millisecond, Timeouts: l})
	fire, windows := timed(f, &clock)
	_, err := f.CreateChatCompletion(context.Background(), ask("a"))
	assert.ErrorContains(t, err, "orçamento de 40ms esgotado após 2 tentativa(s)")
	assert.Equal(t, []time.Duration{30 * time.Millisecond, 10 * time.Millisecond}, *windows)
	assert.Equal(t, []string{"a/false=30ms"}, l.seen, "a janela de b foi cortada pelo orçamento: não ensina")

	l.seen = nil
	ctx, cancel := context.WithCancel(context.Background())
	up.create = func(context.Context, string) (*domain.ChatCompletionResponse, error) {
		cancel() // o chamador desiste no meio da tentativa
		return nil, context.Canceled
	}
	_, err = newFallback(t, up, Options{Fallbacks: chain, Timeouts: l}).CreateChatCompletion(ctx, ask("a"))
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, l.seen)
}
