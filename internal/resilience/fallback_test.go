package resilience

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

var chain = map[string][]string{"a": {"b", "c"}}

// fake é um openrouter.Client roteirizado por modelo que anota as chamadas
// (todas na goroutine do teste: a cascata é síncrona).
type fake struct {
	calls  []string
	create func(ctx context.Context, model string) (*domain.ChatCompletionResponse, error)
	stream func(ctx context.Context, model string) (<-chan domain.StreamChunk, <-chan error, error)
}

func (u *fake) CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	u.calls = append(u.calls, req.Model)
	return u.create(ctx, req.Model)
}

func (u *fake) StreamChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	u.calls = append(u.calls, req.Model)
	return u.stream(ctx, req.Model)
}

func newFallback(t *testing.T, up *fake, opts Options) *Fallback {
	t.Helper()
	if opts.AttemptTimeout == 0 {
		opts.AttemptTimeout = time.Minute
	}
	f, err := New(up, opts)
	require.NoError(t, err)
	return f
}

// manual troca o time.AfterFunc de f: nada dispara sozinho; o canal traz, em
// ordem, o disparo da janela de cada tentativa.
func manual(f *Fallback) chan func() {
	fire := make(chan func(), 8)
	f.after = func(_ time.Duration, fn func()) func() bool {
		var state atomic.Int32 // 0 armado, 1 parado, 2 disparado
		fire <- func() {
			if state.CompareAndSwap(0, 2) {
				fn()
			}
		}
		return func() bool { return state.CompareAndSwap(0, 1) }
	}
	return fire
}

func ask(model string) *domain.ChatCompletionRequest {
	return &domain.ChatCompletionRequest{Model: model, Messages: []domain.ChatMessage{{Role: domain.RoleUser, Content: "oi"}}}
}

func recv[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(10 * time.Second): // só evita travar a suíte
		t.Fatal("nada chegou em 10s")
		panic("inalcançável")
	}
}

// hang segura até o ctx da tentativa acabar, como o cliente HTTP.
func hang(ctx context.Context) (*domain.ChatCompletionResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestNewValidates(t *testing.T) {
	for _, opts := range []Options{{}, {AttemptTimeout: time.Second, Budget: -1}, {AttemptTimeout: time.Second, Fallbacks: map[string][]string{"a": {""}}}} {
		_, err := New(&fake{}, opts)
		assert.Error(t, err)
	}
	_, err := New(nil, Options{AttemptTimeout: time.Second})
	assert.Error(t, err)
}

func TestTimeoutFallsBackToNextModel(t *testing.T) {
	var fire chan func()
	var cause error
	up := &fake{create: func(ctx context.Context, model string) (*domain.ChatCompletionResponse, error) {
		if model != "a" {
			return &domain.ChatCompletionResponse{Model: model}, nil
		}
		recv(t, fire)() // a janela de "a" vence com a chamada em curso
		defer func() { cause = context.Cause(ctx) }()
		return hang(ctx)
	}}
	f := newFallback(t, up, Options{Fallbacks: chain})
	fire = manual(f)

	resp, err := f.CreateChatCompletion(context.Background(), ask("a"))
	require.NoError(t, err)
	assert.Equal(t, "b", resp.Model)
	assert.Equal(t, []string{"a", "b"}, up.calls)
	assert.ErrorIs(t, cause, gwerrors.ErrUpstreamTimeout, "a tentativa lenta foi cancelada com a causa de timeout")
}

func TestOnlyRetryableErrorsFallBack(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		fallback bool
	}{
		{"400 do cliente", gwerrors.New("BAD_REQUEST", "x", http.StatusBadRequest, -32602), false},
		{"modelo inexistente", gwerrors.NewModelNotFoundError("a"), false},
		{"erro desconhecido", errors.New("boom"), false},
		{"429 do upstream", gwerrors.NewRateLimitError(nil), true},
		{"502 do upstream", gwerrors.New("DOWN", "x", http.StatusBadGateway, -32603), true},
		{"circuito aberto", gwerrors.ErrCircuitOpen, true},
		{"timeout do transporte", &url.Error{Op: "Post", URL: "https://x", Err: &net.DNSError{IsTimeout: true}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := &fake{create: func(context.Context, string) (*domain.ChatCompletionResponse, error) { return nil, tc.err }}
			_, err := newFallback(t, up, Options{Fallbacks: chain}).CreateChatCompletion(context.Background(), ask("a"))
			assert.ErrorIs(t, err, tc.err, "o erro do upstream chega intacto")
			want := []string{"a"}
			if tc.fallback {
				want = []string{"a", "b", "c"}
			}
			assert.Equal(t, want, up.calls)
		})
	}
}

func TestCallerGivingUpStopsTheCascade(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	var inherited time.Time
	up := &fake{create: func(actx context.Context, _ string) (*domain.ChatCompletionResponse, error) {
		inherited, _ = actx.Deadline()
		cancel() // o chamador desiste com a chamada em curso
		return hang(actx)
	}}
	f := newFallback(t, up, Options{Fallbacks: chain})
	_, err := f.CreateChatCompletion(ctx, ask("a"))
	assert.ErrorIs(t, err, context.Canceled)
	assert.True(t, inherited.Equal(deadline), "a tentativa herda o prazo do chamador")

	past, cancelPast := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelPast()
	_, err = f.CreateChatCompletion(past, ask("a"))
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotErrorIs(t, err, gwerrors.ErrUpstreamTimeout, "o prazo vencido é do chamador, não do upstream")
	assert.Equal(t, []string{"a", "a"}, up.calls, "desistência e prazo do chamador não acionam alternativa")
}

func TestBudgetShrinksTheWindowAndStopsTheCascade(t *testing.T) {
	var clock atomic.Int64
	took := map[string]time.Duration{"a": 4 * time.Second, "b": 2 * time.Second}
	up := &fake{create: func(_ context.Context, model string) (*domain.ChatCompletionResponse, error) {
		clock.Add(int64(took[model]))
		return nil, gwerrors.New("DOWN", "x", http.StatusBadGateway, -32603)
	}}
	f := newFallback(t, up, Options{AttemptTimeout: 4 * time.Second, Budget: 6 * time.Second, Fallbacks: chain})
	f.now = func() time.Time { return time.Unix(0, clock.Load()) }
	var windows []time.Duration
	f.after = func(d time.Duration, _ func()) func() bool {
		windows = append(windows, d)
		return func() bool { return true }
	}

	_, err := f.CreateChatCompletion(context.Background(), ask("a"))
	assert.ErrorIs(t, err, gwerrors.ErrUpstreamTimeout)
	assert.Equal(t, http.StatusGatewayTimeout, gwerrors.ToHTTPStatus(err))
	assert.Contains(t, err.Error(), "orçamento de 6s esgotado após 2 tentativa(s)")
	assert.Equal(t, []string{"a", "b"}, up.calls, "c não começa sem orçamento")
	assert.Equal(t, []time.Duration{4 * time.Second, 2 * time.Second}, windows, "b só ganha o que sobrou")
}

// words abre um stream que entrega ws; com hold, fica aberto até o
// cancelamento, senão fecha. exited avisa o fim da goroutine.
func words(ctx context.Context, model string, hold bool, exited chan<- string, ws ...string) (<-chan domain.StreamChunk, <-chan error, error) {
	chunks, errs := make(chan domain.StreamChunk), make(chan error, 1)
	go func() {
		defer func() { close(errs); close(chunks); exited <- model }()
		for _, w := range ws {
			select {
			case chunks <- domain.StreamChunk{Model: model, DeltaContent: w}:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
		}
		if hold {
			<-ctx.Done()
			errs <- ctx.Err()
		}
	}()
	return chunks, errs, nil
}

func drain(t *testing.T, chunks <-chan domain.StreamChunk) []string {
	t.Helper()
	done := make(chan []string, 1)
	go func() {
		var got []string
		for c := range chunks {
			got = append(got, c.Model+":"+c.DeltaContent)
		}
		done <- got
	}()
	return recv(t, done)
}

func TestStreamFallsBackOnlyBeforeTheFirstChunk(t *testing.T) {
	var fire chan func()
	exited := make(chan string, 2)
	up := &fake{stream: func(ctx context.Context, model string) (<-chan domain.StreamChunk, <-chan error, error) {
		if model == "a" {
			recv(t, fire)() // "a" abre o stream, mas não entrega nada dentro da janela
			return words(ctx, model, true, exited)
		}
		return words(ctx, model, true, exited, "x", "y")
	}}
	f := newFallback(t, up, Options{Fallbacks: chain})
	fire = manual(f)
	ctx, cancel := context.WithCancel(context.Background())
	chunks, errs, err := f.StreamChatCompletion(ctx, ask("a"))
	require.NoError(t, err)
	recv(t, fire)() // a janela de "b" vence com "y" ainda no upstream: não pode cortar nem trocar

	assert.Equal(t, domain.StreamChunk{Model: "b", DeltaContent: "x"}, recv(t, chunks))
	assert.Equal(t, domain.StreamChunk{Model: "b", DeltaContent: "y"}, recv(t, chunks))
	cancel()
	assert.Empty(t, drain(t, chunks))
	assert.ErrorIs(t, recv(t, errs), context.Canceled)
	assert.ElementsMatch(t, []string{"a", "b"}, []string{recv(t, exited), recv(t, exited)}, "nenhuma goroutine de stream ficou para trás")
	assert.Equal(t, []string{"a", "b"}, up.calls)
}

func TestStreamLateFirstChunkOpenErrorAndEmptyStream(t *testing.T) {
	late := false // a janela venceu, mas o AfterFunc ainda não rodou o cancel
	up := &fake{stream: func(_ context.Context, model string) (<-chan domain.StreamChunk, <-chan error, error) {
		chunks, errs := make(chan domain.StreamChunk, 1), make(chan error)
		close(errs)
		late = model == "a" || model == "z"
		switch model {
		case "a", "z": // o primeiro chunk está pronto, mas chegou depois da janela
			chunks <- domain.StreamChunk{Model: model, DeltaContent: "x"}
		case "b":
			return nil, nil, gwerrors.New("DOWN", "x", http.StatusServiceUnavailable, -32603)
		}
		close(chunks) // "c": vazio, sem erro
		return chunks, errs, nil
	}}
	f := newFallback(t, up, Options{Fallbacks: chain})
	f.after = func(time.Duration, func()) func() bool { return func() bool { return !late } }

	chunks, errs, err := f.StreamChatCompletion(context.Background(), ask("a"))
	require.NoError(t, err)
	assert.Empty(t, drain(t, chunks), `nada de "a" chega ao cliente`)
	assert.NoError(t, recv(t, errs))
	assert.Equal(t, []string{"a", "b", "c"}, up.calls)

	_, _, err = f.StreamChatCompletion(context.Background(), ask("z")) // sem alternativa
	assert.ErrorIs(t, err, gwerrors.ErrUpstreamTimeout)
}
