package stress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/circuitbreaker"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/httpclient"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/lifecycle"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ratelimit"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/resilience"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ringbuffer"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/singleflight"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/state"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil/openroutertest"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/tracing"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/workerpool"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/pool"
)

// load é o número de goroutines simultâneas (ver o doc do pacote).
func load(t *testing.T) int {
	t.Helper()
	if v := os.Getenv("STRESS_GOROUTINES"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err, "STRESS_GOROUTINES")
		require.GreaterOrEqual(t, n, 100, "STRESS_GOROUTINES")
		return n
	}
	if testing.Short() {
		return 100
	}
	return 1000
}

// noLeak espera as goroutines voltarem ao número de antes do teste. Não há
// canal para isso (as do transporte HTTP saem sozinhas depois do Close); o
// prazo só existe para o teste falhar em vez de travar.
func noLeak(t *testing.T, before int) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); runtime.NumGoroutine() > before && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	assert.LessOrEqual(t, runtime.NumGoroutine(), before, "goroutine sobrando depois do encerramento")
}

// upstream é o openrouter.Client mínimo que fala HTTP com o mock (o cliente
// real da OpenRouter ainda não existe). A URL sai do modelo, e status de erro
// vira GatewayError com ele, que é o que o Fallback classifica.
type upstream struct {
	hc   *http.Client
	base func(model string) string
}

func (u upstream) CreateChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.base(req.Model)+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	tracing.Inject(ctx, hreq.Header)
	res, err := u.hc.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, res.Body)
		return nil, gwerrors.New("upstream_error", res.Status, res.StatusCode, -32603)
	}
	var out domain.ChatCompletionResponse
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (upstream) StreamChatCompletion(context.Context, *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	return nil, nil, errors.New("stress: stream fora do roteiro")
}

// TestPipelineUnderLoad solta as goroutines de uma vez (barreira) no caminho
// completo de uma requisição: rate limit por tenant → worker pool →
// singleflight → fallback com disjuntor por modelo → HTTP com trace → mock. O
// sucesso vai para o ring buffer e para o state, limpo pelo supervisor. Um
// terço pede o modelo "ruim", cujo mock só devolve 503: o disjuntor dele abre e
// todas acabam servidas pelo "bom". Uma em dez desiste antes de enfileirar e
// uma em dez logo depois.
func TestPipelineUnderLoad(t *testing.T) {
	n := load(t)
	const tenants = 10
	burst := n / tenants * 3 / 5 // a recarga é desprezível: passam exatamente burst por tenant
	before := runtime.NumGoroutine()
	log, logs := testutil.NewLogger(t)

	good, bad := openroutertest.New(t), openroutertest.New(t)
	for i := 0; i < n; i++ {
		bad.Enqueue(openroutertest.ServerError(http.StatusServiceUnavailable))
	}
	hc := httpclient.New(config.OpenRouterConfig{MaxIdleConnsPerHost: 16, MaxConnsPerHost: 16})
	gate, err := circuitbreaker.NewModels(circuitbreaker.Settings{FailureThreshold: 3, OpenTimeout: time.Hour, HalfOpenProbes: 1})
	require.NoError(t, err)
	fb, err := resilience.New(upstream{hc: hc, base: func(model string) string {
		if model == "ruim" {
			return bad.URL
		}
		return good.URL
	}}, resilience.Options{AttemptTimeout: 30 * time.Second, Fallbacks: map[string][]string{"ruim": {"bom"}}, Gate: gate})
	require.NoError(t, err)
	client := singleflight.NewClient(fb)
	limiter, err := ratelimit.New(ratelimit.Config{Rate: 1e-3, Burst: burst, MaxKeys: tenants})
	require.NoError(t, err)
	workers := workerpool.New(pool.Config{Workers: 8, QueueCapacity: 16}, log)
	ring, err := ringbuffer.New[int](n)
	require.NoError(t, err)
	store, err := state.New[string](state.Config{MaxItems: n, CleanupEvery: time.Millisecond})
	require.NoError(t, err)
	sup := lifecycle.New(log)
	require.NoError(t, sup.Go("state.limpeza", store.Run))

	var rejected, served, gaveUp atomic.Int64
	executed := make([]atomic.Int32, n)
	submitted := make([]bool, n) // cada índice só é escrito pela própria goroutine
	traces := map[string]bool{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		sc := tracing.New()
		traces[sc.TraceID.String()] = true
		wg.Add(1)
		go func(i int, ctx context.Context) {
			defer wg.Done()
			<-start
			tenant := "t" + strconv.Itoa(i%tenants)
			if ok, _ := limiter.Allow(tenant); !ok {
				rejected.Add(1)
				return
			}
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			if i%10 == 0 {
				cancel() // desiste antes de enfileirar
			}
			model := "bom"
			if i%3 == 0 {
				model = "ruim"
			}
			req := &domain.ChatCompletionRequest{Model: model, TenantID: tenant,
				Messages: []domain.ChatMessage{{Role: domain.RoleUser, Content: "pergunta " + strconv.Itoa(i%7)}}}
			type result struct {
				resp *domain.ChatCompletionResponse
				err  error
			}
			done := make(chan result, 1)
			if err := workers.SubmitContext(ctx, func(context.Context) {
				executed[i].Add(1)
				resp, err := client.CreateChatCompletion(ctx, req)
				done <- result{resp, err}
			}); err != nil {
				assert.ErrorIs(t, err, context.Canceled, "goroutine %d", i)
				assert.Zero(t, i%10, "goroutine %d recusada sem desistir", i)
				gaveUp.Add(1)
				return
			}
			submitted[i] = true
			if i%10 == 1 {
				cancel() // desiste com a tarefa na fila ou em voo
			}
			r := <-done
			if r.err != nil {
				assert.ErrorIs(t, r.err, context.Canceled, "goroutine %d", i)
				assert.Equal(t, 1, i%10, "goroutine %d falhou sem desistir: %v", i, r.err)
				gaveUp.Add(1)
				return
			}
			assert.Equal(t, "bom", r.resp.Model, "goroutine %d: o ruim nunca serve", i)
			ring.Push(i)
			assert.NoError(t, store.Set(strconv.Itoa(i), r.resp.ID, time.Hour))
			served.Add(1)
		}(i, tracing.WithContext(context.Background(), sc))
	}
	close(start)
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, workers.Shutdown(ctx))
	require.NoError(t, sup.Shutdown(ctx))

	// Rate limit exato: sem recarga, cada tenant passa burst vezes, nem uma a mais.
	admitted := n - int(rejected.Load())
	want := 0
	for k := 0; k < tenants; k++ {
		want += min(burst, (n-k+tenants-1)/tenants) // goroutines do tenant k
	}
	assert.Equal(t, want, admitted)
	assert.EqualValues(t, admitted, served.Load()+gaveUp.Load())
	// Worker pool: toda tarefa aceita rodou uma vez só, e nenhuma recusada rodou.
	for i := range executed {
		wantRuns := int32(0)
		if submitted[i] {
			wantRuns = 1
		}
		assert.Equal(t, wantRuns, executed[i].Load(), "tarefa %d", i)
	}
	// Ring buffer e state: um evento por sucesso, sequência sem buraco, sem repetição.
	events := ring.Snapshot()
	require.Len(t, events, int(served.Load()))
	seen := map[int]bool{}
	for k, e := range events {
		assert.EqualValues(t, k+1, e.Seq)
		assert.False(t, seen[e.Value], "goroutine %d entrou duas vezes", e.Value)
		seen[e.Value] = true
		_, ok := store.Get(strconv.Itoa(e.Value))
		assert.True(t, ok, "goroutine %d sem estado", e.Value)
	}
	assert.Equal(t, int(served.Load()), store.Len())
	// Disjuntor e upstream: o ruim abriu e o bom nunca falhou; o singleflight
	// nunca chama mais que os pedidos, e toda chamada leva o trace de um deles.
	assert.Equal(t, circuitbreaker.Open, gate.Breaker("ruim").State())
	assert.Equal(t, circuitbreaker.Closed, gate.Breaker("bom").State())
	calls := append(good.Calls(), bad.Calls()...)
	assert.GreaterOrEqual(t, len(bad.Calls()), 3)
	assert.LessOrEqual(t, len(good.Calls()), admitted)
	for _, c := range calls {
		tp := c.Header.Get(tracing.TraceparentHeader)
		require.Len(t, tp, 55)
		assert.True(t, traces[tp[3:35]], "trace-id %s não é de nenhuma goroutine", tp[3:35])
	}

	good.Close()
	bad.Close()
	hc.CloseIdleConnections()
	noLeak(t, before)
	assert.Empty(t, logs.String(), "nenhum panic nem falha de goroutine supervisionada")
}

// TestShutdownUnderLoad encerra o worker pool no meio da enxurrada de
// submissões (quando metade já foi aceita): o que foi aceito roda uma vez só, e
// o que foi recusado (ErrPoolClosed) não roda.
func TestShutdownUnderLoad(t *testing.T) {
	n := load(t)
	before := runtime.NumGoroutine()
	log, logs := testutil.NewLogger(t)
	workers := workerpool.New(pool.Config{Workers: 4, QueueCapacity: 8}, log)
	var accepted, refused, ran atomic.Int64
	start, half := make(chan struct{}), make(chan struct{})
	stopped := make(chan error, 1)
	go func() {
		<-half
		stopped <- workers.Shutdown(context.Background())
	}()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := workers.SubmitContext(context.Background(), func(context.Context) { ran.Add(1) })
			switch {
			case err == nil:
				if accepted.Add(1) == int64(n/2) {
					close(half)
				}
			case errors.Is(err, pool.ErrPoolClosed):
				refused.Add(1)
			default:
				t.Errorf("submit: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	require.NoError(t, <-stopped)
	assert.EqualValues(t, n, accepted.Load()+refused.Load())
	assert.GreaterOrEqual(t, accepted.Load(), int64(n/2))
	assert.Equal(t, accepted.Load(), ran.Load(), "toda tarefa aceita rodou, e uma vez só")
	noLeak(t, before)
	assert.Empty(t, logs.String())
}

// TestRateLimitExactUnderContention martela uma chave só com todas as
// goroutines: sem recarga, passam exatamente Burst, nem uma a mais, por mais
// que as tentativas se cruzem no laço de CAS.
func TestRateLimitExactUnderContention(t *testing.T) {
	n := load(t)
	limiter, err := ratelimit.New(ratelimit.Config{Rate: 1e-3, Burst: n, MaxKeys: 1})
	require.NoError(t, err)
	var admitted atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				if ok, _ := limiter.Allow("k"); ok {
					admitted.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	assert.EqualValues(t, n, admitted.Load())
}

// TestSingleflightNeverHandsOverCanceledCall: com a chamada de uma chave presa
// no upstream, o único chamador desiste e volta; quem pede a mesma chave depois
// tem de ganhar uma chamada nova, nunca a cancelada. Uma chave por goroutine.
func TestSingleflightNeverHandsOverCanceledCall(t *testing.T) {
	n := load(t)
	before := runtime.NumGoroutine()
	var g singleflight.Group[int]
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := strconv.Itoa(i)
			started, release := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			gaveUp := make(chan error, 1)
			go func() {
				_, err := g.Do(ctx, key, func(context.Context) (int, error) {
					close(started)
					<-release // upstream que demora a perceber o cancelamento
					return -1, nil
				})
				gaveUp <- err
			}()
			<-started
			cancel()
			assert.ErrorIs(t, <-gaveUp, context.Canceled)
			// O prazo só pesa se o novo pedido pegar a chamada velha (presa no release).
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			v, err := g.Do(ctx, key, func(context.Context) (int, error) { return i, nil })
			close(release)
			assert.NoError(t, err, "chave %s", key)
			assert.Equal(t, i, v, "chave %s: o pedido novo herdou a chamada cancelada", key)
		}(i)
	}
	wg.Wait()
	noLeak(t, before)
}
