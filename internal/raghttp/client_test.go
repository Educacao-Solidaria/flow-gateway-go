package raghttp

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/circuitbreaker"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/tracing"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/rag"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

// fake é o brain-router de mentira: conta as chamadas e responde com handle,
// que recebe o número da chamada (1, 2, ...) e o corpo, já lido inteiro — o
// servidor do Go só percebe o cliente que desconecta depois do fim do corpo.
func fake(t *testing.T, handle func(n int32, body string, w http.ResponseWriter, r *http.Request)) (*Client, *atomic.Int32) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		handle(calls.Add(1), string(body), w, r)
	}))
	t.Cleanup(srv.Close)
	c, err := New(srv.URL+"/", srv.Client())
	require.NoError(t, err)
	c.searchTimeout, c.backoff = 200*time.Millisecond, time.Millisecond
	return c, &calls
}

func reply(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

const searchOK = `{"total_found":2,"execution_time_ms":3.5,"query":"q","results":[
 {"rank":1,"rrf_score":0.9,"dense_score":0.8,"sparse_score":null,"chunk":{"id":"c1","document_id":"d1","content":"um",
  "embedding":[0.1,0.2],"token_count":1,"metadata":{"source_uri":"file:///a.md","position":{"start":0}}}},
 {"rank":2,"rrf_score":1,"chunk":{"id":"c2","document_id":"d2","content":"dois","metadata":{}}}]}`

func TestSearchFollowsContract(t *testing.T) {
	sc := tracing.New()
	ctx := tracing.WithContext(logger.WithTraceID(context.Background(), "req-1"), sc)
	c, calls := fake(t, func(_ int32, body string, w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST /api/v1/search", r.Method+" "+r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "req-1", r.Header.Get("X-Request-Id"))
		assert.Contains(t, r.Header.Get("Traceparent"), sc.TraceID.String())
		assert.JSONEq(t, `{"query":"q","top_k":5,"alpha":0.5}`, body)
		reply(w, http.StatusOK, searchOK)
	})
	got, err := c.HybridSearch(ctx, "q", 5, 0.5)
	require.NoError(t, err)
	assert.Equal(t, &rag.SearchResult{Query: "q", TotalFound: 2, ExecutionTimeMs: 3.5, Hits: []rag.ChunkHit{
		{ID: "c1", Content: "um", SourceURI: "file:///a.md", Score: 0.9, Rank: 1},
		{ID: "c2", Content: "dois", Score: 1, Rank: 2}, // sem source_uri no metadata: vazio, sem erro
	}}, got)
	assert.EqualValues(t, 1, calls.Load())

	// Zero resultados é lista vazia, não nil.
	c, _ = fake(t, func(_ int32, _ string, w http.ResponseWriter, _ *http.Request) {
		reply(w, http.StatusOK, `{"query":"q","results":[],"total_found":0,"execution_time_ms":1}`)
	})
	got, err = c.HybridSearch(context.Background(), "q", 5, 0.5)
	require.NoError(t, err)
	assert.NotNil(t, got.Hits)
	assert.Empty(t, got.Hits)
}

func TestSearchRetriesOnlyTransientFailures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		responses []int // status por chamada; o último se repete
		calls     int32
		wantErr   int // status do GatewayError esperado; 0 = sucesso
	}{
		{"503 e 500 e depois 200", []int{503, 500, 200}, 3, 0},
		{"conexão cortada repete", []int{-1, 200}, 2, 0},
		{"429 repete", []int{429, 200}, 2, 0},
		{"422 não repete", []int{422}, 1, 422},
		{"404 não repete", []int{404}, 1, 404},
		{"esgota as tentativas", []int{502}, 3, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, calls := fake(t, func(n int32, _ string, w http.ResponseWriter, _ *http.Request) {
				status := tc.responses[min(int(n), len(tc.responses))-1]
				if status < 0 { // corta sem resposta: falha de transporte
					nc, _, _ := w.(http.Hijacker).Hijack()
					_ = nc.Close()
					return
				}
				if status == http.StatusOK {
					reply(w, status, searchOK)
					return
				}
				reply(w, status, `{"error":{"code":"backend_unavailable","message":"a busca não está configurada"}}`)
			})
			got, err := c.HybridSearch(context.Background(), "q", 5, 0.5)
			assert.Equal(t, tc.calls, calls.Load())
			if tc.wantErr == 0 {
				require.NoError(t, err)
				assert.Len(t, got.Hits, 2)
				return
			}
			var ge *gwerrors.GatewayError
			require.ErrorAs(t, err, &ge)
			assert.Equal(t, tc.wantErr, ge.HTTPStatus)
			assert.Equal(t, "backend_unavailable", ge.Code)
			assert.Equal(t, "brain-router: a busca não está configurada", ge.Message)
		})
	}
}

func TestSearchAttemptTimeoutAndCallerCancel(t *testing.T) {
	// A 1ª tentativa trava até o prazo dela; a 2ª responde.
	c, calls := fake(t, func(n int32, _ string, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			<-r.Context().Done()
			return
		}
		reply(w, http.StatusOK, searchOK)
	})
	c.searchTimeout = 300 * time.Millisecond // folga para a 2ª responder em runner lento
	_, err := c.HybridSearch(context.Background(), "q", 5, 0.5)
	require.NoError(t, err)
	assert.EqualValues(t, 2, calls.Load())

	// Todas travam: o erro é o prazo do upstream, depois de todas as tentativas.
	c, calls = fake(t, func(_ int32, _ string, _ http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	c.searchTimeout = 100 * time.Millisecond
	_, err = c.HybridSearch(context.Background(), "q", 5, 0.5)
	require.ErrorIs(t, err, gwerrors.ErrUpstreamTimeout)
	assert.EqualValues(t, 3, calls.Load())

	// O chamador que desiste para tudo: nem repete, nem conta para o disjuntor.
	ctx, cancel := context.WithCancel(context.Background())
	c, calls = fake(t, func(_ int32, _ string, _ http.ResponseWriter, r *http.Request) {
		cancel()
		<-r.Context().Done()
	})
	c.breaker, _ = circuitbreaker.New("rag", circuitbreaker.Settings{FailureThreshold: 1, OpenTimeout: time.Hour, HalfOpenProbes: 1})
	_, err = c.HybridSearch(ctx, "q", 5, 0.5)
	require.ErrorIs(t, err, context.Canceled)
	assert.EqualValues(t, 1, calls.Load())
	assert.Equal(t, circuitbreaker.Closed, c.breaker.State())
}

func TestBreakerOpensAndFailsFast(t *testing.T) {
	c, calls := fake(t, func(_ int32, _ string, w http.ResponseWriter, _ *http.Request) {
		reply(w, http.StatusServiceUnavailable, `{"error":{"code":"backend_unavailable","message":"x"}}`)
	})
	for i := 0; i < 2; i++ { // 2 buscas × 3 tentativas passam do limiar de 5 falhas
		_, _ = c.HybridSearch(context.Background(), "q", 5, 0.5)
	}
	before := calls.Load()
	_, err := c.HybridSearch(context.Background(), "q", 5, 0.5)
	require.ErrorIs(t, err, gwerrors.ErrCircuitOpen)
	_, err = c.IndexDocument(context.Background(), "t", "c", "u")
	require.ErrorIs(t, err, gwerrors.ErrCircuitOpen)
	assert.Equal(t, before, calls.Load(), "circuito aberto não chama o brain-router")
}

func TestIndexDocument(t *testing.T) {
	c, calls := fake(t, func(n int32, body string, w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST /api/v1/index", r.Method+" "+r.URL.Path)
		assert.JSONEq(t, `{"title":"t","content":"c","source_uri":"file:///a.md"}`, body)
		if n == 1 {
			reply(w, http.StatusCreated, `{"document_id":"d1","chunks_created":3,"success":true,"message":"ok"}`)
			return
		}
		reply(w, http.StatusServiceUnavailable, `{"error":{"code":"backend_unavailable","message":"x"}}`)
	})
	got, err := c.IndexDocument(context.Background(), "t", "c", "file:///a.md")
	require.NoError(t, err)
	assert.Equal(t, &rag.IndexResult{DocumentID: "d1", ChunksCreated: 3, Success: true}, got)

	// 503 na indexação não repete: a tentativa pode ter indexado.
	_, err = c.IndexDocument(context.Background(), "t", "c", "file:///a.md")
	var ge *gwerrors.GatewayError
	require.ErrorAs(t, err, &ge)
	assert.Equal(t, http.StatusServiceUnavailable, ge.HTTPStatus)
	assert.EqualValues(t, 2, calls.Load())
}

func TestDegenerateInputsAndResponses(t *testing.T) {
	_, err := New("brain-router:8000", http.DefaultClient)
	require.Error(t, err, "base sem esquema")
	_, err = New("http://brain-router:8000", nil)
	require.Error(t, err, "sem transporte")

	c, calls := fake(t, func(n int32, _ string, w http.ResponseWriter, _ *http.Request) {
		switch n {
		case 1:
			reply(w, http.StatusOK, `{"results": [`) // corta no meio
		default:
			reply(w, http.StatusBadGateway, `{"detail":"Bad Gateway"}`) // proxy no meio: corpo fora do envelope
		}
	})
	_, err = c.HybridSearch(context.Background(), "q", 5, float32(math.NaN()))
	require.ErrorContains(t, err, "NaN")
	assert.Zero(t, calls.Load(), "pedido que não codifica não sai")

	_, err = c.HybridSearch(context.Background(), "q", 5, 0.5)
	require.ErrorContains(t, err, "resposta ilegível")
	assert.EqualValues(t, 1, calls.Load(), "resposta ilegível não repete")

	_, err = c.HybridSearch(context.Background(), "q", 5, 0.5)
	var ge *gwerrors.GatewayError
	require.ErrorAs(t, err, &ge)
	assert.Equal(t, "http_502", ge.Code)

	// Sem servidor: falha de transporte, repetida até o fim das tentativas.
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	c, err = New(srv.URL, http.DefaultClient)
	require.NoError(t, err)
	c.backoff = time.Millisecond
	_, err = c.HybridSearch(context.Background(), "q", 5, 0.5)
	var ue *url.Error
	require.ErrorAs(t, err, &ue)
}
