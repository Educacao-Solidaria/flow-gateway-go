package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/cachewriter"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ipc"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/raghttp"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/resilience"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/sse"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil/openroutertest"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/udscache"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/rag"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/openrouter"
)

// gateway é a composição que a rota /v1/chat/completions do Luan (PR 66) vai
// montar: Lookup no cache (erro = miss), contexto do RAG no miss (erro = sem
// contexto), o upstream com retry e fallback, o cachewriter no meio do stream e
// o sse.Writer até o cliente.
type gateway struct {
	cache  cache.Client
	rag    rag.Client
	llm    openrouter.Client
	writer *cachewriter.Writer
}

func (g gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req domain.ChatCompletionRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // até o EOF: só assim o servidor percebe quem desconecta
	if err != nil || json.Unmarshal(body, &req) != nil || len(req.Messages) == 0 {
		http.Error(w, "pedido inválido", http.StatusBadRequest)
		return
	}
	ctx, prompt := r.Context(), req.Messages[len(req.Messages)-1].Content
	emb := embed(prompt)
	sw := sse.NewWriter(w)
	defer sw.Close()
	if hit, err := g.cache.Lookup(ctx, prompt, emb, 0.95); err == nil && hit.Hit {
		_ = sw.Chunk(1, domain.StreamChunk{Model: req.Model, DeltaContent: hit.CachedResponse, FinishReason: "stop", IsLast: true})
		_ = sw.Event("done", map[string]string{"finish_reason": "stop", "source": "cache"})
		return
	}
	if found, err := g.rag.HybridSearch(ctx, prompt, 3, 0.5); err == nil {
		var sys []domain.ChatMessage
		for _, h := range found.Hits {
			sys = append(sys, domain.ChatMessage{Role: domain.RoleSystem, Content: h.Content})
		}
		req.Messages = append(sys, req.Messages...)
	}
	req.Stream = true
	chunks, errs, err := g.llm.StreamChatCompletion(ctx, &req)
	if err == nil {
		chunks, errs = g.writer.Tee(ctx, prompt, emb, chunks, errs)
		var seq uint64
		for c := range chunks {
			seq++
			_ = sw.Chunk(seq, c)
		}
		err = <-errs
	}
	if err != nil {
		code := "INTERNAL_ERROR"
		if ge := (*gwerrors.GatewayError)(nil); errors.As(err, &ge) {
			code = ge.Code
		}
		_ = sw.Event("error", map[string]string{"code": code, "message": err.Error()})
		return
	}
	_ = sw.Event("done", map[string]string{"finish_reason": "stop", "source": "upstream"})
}

// embed é o embedding de mentira (o serviço de embeddings não é deste lote):
// 1536 dimensões determinísticas por prompt, o tamanho real no IPC.
func embed(prompt string) []float32 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(prompt))
	r := rand.New(rand.NewPCG(h.Sum64(), 0))
	v := make([]float32, 1536)
	for i := range v {
		v[i] = r.Float32()*2 - 1
	}
	return v
}

// upstream é o openrouter.Client mínimo de stream (o cliente HTTP real da
// OpenRouter ainda não existe): status de erro vira GatewayError com ele, que
// é o que a resiliência classifica, e o corpo vai para o sse.Decode.
type upstream struct {
	base string
	hc   *http.Client
}

func (upstream) CreateChatCompletion(context.Context, *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	return nil, errors.New("e2e: só stream")
}

func (u upstream) StreamChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.base+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	res, err := u.hc.Do(hreq)
	if err != nil {
		return nil, nil, err
	}
	if res.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
		return nil, nil, gwerrors.New("upstream_error", res.Status, res.StatusCode, -32603)
	}
	chunks, errs := sse.Decode(ctx, res.Body, 1<<20) // o Decode fecha o corpo
	return chunks, errs, nil
}

// semcache é o semantic-cache de mentira: o cache.InMemoryClient do
// pkg/client/cache atrás do IPC v1, num socket de caminho curto (o handler do
// semantic-cache-rs de hoje ainda não tem LOOKUP nem STORE).
func semcache(t *testing.T) (string, *cache.InMemoryClient) {
	mem := cache.NewInMemoryClient()
	dir, err := os.MkdirTemp("", "sc") // o sun_path do macOS tem 104 bytes
	require.NoError(t, err)
	path := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close(); _ = os.RemoveAll(dir) })
	go func() {
		for nc, err := ln.Accept(); err == nil; nc, err = ln.Accept() {
			go func() {
				defer nc.Close()
				r := bufio.NewReader(nc)
				for f, err := ipc.ReadFrame(r); err == nil; f, err = ipc.ReadFrame(r) {
					var req struct {
						Prompt, Response string
						Embedding        []float32
						Threshold        float32
						TTL              int `json:"ttl_seconds"`
					}
					_ = json.Unmarshal(f.Payload, &req)
					var out any = struct{}{}
					kind, ctx := f.Kind+1, context.Background()
					switch f.Kind {
					case ipc.KindLookupReq:
						out, _ = mem.Lookup(ctx, req.Prompt, req.Embedding, req.Threshold)
					case ipc.KindStoreReq:
						_ = mem.Store(ctx, req.Prompt, req.Response, req.Embedding, req.TTL)
					default:
						kind, out = ipc.KindError, udscache.ServerError{Code: ipc.CodeUnsupported, Message: "kind"}
					}
					payload, _ := json.Marshal(out)
					raw, _ := ipc.Append(nil, ipc.Frame{Kind: kind, RequestID: f.RequestID, Payload: payload})
					if _, err := nc.Write(raw); err != nil {
						return
					}
				}
			}()
		}
	}()
	return path, mem
}

// brainRouter é o brain-router de mentira: toda busca devolve um trecho com content.
func brainRouter(t *testing.T, content string) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"query": "q", "total_found": 1, "execution_time_ms": 1,
			"results": []any{map[string]any{"rank": 1, "rrf_score": 0.9, "chunk": map[string]any{"id": "c1", "content": content}}}})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// env é o gateway de pé sobre os fakes; hc é o transporte do cliente do teste,
// do upstream e do RAG.
type env struct {
	url    string
	hc     *http.Client
	cache  *udscache.Client
	writer *cachewriter.Writer
}

// stack sobe o gateway com o upstream em llm, o semantic-cache em sock e o
// brain-router em brain; o encerramento drena o cachewriter.
func stack(t *testing.T, llm, sock, brain string) env {
	log, _ := testutil.NewLogger(t)
	hc := &http.Client{Transport: &http.Transport{}}
	uc := udscache.New(sock, -1, log)
	rc, err := raghttp.New(brain, hc)
	require.NoError(t, err)
	retry, err := resilience.NewRetry(upstream{llm, hc}, resilience.RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond})
	require.NoError(t, err)
	llmc, err := resilience.New(retry, resilience.Options{AttemptTimeout: 5 * time.Second, Fallbacks: map[string][]string{"m": {"m-reserva"}}})
	require.NoError(t, err)
	w := cachewriter.New(uc, cachewriter.Options{}, log)
	ctx, stop := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- w.Run(ctx) }()
	srv := httptest.NewServer(gateway{cache: uc, rag: rc, llm: llmc, writer: w})
	t.Cleanup(func() {
		srv.Close()
		stop()
		assert.NoError(t, <-ran)
		_ = uc.Close()
		hc.CloseIdleConnections()
	})
	return env{srv.URL, hc, uc, w}
}

// post manda o prompt ao gateway.
func post(t *testing.T, e env, prompt string) *http.Response {
	b, err := json.Marshal(domain.ChatCompletionRequest{Model: "m", Messages: []domain.ChatMessage{{Role: domain.RoleUser, Content: prompt}}})
	require.NoError(t, err)
	resp, err := e.hc.Post(e.url, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	return resp
}

// chat lê o SSE do gateway com o sse.Reader e devolve o texto dos pedaços, o
// último evento (done ou error) e o corpo cru.
func chat(t *testing.T, e env, prompt string) (text string, last sse.Event, raw string) {
	t.Helper()
	resp := post(t, e, prompt)
	defer resp.Body.Close()
	var buf strings.Builder
	r := sse.NewReader(io.TeeReader(resp.Body, &buf), 1<<20)
	for {
		ev, err := r.Next()
		if errors.Is(err, io.EOF) {
			return text, last, buf.String()
		}
		require.NoError(t, err)
		if ev.Event == "chunk" {
			var c domain.StreamChunk
			require.NoError(t, json.Unmarshal([]byte(ev.Data), &c))
			text += c.DeltaContent
		}
		last = ev
	}
}

// waitFor espera cond por até 10 s: a gravação no cache é em segundo plano, sem aviso.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		require.True(t, time.Now().Before(deadline), "a condição não chegou em 10 s")
	}
}

// Miss: o pedido sai para o upstream com o contexto do RAG na frente, os
// pedaços chegam em ordem e numerados, e a resposta inteira vai para o cache.
// O mesmo prompt depois sai do cache, sem chamar o upstream.
func TestStreamMissThenCacheHit(t *testing.T) {
	or := openroutertest.New(t)
	or.Enqueue(openroutertest.Response{Chunks: []string{"Olá", ", ", "mundo"}})
	sock, mem := semcache(t)
	e := stack(t, or.URL, sock, brainRouter(t, "trecho do RAG"))

	text, last, raw := chat(t, e, "oi")
	assert.Equal(t, "Olá, mundo", text)
	assert.Equal(t, "done", last.Event)
	assert.Contains(t, raw, "id: 3\nevent: chunk\n")
	calls := or.Calls()
	require.Len(t, calls, 1)
	assert.True(t, calls[0].Request.Stream)
	assert.Equal(t, []domain.ChatMessage{{Role: domain.RoleSystem, Content: "trecho do RAG"}, {Role: domain.RoleUser, Content: "oi"}}, calls[0].Request.Messages)

	waitFor(t, func() bool { return e.writer.Stats().Stored == 1 })
	hit, err := mem.Lookup(context.Background(), "oi", nil, 1)
	require.NoError(t, err)
	assert.Equal(t, "Olá, mundo", hit.CachedResponse)
	text, last, _ = chat(t, e, "oi")
	assert.Equal(t, "Olá, mundo", text)
	assert.Contains(t, last.Data, `"source":"cache"`)
	assert.Len(t, or.Calls(), 1, "o hit não chama o upstream")
}

// O upstream falha antes do primeiro pedaço: o retry repete o modelo, o
// fallback passa ao reserva, que responde, e o cliente não vê erro.
func TestFailureBeforeTheFirstChunkIsRetriedAndFallsBack(t *testing.T) {
	or := openroutertest.New(t)
	or.Enqueue(openroutertest.ServerError(503), openroutertest.ServerError(503), openroutertest.Response{Chunks: []string{"da ", "reserva"}})
	sock, _ := semcache(t)
	e := stack(t, or.URL, sock, brainRouter(t, "trecho"))
	text, last, raw := chat(t, e, "oi")
	assert.Equal(t, "da reserva", text)
	assert.Equal(t, "done", last.Event)
	assert.Contains(t, raw, `"model":"m-reserva"`)
	var models []string
	for _, c := range or.Calls() {
		models = append(models, c.Request.Model)
	}
	assert.Equal(t, []string{"m", "m", "m-reserva"}, models)
}

// Erro do provedor no meio do stream: o cliente recebe o que já tinha saído e
// o evento error; nada repete (já houve pedaço) e nada vai para o cache.
func TestMidStreamErrorReachesTheClientAndIsNotCached(t *testing.T) {
	var calls atomic.Int32
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"g1","model":"m","choices":[{"delta":{"content":"meia "}}]}`+"\n\n"+
			`data: {"error":{"code":502,"message":"o provedor caiu"}}`+"\n\n")
	}))
	t.Cleanup(llm.Close)
	sock, mem := semcache(t)
	e := stack(t, llm.URL, sock, brainRouter(t, "trecho"))
	text, last, _ := chat(t, e, "oi")
	assert.Equal(t, "meia ", text)
	assert.Equal(t, "error", last.Event)
	assert.Contains(t, last.Data, "UPSTREAM_STREAM_ERROR")
	assert.EqualValues(t, 1, calls.Load())
	assert.Equal(t, cachewriter.Stats{}, e.writer.Stats())
	miss, err := mem.Lookup(context.Background(), "oi", nil, 1)
	require.NoError(t, err)
	assert.False(t, miss.Hit)
}

// Cache fora do ar e brain-router com o backend fora (503): o Lookup falha na
// hora e vira miss, a busca vira resposta sem contexto, e a gravação no cache
// falha em segundo plano. O cliente recebe o stream inteiro do mesmo jeito.
func TestStreamDegradesWithoutCacheAndRAG(t *testing.T) {
	or := openroutertest.New(t)
	or.Enqueue(openroutertest.Response{Chunks: []string{"sem ", "ajuda"}})
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(brain.Close)
	e := stack(t, or.URL, filepath.Join(t.TempDir(), "nada.sock"), brain.URL)
	text, last, _ := chat(t, e, "oi")
	assert.Equal(t, "sem ajuda", text)
	assert.Equal(t, "done", last.Event)
	calls := or.Calls()
	require.Len(t, calls, 1)
	assert.Len(t, calls[0].Request.Messages, 1, "sem o contexto do RAG")
	waitFor(t, func() bool { return e.writer.Stats().Failed == 1 })
}

// O cliente que desconecta no meio do stream cancela o upstream, e tudo que a
// requisição abriu (leitura do SSE, Tee, conexões) termina.
func TestClientDisconnectCancelsTheUpstream(t *testing.T) {
	gone := make(chan struct{})
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // sem ler até o EOF, o servidor não percebe a desconexão
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"g1","model":"m","choices":[{"delta":{"content":"começo"}}]}`+"\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(gone)
	}))
	t.Cleanup(llm.Close)
	sock, _ := semcache(t)
	e := stack(t, llm.URL, sock, brainRouter(t, "trecho"))
	// A conexão com o semantic-cache abre antes da contagem: fechá-la no fim
	// dependeria do AF_UNIX do Windows entregar o fechamento ao fake (no CI, às
	// vezes não entrega). O STATS volta unsupported do fake, e a conexão fica.
	_, _ = e.cache.Stats(context.Background())
	before := runtime.NumGoroutine()

	resp := post(t, e, "oi")
	ev, err := sse.NewReader(resp.Body, 1<<20).Next()
	require.NoError(t, err)
	assert.Contains(t, ev.Data, "começo")
	require.NoError(t, resp.Body.Close()) // o cliente vai embora no meio
	select {
	case <-gone:
	case <-time.After(10 * time.Second):
		t.Fatal("o upstream não foi cancelado")
	}
	e.hc.CloseIdleConnections()
	testutil.NoGoroutineLeak(t, before)
}
