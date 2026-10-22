// Package gatewaytest monta o gateway sobre fakes para a suíte ponta a ponta
// (internal/e2e) e a carga (test/load): Route é a composição que a rota
// /v1/chat/completions do Luan (PR 66) vai montar, e Start a põe atrás do
// servidor de verdade, com fakes sem estado.
package gatewaytest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/cachewriter"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ipc"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/sse"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/udscache"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/rag"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/openrouter"
)

// Route é a composição da rota /v1/chat/completions: Lookup no cache (erro =
// miss), contexto do RAG no miss (erro = sem contexto), o upstream, o
// cachewriter no meio do stream e o sse.Writer até o cliente.
type Route struct {
	Cache  cache.Client
	RAG    rag.Client
	LLM    openrouter.Client
	Writer *cachewriter.Writer
}

func (g Route) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req domain.ChatCompletionRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // até o EOF: só assim o servidor percebe quem desconecta
	if err != nil || json.Unmarshal(body, &req) != nil || len(req.Messages) == 0 {
		http.Error(w, "pedido inválido", http.StatusBadRequest)
		return
	}
	ctx, prompt := r.Context(), req.Messages[len(req.Messages)-1].Content
	emb := Embed(prompt)
	sw := sse.NewWriter(w)
	defer sw.Close()
	if hit, err := g.Cache.Lookup(ctx, prompt, emb, 0.95); err == nil && hit.Hit {
		_ = sw.Chunk(1, domain.StreamChunk{Model: req.Model, DeltaContent: hit.CachedResponse, FinishReason: "stop", IsLast: true})
		_ = sw.Event("done", map[string]string{"finish_reason": "stop", "source": "cache"})
		return
	}
	if found, err := g.RAG.HybridSearch(ctx, prompt, 3, 0.5); err == nil {
		var sys []domain.ChatMessage
		for _, h := range found.Hits {
			sys = append(sys, domain.ChatMessage{Role: domain.RoleSystem, Content: h.Content})
		}
		req.Messages = append(sys, req.Messages...)
	}
	req.Stream = true
	chunks, errs, err := g.LLM.StreamChatCompletion(ctx, &req)
	if err == nil {
		chunks, errs = g.Writer.Tee(ctx, prompt, emb, chunks, errs)
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

// Embed é o embedding de mentira (o serviço de embeddings ainda não existe):
// 1536 dimensões determinísticas por prompt, o tamanho real no IPC.
func Embed(prompt string) []float32 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(prompt))
	r := rand.New(rand.NewPCG(h.Sum64(), 0))
	v := make([]float32, 1536)
	for i := range v {
		v[i] = r.Float32()*2 - 1
	}
	return v
}

// Upstream é o openrouter.Client mínimo de stream sobre HTTP (o cliente HTTP
// real da OpenRouter ainda não existe): status de erro vira GatewayError com
// ele, que é o que a resiliência classifica, e o corpo vai para o sse.Decode.
type Upstream struct {
	URL string // base da API: o pedido vai para URL+"/chat/completions"
	HC  *http.Client
}

func (Upstream) CreateChatCompletion(context.Context, *domain.ChatCompletionRequest) (*domain.ChatCompletionResponse, error) {
	return nil, errors.New("gatewaytest: só stream")
}

func (u Upstream) StreamChatCompletion(ctx context.Context, req *domain.ChatCompletionRequest) (<-chan domain.StreamChunk, <-chan error, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.URL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	res, err := u.HC.Do(hreq)
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

// ServeCache atende o IPC v1 em ln, com c no papel do semantic-cache-rs, até ln
// fechar: LOOKUP_REQ e STORE_REQ; outro kind recebe ERROR unsupported. Cada
// frame roda na sua goroutine (o udscache casa a resposta pelo request_id): um
// frame por vez, na conexão única do udscache, poria a decodificação de toda a
// carga numa goroutine só, e o gargalo seria o fake, não o gateway.
func ServeCache(ln net.Listener, c cache.Client) {
	for nc, err := ln.Accept(); err == nil; nc, err = ln.Accept() {
		go func() {
			defer nc.Close()
			r := bufio.NewReader(nc)
			for f, err := ipc.ReadFrame(r); err == nil; f, err = ipc.ReadFrame(r) {
				go func() {
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
						out, _ = c.Lookup(ctx, req.Prompt, req.Embedding, req.Threshold)
					case ipc.KindStoreReq:
						_ = c.Store(ctx, req.Prompt, req.Response, req.Embedding, req.TTL)
					default:
						kind, out = ipc.KindError, udscache.ServerError{Code: ipc.CodeUnsupported, Message: "kind"}
					}
					payload, _ := json.Marshal(out)
					raw, _ := ipc.Append(nil, ipc.Frame{Kind: kind, RequestID: f.RequestID, Payload: payload})
					_, _ = nc.Write(raw) // um Write por frame: o net.Conn não intercala Writes concorrentes; com a conexão caída, a leitura também acaba
				}()
			}
		}()
	}
}

// BrainRouter é o brain-router de mentira: toda busca devolve um trecho com content.
func BrainRouter(content string) http.Handler {
	body, _ := json.Marshal(map[string]any{"query": "q", "total_found": 1, "execution_time_ms": 1,
		"results": []any{map[string]any{"rank": 1, "rrf_score": 0.9, "chunk": map[string]any{"id": "c1", "content": content}}}})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

// Marcas que a OpenRouter de mentira (Stream) procura no pedido, para quem
// precisa dos caminhos de erro do gateway (o soak): o prompt com FailPrefix
// recebe 503 em toda tentativa; com CutPrefix, a conexão cai depois do 1º pedaço.
const (
	FailPrefix = "falha:"
	CutPrefix  = "corte:"
)

// Stream é a OpenRouter de mentira da carga: o comentário de keep-alive na
// hora, n pedaços SSE (o 1º após ttft, os outros a cada gap, o último com
// finish_reason "stop") e o [DONE]; ver FailPrefix e CutPrefix. Sem estado: o
// openroutertest anota cada chamada e cresceria sem parar sob carga.
func Stream(ttft, gap time.Duration, n int) http.Handler {
	events := make([][]byte, n)
	for i := range events {
		choice := domain.Choice{Delta: &domain.ChatMessage{Role: domain.RoleAssistant, Content: fmt.Sprintf("pedaço %d ", i)}}
		if i == n-1 {
			choice.FinishReason = "stop"
		}
		data, _ := json.Marshal(domain.ChatCompletionResponse{ID: "gen-load", Object: "chat.completion.chunk", Model: "m", Choices: []domain.Choice{choice}})
		events[i] = fmt.Appendf(nil, "data: %s\n\n", data)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body) // até o EOF: só assim o servidor percebe quem desconecta
		if bytes.Contains(body, []byte(FailPrefix)) {
			http.Error(w, "falha de mentira", http.StatusServiceUnavailable)
			return
		}
		cut := bytes.Contains(body, []byte(CutPrefix))
		w.Header().Set("Content-Type", "text/event-stream")
		rc := http.NewResponseController(w)
		_, _ = io.WriteString(w, ": OPENROUTER PROCESSING\n\n")
		_ = rc.Flush()
		t := time.NewTimer(ttft)
		defer t.Stop()
		for i, ev := range events {
			if i > 0 {
				t.Reset(gap)
			}
			select {
			case <-r.Context().Done():
				return
			case <-t.C:
			}
			_, _ = w.Write(ev)
			_ = rc.Flush()
			if cut {
				panic(http.ErrAbortHandler) // o servidor derruba a conexão sem fechar o corpo
			}
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
}
