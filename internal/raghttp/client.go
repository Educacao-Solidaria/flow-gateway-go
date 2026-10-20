// Package raghttp implementa o rag.Client (pkg/client/rag) sobre a API HTTP v1
// do brain-router-py (POST /api/v1/search e /api/v1/index), com os schemas
// pydantic do app/schemas/mcp.py dele como fonte da verdade.
//
// A busca é idempotente e repete em falha transitória (rede, prazo da
// tentativa, 408, 429 e 5xx), com espera que dobra entre as tentativas. A
// indexação não repete sozinha: a tentativa que pareceu falhar pode ter
// indexado, e repetir duplicaria o documento. As duas passam pelo mesmo
// disjuntor (internal/circuitbreaker): com o brain-router fora, a chamada falha
// na hora em vez de esgotar prazos.
package raghttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/circuitbreaker"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/tracing"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/rag"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

// maxBody é o teto do corpo de resposta lido: a busca devolve o chunk inteiro,
// embedding incluso (50 resultados de 1536 dimensões ficam perto de 1 MiB).
const maxBody = 8 << 20

// Client implementa rag.Client e é seguro para uso concorrente.
type Client struct {
	base          string
	hc            *http.Client
	breaker       *circuitbreaker.Breaker
	searchTimeout time.Duration // prazo de cada tentativa da busca
	indexTimeout  time.Duration
	attempts      int           // tentativas da busca
	backoff       time.Duration // espera antes da 2ª tentativa; dobra a cada uma
}

var _ rag.Client = (*Client)(nil)

// New cria o cliente do brain-router em baseURL (http ou https) sobre hc, o
// transporte do internal/httpclient; o pool fica com quem o criou.
func New(baseURL string, hc *http.Client) (*Client, error) {
	if u, err := url.Parse(baseURL); hc == nil || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("raghttp: base %q não é uma URL http(s), ou o transporte é nil", baseURL)
	}
	b, err := circuitbreaker.New("rag", circuitbreaker.Settings{FailureThreshold: 5, OpenTimeout: 10 * time.Second, HalfOpenProbes: 1})
	if err != nil {
		return nil, err
	}
	return &Client{base: strings.TrimSuffix(baseURL, "/"), hc: hc, breaker: b, searchTimeout: 2 * time.Second,
		indexTimeout: time.Minute, attempts: 3, backoff: 50 * time.Millisecond}, nil
}

type searchReq struct {
	Query string  `json:"query"`
	TopK  int     `json:"top_k"`
	Alpha float32 `json:"alpha"`
}

// searchResp é o HybridSearchOutput, com do Chunk só o que o rag.ChunkHit usa.
type searchResp struct {
	Query   string `json:"query"`
	Results []struct {
		Chunk struct {
			ID       string         `json:"id"`
			Content  string         `json:"content"`
			Metadata map[string]any `json:"metadata"`
		} `json:"chunk"`
		RRFScore float32 `json:"rrf_score"`
		Rank     int     `json:"rank"`
	} `json:"results"`
	TotalFound      int     `json:"total_found"`
	ExecutionTimeMs float64 `json:"execution_time_ms"`
}

type indexReq struct {
	Title     string `json:"title"`
	Content   string `json:"content"`
	SourceURI string `json:"source_uri"`
}

// HybridSearch implementa rag.Client: results vira hits, rrf_score vira Score
// e o metadata.source_uri do chunk vira SourceURI (vazio se ausente: o Chunk do
// brain-router não tem campo de origem, convenção do contrato).
func (c *Client) HybridSearch(ctx context.Context, query string, topK int, alpha float32) (*rag.SearchResult, error) {
	req := searchReq{query, topK, alpha}
	var out searchResp
	err := c.attempt(ctx, c.searchTimeout, "/api/v1/search", req, &out)
	for i := 1; i < c.attempts && retryable(err); i++ {
		t := time.NewTimer(c.backoff << (i - 1))
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
		err = c.attempt(ctx, c.searchTimeout, "/api/v1/search", req, &out) // cortada no meio não preenche out (o Decoder espera o JSON inteiro)
	}
	if err != nil {
		return nil, err
	}
	hits := make([]rag.ChunkHit, len(out.Results))
	for i, r := range out.Results {
		src, _ := r.Chunk.Metadata["source_uri"].(string)
		hits[i] = rag.ChunkHit{ID: r.Chunk.ID, Content: r.Chunk.Content, SourceURI: src, Score: r.RRFScore, Rank: r.Rank}
	}
	return &rag.SearchResult{Query: out.Query, Hits: hits, TotalFound: out.TotalFound, ExecutionTimeMs: out.ExecutionTimeMs}, nil
}

// IndexDocument implementa rag.Client (uri vira source_uri). Não repete sozinho.
func (c *Client) IndexDocument(ctx context.Context, title string, content string, uri string) (*rag.IndexResult, error) {
	var out rag.IndexResult
	if err := c.attempt(ctx, c.indexTimeout, "/api/v1/index", indexReq{title, content, uri}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Close implementa rag.Client. O transporte é de quem o passou a New.
func (c *Client) Close() error { return nil }

// attempt faz uma chamada com prazo próprio, passando pelo disjuntor. A
// desistência do chamador não conta como falha do brain-router; o prazo
// vencido da tentativa conta, como ErrUpstreamTimeout.
func (c *Client) attempt(ctx context.Context, timeout time.Duration, path string, in, out any) error {
	done, err := c.breaker.Allow()
	if err != nil {
		return err // circuito aberto: falha na hora
	}
	actx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err = c.post(actx, path, in, out)
	switch {
	case err == nil:
	case ctx.Err() != nil:
		done(context.Canceled)
		return ctx.Err()
	case actx.Err() != nil:
		err = fmt.Errorf("%w: brain-router não respondeu em %s", gwerrors.ErrUpstreamTimeout, timeout)
	}
	done(err)
	return err
}

// post manda in em JSON e decodifica em out a resposta 2xx; o resto vira
// *gwerrors.GatewayError com o status e o envelope {"error": {code, message}}.
func (c *Client) post(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("raghttp: codificando o pedido: %w", err) // ex.: alpha NaN
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if id := logger.TraceID(ctx); id != "" {
		req.Header.Set("X-Request-Id", id)
	}
	tracing.Inject(ctx, req.Header)
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { // até o EOF, para a conexão voltar ao pool
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBody))
		_ = resp.Body.Close()
	}()
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxBody))
	if resp.StatusCode/100 == 2 {
		if err := dec.Decode(out); err != nil {
			return fmt.Errorf("raghttp: resposta ilegível: %w", err)
		}
		return nil
	}
	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if _ = dec.Decode(&env); env.Error.Code == "" { // corpo fora do envelope (ex.: proxy no meio)
		env.Error.Code, env.Error.Message = "http_"+strconv.Itoa(resp.StatusCode), resp.Status
	}
	return gwerrors.New(env.Error.Code, "brain-router: "+env.Error.Message, resp.StatusCode, 0)
}

// retryable: falha de transporte, prazo da tentativa, 408, 429 e 5xx. Circuito
// aberto, erro do pedido (4xx), resposta ilegível e desistência não repetem.
func retryable(err error) bool {
	var ue *url.Error
	var ge *gwerrors.GatewayError
	switch {
	case errors.Is(err, gwerrors.ErrUpstreamTimeout), errors.As(err, &ue):
		return true
	case errors.As(err, &ge):
		return ge.HTTPStatus == http.StatusRequestTimeout || ge.HTTPStatus == http.StatusTooManyRequests || ge.HTTPStatus >= 500
	}
	return false
}
