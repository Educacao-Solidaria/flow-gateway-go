package metrics

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"

	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

// MaxModels é o teto de valores distintos do label model: do 51º em diante, e
// também nome vazio, "other" literal, UTF-8 inválido ou mais de maxModelLen
// bytes, tudo conta como model="other".
const MaxModels, maxModelLen = 50, 128

// promBuckets são os limites dos histogramas do gateway (HTTP, upstream e
// TTFT) no contrato da Fase 4. Cada um é também limite do Latency, então o
// bucket cumulativo sai exato da soma dos buckets internos.
var promBuckets = [...]time.Duration{
	5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond,
	100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, 800 * time.Millisecond,
	time.Second, 2500 * time.Millisecond, 5 * time.Second, 10 * time.Second, 30 * time.Second,
}

// Valores dos labels de conjunto fixo, na ordem dos índices usados abaixo.
var (
	outcomes   = [...]string{"ok", "error", "timeout", "canceled"}
	directions = [...]string{"input", "output"}
	results    = [...]string{"hit", "miss", "error"}
)

// Gateway reúne as métricas com os nomes e os labels do contrato da Fase 4
// (docs/observability.md). O valor zero está pronto; não copie depois de usar.
// Cada registro acha a série sob RLock e soma atômico; só série nova toma o Lock.
type Gateway struct {
	inFlight atomic.Int64
	cache    [len(results)]atomic.Uint64

	mu        sync.RWMutex
	routes    map[routeKey]*Latency
	codes     map[codeKey]*atomic.Uint64
	models    map[string]*modelSeries
	other     modelSeries
	usedOther atomic.Bool
}

type routeKey struct{ route, method string }

type codeKey struct {
	routeKey
	code int
}

type modelSeries struct {
	ttft     Latency
	requests [len(outcomes)]atomic.Uint64
	tokens   [len(directions)]atomic.Uint64
}

// Middleware mede cada requisição: in-flight enquanto o handler roda e, quando
// ele volta (panic e cancelamento inclusive), a contagem por (route, method,
// code) e a duração por (route, method). route é o template do chi lido depois
// do roteamento, nunca o path cru, e "unmatched" quando nenhuma rota casou;
// método fora dos do HTTP vira "other". Monte por fora do Recoverer, para o
// panic chegar aqui como 500.
func (g *Gateway) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.inFlight.Add(1)
		start := time.Now()
		ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
		defer func() {
			g.inFlight.Add(-1)
			k := routeKey{cmp.Or(chi.RouteContext(r.Context()).RoutePattern(), "unmatched"), method(r.Method)}
			code := cmp.Or(ww.Status(), http.StatusOK) // nada escrito: o net/http responde 200
			get(&g.mu, &g.codes, codeKey{k, code}, 0).Add(1)
			get(&g.mu, &g.routes, k, 0).Observe(time.Since(start))
		}()
		next.ServeHTTP(ww, r)
	})
}

func method(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return m
	}
	return "other" // o método vem do cliente: sem isto, cada um viraria uma série
}

// ObserveTTFT registra o tempo até o primeiro token de uma chamada ao model.
// Passe o nome já validado (o que a OpenRouter respondeu), não o texto cru do
// cliente: os primeiros MaxModels nomes ficam com as séries.
func (g *Gateway) ObserveTTFT(model string, d time.Duration) { g.model(model).ttft.Observe(d) }

// LLMRequest registra o fim de uma chamada ao model pelo erro que ela devolveu:
// nil = ok; context.Canceled = canceled (o chamador desistiu); prazo estourado
// ou gwerrors.ErrUpstreamTimeout = timeout; o resto = error.
func (g *Gateway) LLMRequest(model string, err error) {
	o := 1
	switch {
	case err == nil:
		o = 0
	case errors.Is(err, context.Canceled):
		o = 3
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, gwerrors.ErrUpstreamTimeout):
		o = 2
	}
	g.model(model).requests[o].Add(1)
}

// LLMTokens soma os tokens de entrada (prompt) e de saída (completion) de uma
// chamada ao model, como vêm no domain.Usage; negativo não conta.
func (g *Gateway) LLMTokens(model string, input, output int) {
	s := g.model(model)
	s.tokens[0].Add(uint64(max(input, 0)))
	s.tokens[1].Add(uint64(max(output, 0)))
}

// CacheLookup registra uma consulta ao semantic-cache: com erro, error; sem,
// hit ou miss.
func (g *Gateway) CacheLookup(hit bool, err error) {
	i := 1
	if err != nil {
		i = 2
	} else if hit {
		i = 0
	}
	g.cache[i].Add(1)
}

func (g *Gateway) model(name string) *modelSeries {
	if name != "" && name != "other" && len(name) <= maxModelLen && utf8.ValidString(name) {
		if s := get(&g.mu, &g.models, name, MaxModels); s != nil {
			return s
		}
	}
	g.usedOther.Store(true)
	return &g.other
}

// get devolve a série de k, criando-a na primeira vez; com limit > 0 e o mapa
// já com limit séries, devolve nil.
func get[K comparable, V any](mu *sync.RWMutex, m *map[K]*V, k K, limit int) *V {
	mu.RLock()
	v := (*m)[k]
	mu.RUnlock()
	if v != nil {
		return v
	}
	mu.Lock()
	defer mu.Unlock()
	if v = (*m)[k]; v == nil && (limit == 0 || len(*m) < limit) {
		if *m == nil {
			*m = make(map[K]*V)
		}
		v = new(V)
		(*m)[k] = v
	}
	return v
}

// WriteText escreve as métricas no formato texto do Prometheus (0.0.4), as
// famílias na ordem do contrato e as séries sem ordem definida. Toda família
// sai com HELP e TYPE mesmo sem série, para a lista de nomes não depender do
// tráfego. Cada contador é lido atomicamente, mas não todos no mesmo instante:
// com tráfego, um _sum pode estar algumas observações à frente do _count.
func (g *Gateway) WriteText(w io.Writer) error {
	var b bytes.Buffer
	models := map[string]*modelSeries{}
	g.mu.RLock() // só durante a formatação em memória; quem cria série nova (raro) espera
	n := family(&b, "flow_gateway_http_requests_total", "counter", "Requisições HTTP atendidas, por rota, método e status.")
	for k, v := range g.codes {
		fmt.Fprintf(&b, "%s{%s} %d\n", n, labels("route", k.route, "method", k.method, "code", strconv.Itoa(k.code)), v.Load())
	}
	n = family(&b, "flow_gateway_http_request_duration_seconds", "histogram", "Duração das requisições HTTP, por rota e método.")
	for k, v := range g.routes {
		histogram(&b, n, labels("route", k.route, "method", k.method), v.Snapshot())
	}
	maps.Copy(models, g.models)
	g.mu.RUnlock()
	if g.usedOther.Load() {
		models["other"] = &g.other
	}

	n = family(&b, "flow_gateway_http_requests_in_flight", "gauge", "Requisições HTTP em andamento.")
	fmt.Fprintf(&b, "%s %d\n", n, g.inFlight.Load())
	n = family(&b, "flow_gateway_llm_ttft_seconds", "histogram", "Tempo até o primeiro token, por modelo.")
	for m, s := range models {
		histogram(&b, n, labels("model", m), s.ttft.Snapshot())
	}
	n = family(&b, "flow_gateway_llm_requests_total", "counter", "Chamadas ao LLM concluídas, por modelo e resultado.")
	for m, s := range models {
		for i, o := range outcomes {
			fmt.Fprintf(&b, "%s{%s} %d\n", n, labels("model", m, "outcome", o), s.requests[i].Load())
		}
	}
	n = family(&b, "flow_gateway_llm_tokens_total", "counter", "Tokens do LLM, por modelo e sentido.")
	for m, s := range models {
		for i, d := range directions {
			fmt.Fprintf(&b, "%s{%s} %d\n", n, labels("model", m, "direction", d), s.tokens[i].Load())
		}
	}
	n = family(&b, "flow_gateway_cache_lookups_total", "counter", "Consultas ao semantic-cache, por resultado.")
	for i, r := range results {
		fmt.Fprintf(&b, "%s{%s} %d\n", n, labels("result", r), g.cache[i].Load())
	}
	_, err := b.WriteTo(w)
	return err
}

func family(b *bytes.Buffer, name, typ, help string) string {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	return name
}

// histogram escreve os buckets cumulativos nos limites do contrato; o que
// passa de 30 s só entra no +Inf, que é o _count.
func histogram(b *bytes.Buffer, name, lbl string, s Snapshot) {
	var cum uint64
	i := 0
	for _, le := range promBuckets {
		for ; i < len(bounds) && bounds[i] <= le; i++ {
			cum += s.Buckets[i]
		}
		fmt.Fprintf(b, "%s_bucket{%s,le=\"%s\"} %d\n", name, lbl, strconv.FormatFloat(le.Seconds(), 'g', -1, 64), cum)
	}
	sum := strconv.FormatFloat(float64(s.SumMicros)/1e6, 'g', -1, 64)
	fmt.Fprintf(b, "%s_bucket{%s,le=\"+Inf\"} %d\n%s_sum{%s} %s\n%s_count{%s} %d\n", name, lbl, s.Count, name, lbl, sum, name, lbl, s.Count)
}

// escape faz as três trocas que o formato texto exige no valor de um label.
var escape = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func labels(kv ...string) string {
	pairs := make([]string, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		pairs = append(pairs, kv[i]+`="`+escape.Replace(kv[i+1])+`"`)
	}
	return strings.Join(pairs, ",")
}
