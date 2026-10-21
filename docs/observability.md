# Observabilidade do flow-gateway

A parte do gateway nas convenções da Fase 4, fixas entre os repos: os nomes,
labels e buckets abaixo são os que os dashboards, os alertas e os testes usam.

## Métricas (Prometheus)

Formato texto 0.0.4, escrito por `internal/metrics.Gateway.WriteText`. O
`server.New` cria o `Gateway` e monta o middleware HTTP; `Server.Metrics()` o
entrega a quem monta a rota de LLM.

### Exposição: `GET /metrics`

No servidor principal, com basic auth. As credenciais vêm só de config ou env,
sem padrão embutido e nunca por flag: `server.metrics_username` e
`server.metrics_password` (`FLOW_SERVER_METRICS_USERNAME` e
`FLOW_SERVER_METRICS_PASSWORD`, no ambiente, no `.env` ou no YAML). Sem as duas,
a rota não existe (404); só uma, ou usuário com `:`, é erro de config.
Credencial errada ou ausente: 401 com `WWW-Authenticate: Basic realm="flow-gateway
metrics", charset="UTF-8"`. A comparação é em tempo constante e nada da
credencial vai para log (o log de acesso tem path e status, nunca o header). O
basic auth manda a senha só em base64: raspe por TLS ou pela rede interna.

```yaml
scrape_configs:
  - job_name: flow-gateway
    basic_auth:
      username: prometheus
      password_file: /etc/prometheus/flow-gateway.senha
    static_configs:
      - targets: ["flow-gateway:8080"]
```

### As métricas

| Métrica | Tipo | Labels | Quem alimenta |
|---|---|---|---|
| `flow_gateway_http_requests_total` | counter | route, method, code | `Gateway.Middleware` (toda rota do servidor) |
| `flow_gateway_http_request_duration_seconds` | histogram | route, method | idem |
| `flow_gateway_http_requests_in_flight` | gauge | — | idem |
| `flow_gateway_llm_ttft_seconds` | histogram | model | `ObserveTTFT(model, d)` no primeiro chunk |
| `flow_gateway_llm_requests_total` | counter | model, outcome | `LLMRequest(model, err)` no fim de cada chamada |
| `flow_gateway_llm_tokens_total` | counter | model, direction | `LLMTokens(model, usage.PromptTokens, usage.CompletionTokens)` |
| `flow_gateway_cache_lookups_total` | counter | result | `CacheLookup(hit, err)` depois do `cache.Client.Lookup` |

As quatro últimas ficam sem série (cache: em zero) até a rota
`/v1/chat/completions` chamar os ganchos.

Buckets de latência (s): `0.005 0.01 0.025 0.05 0.1 0.25 0.5 0.8 1 2.5 5 10 30`
e `+Inf` (o 0,8 casa com o SLA de TTFT).

### Cardinalidade

- `route`: o template do chi (`/v1/models/{id}`), nunca o path cru; sem rota
  casada, `unmatched`.
- `method`: os nove métodos do HTTP; qualquer outro, `other`.
- `model`: os primeiros 50 nomes distintos; o resto, e nome vazio, `other`
  literal, mais de 128 bytes ou UTF-8 inválido, vira `other`. Registre o nome
  validado (o que a OpenRouter respondeu), não o texto cru do cliente: lixo
  ocuparia as 50 vagas até o processo reiniciar.
- `outcome`: `ok`; `canceled` (o chamador desistiu: `context.Canceled`);
  `timeout` (prazo estourado ou `ErrUpstreamTimeout`); `error` (o resto).
- Proibidos: tenant, API key, prompt, resposta, query, request id, trace id, IP,
  user agent.

### Leitura e o que não sai

Cada contador é atômico, mas a leitura não pega todos no mesmo instante: com
tráfego, `_sum` pode estar algumas observações à frente de `_count` (dentro do
histograma os buckets são coerentes e `+Inf` é o `_count`). Não há `go_*` nem
`process_*`: viriam com o `prometheus/client_golang`, que não entrou (motivo no
PR 77).

## Dashboard (Grafana)

`deploy/grafana/flow-gateway.json`, com `uid` fixo `flow-gateway`. A fonte de
dados é a variável `DS_PROMETHEUS` (tipo datasource, Prometheus): todo painel e
toda query usam `${DS_PROMETHEUS}`, então o mesmo JSON entra pelo *Import* da
interface ou por provisionamento de arquivo, sem editar uid de fonte.

Painéis: TTFT p50/p95/p99 (com a linha do SLA de 0,8 s), requisições por
segundo por rota, taxa de cache hit, erros HTTP por código (4xx e 5xx), modelos
mais usados (top 10 no período), chamadas ao LLM por resultado, latência HTTP
p95 por rota, tokens por segundo e requisições em andamento.

O `TestDashboard` (`internal/metrics`) confere no CI que toda métrica citada nas
queries está no texto que o `Gateway` escreve (nome errado não passa), que o
`uid` é o fixo e que toda fonte é `${DS_PROMETHEUS}`.

## Profiling (pprof)

Desligado por padrão. `server.pprof_enabled=true` (`FLOW_SERVER_PPROF_ENABLED`)
sobe um listener administrativo próprio em `server.pprof_addr` (padrão
`127.0.0.1:6060`), nunca no servidor público. Fora do loopback (`0.0.0.0`,
IP da rede, nome), a config exige a basic auth do `/metrics`; com ela
configurada, o pprof pede a mesma credencial em qualquer endereço.

Rotas: `/debug/pprof/` (índice e perfis nomeados: `heap`, `goroutine`,
`allocs`, `block`, `mutex`, `threadcreate`), `/debug/pprof/profile?seconds=N`
(CPU, 30 s por padrão) e `/debug/pprof/trace?seconds=N`. Ficam de fora o
`cmdline` (os.Args) e o `/debug/vars` (expvar). O `net/http/pprof` registra as
rotas no `http.DefaultServeMux` no init (e o `chi/middleware` já o importa);
nenhum servidor do gateway serve o `DefaultServeMux`, e o `TestPublicServerHasNoPprof`
prova que o público responde 404 nelas. `block` e `mutex` só têm amostra com
`runtime.SetBlockProfileRate`/`SetMutexProfileFraction`, que o gateway não liga.

```sh
go tool pprof http://127.0.0.1:6060/debug/pprof/heap
curl -o cpu.pprof 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30'
```
