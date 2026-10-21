# Observabilidade do flow-gateway

A parte do gateway nas convenções da Fase 4, fixas entre os repos: os nomes,
labels e buckets abaixo são os que os dashboards, os alertas e os testes usam.

## Métricas (Prometheus)

Formato texto 0.0.4, escrito por `internal/metrics.Gateway.WriteText`. O
`server.New` cria o `Gateway` e monta o middleware HTTP; `Server.Metrics()` o
entrega a quem monta a rota de LLM.

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
