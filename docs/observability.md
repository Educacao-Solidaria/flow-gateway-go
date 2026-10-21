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
