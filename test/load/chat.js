// Carga do k6 contra o gateway sobre fakes (alvo: main.go; o run.sh sobe tudo
// e mede a CPU). VUS usuários num laço de POST /v1/chat/completions sem pausa:
// até VUS streams em voo ao mesmo tempo. A fração HIT_RATIO sai do cache (o
// fake responde hit ao prefixo "hit:"); o resto passa pelo RAG, pelo upstream e
// pelo cachewriter. Um VU à parte lê do /metrics as requisições em voo, a prova
// de que a concorrência chegou ao gateway.
// Variáveis: BASE_URL, VUS, RAMP, HOLD, HIT_RATIO, METRICS_AUTH ("usuário:senha").
import http from 'k6/http';
import encoding from 'k6/encoding';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const VUS = parseInt(__ENV.VUS || '200', 10);
const RAMP = __ENV.RAMP || '10s';
const HOLD = __ENV.HOLD || '30s';
const HIT_RATIO = parseFloat(__ENV.HIT_RATIO || '0.2');
const AUTH = `Basic ${encoding.b64encode(__ENV.METRICS_AUTH || '')}`;

// flow_gateway_http_requests_in_flight a cada segundo, sem a própria raspagem.
const inFlight = new Trend('gateway_in_flight');

const stages = (target) => [{ duration: RAMP, target }, { duration: HOLD, target }];

export const options = {
  scenarios: {
    chat: { executor: 'ramping-vus', exec: 'chat', startVUs: 0, stages: stages(VUS) },
    probe: { executor: 'ramping-vus', exec: 'probe', startVUs: 1, stages: stages(1) },
  },
  thresholds: {
    'http_req_failed{scenario:chat}': ['rate<0.01'],
    'checks{scenario:chat}': ['rate>0.99'],
    'checks{scenario:probe}': ['rate==1'],
    // TTFB = o 1º pedaço (o sse.Writer manda o cabeçalho com ele), o TTFT visto
    // pelo cliente; 0,8 s é o SLA do contrato. O stream do fake padrão dura ~10 s:
    // com 4 s, 5.000 VUs davam 1.200 req/s e saturavam os 4 vCPU do runner do CI.
    'http_req_waiting{cache:miss}': ['p(95)<800'],
    'http_req_duration{cache:miss}': ['p(95)<12000'],
    'http_req_duration{cache:hit}': ['p(95)<500'],
    'http_reqs{scenario:chat}': ['count>0'],
    gateway_in_flight: [`max>=${Math.floor(VUS * 0.9)}`],
  },
};

export function chat() {
  const hit = Math.random() < HIT_RATIO;
  const prompt = hit ? 'hit: pergunta frequente' : `pergunta ${__VU}-${__ITER}`;
  const res = http.post(`${BASE}/v1/chat/completions`,
    JSON.stringify({ model: 'm', messages: [{ role: 'user', content: prompt }] }),
    { headers: { 'Content-Type': 'application/json' }, tags: { cache: hit ? 'hit' : 'miss' }, timeout: '30s' });
  check(res, {
    'status 200': (r) => r.status === 200,
    'stream fechou em done, da origem certa': (r) => typeof r.body === 'string' && r.body.includes('event: done')
      && r.body.includes(hit ? '"source":"cache"' : '"source":"upstream"'),
  });
}

export function probe() {
  const res = http.get(`${BASE}/metrics`, { headers: { Authorization: AUTH } });
  const m = /^flow_gateway_http_requests_in_flight (\d+)$/m.exec(typeof res.body === 'string' ? res.body : '');
  if (check(res, { '/metrics com o gauge de em voo': (r) => r.status === 200 && m !== null })) {
    inFlight.add(parseInt(m[1], 10) - 1);
  }
  sleep(1);
}
