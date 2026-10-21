#!/usr/bin/env bash
# Sobe o alvo (main.go; argumentos extras vão para ele), roda o chat.js e mede
# pelo /proc (Linux) a CPU e o pico de memória do alvo, gateway e fakes juntos.
#   VUS=200 RAMP=10s HOLD=30s test/load/run.sh     (precisa de go, k6, curl e jq)
# Sai com o código do k6 (99 = threshold), ou 1 com a CPU por requisição acima de
# CPU_MS_PER_REQ_MAX ou o alvo sem encerrar limpo. Números na saída e no GITHUB_STEP_SUMMARY.
set -euo pipefail
export LC_ALL=C
cd "$(dirname "$0")/../.."
out=$(mktemp -d) pid=
trap 'kill $pid 2>/dev/null || true; rm -rf "$out"' EXIT
go build -o "$out/target" ./test/load
export BASE_URL="http://127.0.0.1:${PORT:-8080}" FLOW_SERVER_METRICS_USERNAME=k6
FLOW_SERVER_METRICS_PASSWORD=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
export FLOW_SERVER_METRICS_PASSWORD METRICS_AUTH="k6:$FLOW_SERVER_METRICS_PASSWORD"
"$out/target" --server.addr "${BASE_URL#http://}" "$@" >"$out/target.log" 2>&1 &
pid=$!
for _ in $(seq 100); do curl -fs "$BASE_URL/livez" >/dev/null && break; sleep 0.1; done
curl -fs "$BASE_URL/livez" >/dev/null || { cat "$out/target.log" >&2; exit 1; }

cpu() { awk '{print $14 + $15}' "/proc/$pid/stat" 2>/dev/null || echo 0; } # utime + stime, em ticks
t0=$(cpu) s0=$SECONDS rc=0
k6 run --quiet --summary-export "$out/summary.json" --summary-trend-stats 'avg,med,p(95),p(99),max' \
  test/load/chat.js || rc=$?
ticks=$(($(cpu) - t0)) secs=$((SECONDS - s0)) hwm=$(awk '/^VmHWM/ {print int($2 / 1024)}' "/proc/$pid/status" 2>/dev/null || echo 0)
kill -TERM "$pid" 2>/dev/null || true
wait "$pid" || { tail -20 "$out/target.log" >&2; [ "$rc" -ne 0 ] || rc=1; }

reqs=$(jq '.metrics["http_reqs{scenario:chat}"].count // 0' "$out/summary.json")
cpu_s=$(awk -v t="$ticks" -v hz="$(getconf CLK_TCK)" 'BEGIN {printf "%.1f", t / hz}')
ms_req=$(awk -v c="$cpu_s" -v n="$reqs" 'BEGIN {printf "%.3f", n ? c * 1000 / n : 0}')
awk -v v="$ms_req" -v max="${CPU_MS_PER_REQ_MAX:-6}" 'BEGIN {exit !(v <= max)}' || { cpu_bad=" — ESTOUROU"; [ "$rc" -ne 0 ] || rc=1; }
{
  echo "### k6: ${VUS:-200} VUs (subida ${RAMP:-10s}, ${HOLD:-30s} com todos) — saída $rc"
  echo "| medida | valor |"
  echo "|---|---|"
  jq -r '.metrics as $m | def ms(k; s): [s[] as $s | "\($m[k][$s] // 0 | round)"] | join(" / ") + " ms";
    "| requisições (cenário chat) | \($m["http_reqs{scenario:chat}"] | "\(.count) (\(.rate | round)/s)") |",
    "| em voo no gateway (máx. lido no /metrics) | \($m.gateway_in_flight.max // 0) |",
    "| miss: TTFB (1º pedaço) p50 / p95 / p99 | \(ms("http_req_waiting{cache:miss}"; ["med", "p(95)", "p(99)"])) |",
    "| miss: duração p50 / p95 / p99 | \(ms("http_req_duration{cache:miss}"; ["med", "p(95)", "p(99)"])) |",
    "| hit: duração p50 / p95 / p99 | \(ms("http_req_duration{cache:hit}"; ["med", "p(95)", "p(99)"])) |",
    "| falhas HTTP / checks ok | \($m["http_req_failed{scenario:chat}"].value * 100)% / \($m["checks{scenario:chat}"].value * 100)% |"' \
    "$out/summary.json"
  echo "| CPU do alvo | ${cpu_s} s em ${secs} s; ${ms_req} ms por requisição (teto ${CPU_MS_PER_REQ_MAX:-6})${cpu_bad:-} |"
  echo "| pico de memória do alvo (VmHWM) | ${hwm} MiB |"
} | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
exit "$rc"
