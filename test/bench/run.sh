#!/usr/bin/env bash
# Benchmarks do código atual (o PR) contra BASE na mesma máquina: compila os dois
# lados uma vez (go test -c por pacote com benchmark) e alterna as rodadas,
# trocando a ordem a cada volta, para o ruído que muda com o tempo cair nos dois.
# O benchstat compara e o portão (main.go) dá o veredito.
#   BASE=origin/main COUNT=10 BENCHTIME=500ms LIMIT=10 test/bench/run.sh
#   BENCH=Observe test/bench/run.sh    (só os benchmarks que casam com o padrão)
# Precisa de go, git e benchstat. O veredito sai em Markdown (também no
# GITHUB_STEP_SUMMARY) e o progresso na saída de erro. Sai 0 sem piora (ou com a
# piora aceita por ACCEPT=true); com piora ou erro, sai diferente de 0.
set -euo pipefail
export LC_ALL=C
cd "$(dirname "$0")/../.."
base=${BASE:-origin/main} count=${COUNT:-10} benchtime=${BENCHTIME:-500ms}
command -v benchstat >/dev/null || { echo "falta o benchstat (golang.org/x/perf/cmd/benchstat)" >&2; exit 2; }
# com menos de 4 amostras por lado o teste do benchstat nunca chega a p ≤ 0,05: o portão não reprovaria nada
[ "$count" -ge 4 ] || { echo "COUNT=$count: use pelo menos 4 rodadas" >&2; exit 2; }
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
mkdir "$out/src" "$out/base" "$out/pr"
touch "$out/base.txt" "$out/pr.txt" "$out/warn.md"
# o que a base deixou de medir vai também para o resumo: o portão compara menos
warn() { echo "aviso: $*" >&2; echo "- $*" >>"$out/warn.md"; }
git archive "$base" | tar -x -C "$out/src"
go build -o "$out/gate" ./test/bench

# Pacotes com benchmark em qualquer dos lados; um binário de teste por lado. Com
# -trimpath, o binário de um pacote que o PR não toca sai idêntico ao da base.
pkgs=$(for t in . "$out/src"; do (cd "$t" && grep -rl --include='*_test.go' '^func Benchmark' . || true); done |
  xargs -r -n1 dirname | sort -u)
for d in $pkgs; do
  if [ -d "$d" ]; then go test -c -trimpath -o "$out/pr/${d//\//_}.test" "$d"; fi
  if [ -d "$out/src/$d" ]; then
    (cd "$out/src" && go test -c -trimpath -o "$out/base/${d//\//_}.test" "$d") || warn "$d não compila na base"
  fi
done >&2

# bench <lado> <pacote>: uma rodada, anexada a <lado>.txt. Falha no PR encerra;
# na base só avisa e a rodada fica de fora (o PR é que está em julgamento).
bench() {
  local bin=$out/$1/${2//\//_}.test dir=$2
  [ "$1" = pr ] || dir=$out/src/$2
  [ -x "$bin" ] || return 0
  if (cd "$dir" && "$bin" -test.run '^$' -test.bench "${BENCH:-.}" -test.benchmem -test.count 1 \
    -test.benchtime "$benchtime" -test.timeout 10m) >"$out/last.txt" 2>&1; then
    cat "$out/last.txt" >>"$out/$1.txt"
    return
  fi
  cat "$out/last.txt" >&2
  [ "$1" = base ] || exit 2
  warn "os benchmarks de $2 falharam na base; a volta ficou de fora"
}
for i in $(seq "$count"); do
  sides="base pr"
  [ $((i % 2)) -eq 1 ] || sides="pr base"
  for d in $pkgs; do for s in $sides; do bench "$s" "$d"; done; done
  echo "rodada $i de $count" >&2
done

rc=0
benchstat base="$out/base.txt" PR="$out/pr.txt" >"$out/table.txt"
benchstat -format csv base="$out/base.txt" PR="$out/pr.txt" 2>/dev/null |
  "$out/gate" -limit "${LIMIT:-10}" -accept="${ACCEPT:-false}" >"$out/verdict.md" || rc=$?
{
  cat "$out/verdict.md"
  if [ -s "$out/warn.md" ]; then printf '\n**Avisos:**\n'; sort -u "$out/warn.md"; fi
  printf '\n<details><summary>benchstat: %s rodadas de %s por lado, base %s</summary>\n\n```\n' \
    "$count" "$benchtime" "$(git rev-parse --short "$base")"
  cat "$out/table.txt"
  printf '```\n\n</details>\n'
} | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
exit "$rc"
