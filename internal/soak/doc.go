// Package soak é o teste de estabilidade contínua (soak) do gateway: a
// composição da rota /v1/chat/completions sobre os fakes do
// internal/testutil/gatewaytest (a mesma do e2e e da carga do k6), de pé por
// SOAK_DURATION sob carga constante, à procura de vazamento lento. Só tem testes.
//
// A carga: 16 conexões fazendo pedidos sem pausa, com prompt único por pedido;
// a cada 10 pedidos, 2 saem do cache, 5 vão ao upstream até o fim, 1 desiste
// no meio do stream e 2 terminam no evento de erro (503 em toda tentativa, com
// a repetição da resiliência, e o upstream que cai depois do 1º pedaço). A cada
// amostra (SOAK_DURATION/500, entre 1 s e 30 s) o /metrics é raspado, como
// faria o Prometheus, e então a carga para: com o cliente sem pedido em voo
// (o gateway ainda pode estar encerrando um cancelado), dois runtime.GC() (o 2º
// esvazia os sync.Pool) e o runtime/metrics dão o heap vivo
// (/gc/heap/live:bytes) e as goroutines.
//
// O teste falha se algum critério não valer:
//   - heap: a reta de mínimos quadrados do heap vivo depois do aquecimento (o
//     primeiro quinto da rodada) sobe no máximo max(1 MiB, 5% da média) da
//     primeira à última amostra. A tolerância não cresce com a duração: quanto
//     mais longa a rodada, menor o vazamento por pedido que ela pega;
//   - goroutines: com a carga parada e as conexões ociosas fechadas, voltam em
//     até 10 s à base medida antes da carga (senão o log mostra as pilhas);
//   - respostas: no máximo 0,01% dos pedidos sem a resposta esperada;
//   - mistura: no máximo 55% dos pedidos gravados no cache (só os 5 de 10 que
//     vão até o fim gravam): acima disso, a desistência ou o erro não
//     interrompeu o stream e a rodada não exercitou esses caminhos.
//
// Rodar: SOAK_DURATION=3m go test -run '^TestSoak$' -v ./internal/soak/ (sem
// SOAK_DURATION o teste é pulado; o -timeout tem de passar da duração em mais
// de 1 min). Com SOAK_PROFILE_DIR, o perfil do heap do fim (heap.pb.gz) fica
// lá. No CI, o workflow soak roda 3 min em todo PR e, sob demanda
// (workflow_dispatch), a duração pedida: 4 h por padrão, dentro do teto de 6 h
// do job hospedado. O relatório vai para o GITHUB_STEP_SUMMARY e, se a rodada
// falhar, o perfil do heap (quando chegou a ser gravado) vira artefato.
package soak
