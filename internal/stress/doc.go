// Package stress é a suíte de estresse da Fase 2: os pacotes de concorrência
// do gateway (ratelimit, workerpool, singleflight, resilience, circuitbreaker,
// ringbuffer, state, lifecycle e tracing) trabalhando juntos sob 1.000
// goroutines simultâneas, contra o mock HTTP da OpenRouter. Só tem testes.
//
// A carga é 1.000 goroutines por padrão, 100 com -short, ou a de
// STRESS_GOROUTINES, a partir de 100 (com -race, o detector aceita até 8.128
// goroutines vivas: STRESS_GOROUTINES=5000 go test -race ./internal/stress/).
package stress
