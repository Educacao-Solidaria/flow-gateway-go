// Package ringbuffer guarda os últimos N eventos de um stream, numerados em
// sequência, para reenviar a quem reconecta: o número do evento é o id do SSE,
// e o Last-Event-ID que o cliente manda ao reconectar diz de onde continuar.
package ringbuffer

import (
	"fmt"
	"sync"
)

// Event é um evento guardado e o seu número de sequência (1, 2, 3...).
type Event[T any] struct {
	Seq   uint64
	Value T
}

// Ring é um buffer circular de capacidade fixa, seguro para uso concorrente:
// cheio, Push sobrescreve o mais antigo. Leituras devolvem cópias, mas a
// cópia é rasa — com T ponteiro ou slice, o conteúdo é compartilhado, então
// guarde valores que ninguém mais altera (ex.: o evento já serializado, nunca
// bytes de um buffer que volta para o pkg/buffer.Pool).
type Ring[T any] struct {
	mu   sync.RWMutex
	buf  []T
	last uint64 // Seq do evento mais novo; 0 = vazio
}

// New cria o buffer com espaço para capacity eventos (precisa ser >= 1).
func New[T any](capacity int) (*Ring[T], error) {
	if capacity < 1 {
		return nil, fmt.Errorf("ringbuffer: capacidade %d; precisa ser >= 1", capacity)
	}
	return &Ring[T]{buf: make([]T, capacity)}, nil
}

// Push guarda v e devolve o número de sequência dele.
func (r *Ring[T]) Push(v T) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last++
	r.buf[r.slot(r.last)] = v
	return r.last
}

// Since devolve, do mais antigo para o mais novo, uma cópia dos eventos com
// Seq maior que after — com after = Last-Event-ID, o que o cliente não viu.
// complete é falso quando parte do que veio depois de after já foi
// sobrescrita (o cliente ficou para trás demais; vêm os que restam) ou quando
// after é maior que o último Seq (id que não é deste stream, como o de antes
// de um restart; não vem nada). Nos dois casos, o chamador deve tratar como
// perda e ressincronizar o cliente.
func (r *Ring[T]) Since(after uint64) (events []Event[T], complete bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if after > r.last {
		return nil, false
	}
	oldest := uint64(1)
	if n := uint64(len(r.buf)); r.last > n {
		oldest = r.last - n + 1
	}
	from := max(after+1, oldest)
	events = make([]Event[T], 0, r.last+1-from)
	for seq := from; seq <= r.last; seq++ {
		events = append(events, Event[T]{Seq: seq, Value: r.buf[r.slot(seq)]})
	}
	return events, after+1 >= oldest
}

// Snapshot devolve uma cópia de todos os eventos guardados, do mais antigo
// para o mais novo.
func (r *Ring[T]) Snapshot() []Event[T] {
	events, _ := r.Since(0)
	return events
}

// slot é a posição de seq em buf; requer r.mu.
func (r *Ring[T]) slot(seq uint64) uint64 {
	return (seq - 1) % uint64(len(r.buf))
}
