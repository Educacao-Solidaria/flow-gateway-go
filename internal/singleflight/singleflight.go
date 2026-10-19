// Package singleflight junta chamadas idênticas em voo: enquanto a primeira
// não volta, as seguintes com a mesma chave esperam o mesmo resultado em vez
// de repetir o trabalho — no gateway, a mesma requisição paga ao OpenRouter,
// feita uma vez só.
//
// Diferente do golang.org/x/sync/singleflight, a chamada roda numa goroutine
// própria, com um contexto que não pertence a nenhum chamador: quem desiste
// (cliente que caiu, prazo estourado) para de esperar sem cancelar a chamada
// dos outros, e ela só é cancelada quando o último desiste. Panic na chamada
// vira erro para quem espera, em vez de derrubar o processo.
package singleflight

import (
	"context"
	"fmt"
	"sync"
)

// Group deduplica chamadas por chave. O valor zero está pronto para uso.
type Group[V any] struct {
	mu    sync.Mutex
	calls map[string]*call[V]
}

type call[V any] struct {
	done    chan struct{} // fecha quando val e err estão prontos
	val     V
	err     error
	waiters int                // quantos ainda esperam; sob Group.mu
	cancel  context.CancelFunc // cancela o contexto da chamada
}

// Do devolve o resultado de fn para key, executando fn uma vez para todos os
// chamadores simultâneos da mesma chave; terminada a chamada, a próxima com a
// mesma chave executa fn de novo (não é cache). fn recebe os valores do
// contexto do primeiro chamador (trace id), mas não o cancelamento de nenhum:
// o contexto dela só é cancelado quando todos os que esperam desistem. Se ctx
// acabar antes do resultado, Do devolve ctx.Err() e a chamada segue para os
// outros. O valor devolvido é o mesmo para todos: trate-o como somente leitura.
func (g *Group[V]) Do(ctx context.Context, key string, fn func(ctx context.Context) (V, error)) (V, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	g.mu.Lock()
	if g.calls == nil {
		g.calls = map[string]*call[V]{}
	}
	c, ok := g.calls[key]
	if !ok {
		callCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		c = &call[V]{done: make(chan struct{}), cancel: cancel}
		g.calls[key] = c
		go g.run(callCtx, key, c, fn)
	}
	c.waiters++
	g.mu.Unlock()

	select {
	case <-c.done:
		return c.val, c.err
	case <-ctx.Done():
		g.leave(key, c)
		return zero, ctx.Err()
	}
}

// leave tira um chamador da espera. O último cancela a chamada e a tira da
// tabela, para que um pedido novo comece do zero em vez de pegar uma chamada
// já cancelada.
func (g *Group[V]) leave(key string, c *call[V]) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c.waiters--
	if c.waiters == 0 {
		c.cancel()
		g.forget(key, c)
	}
}

// forget tira c da tabela se ela ainda for a chamada de key; requer g.mu.
func (g *Group[V]) forget(key string, c *call[V]) {
	if g.calls[key] == c {
		delete(g.calls, key)
	}
}

func (g *Group[V]) run(ctx context.Context, key string, c *call[V], fn func(context.Context) (V, error)) {
	defer func() {
		if rec := recover(); rec != nil {
			c.err = fmt.Errorf("singleflight: panic na chamada: %v", rec)
		}
		c.cancel()
		g.mu.Lock()
		g.forget(key, c)
		g.mu.Unlock()
		close(c.done)
	}()
	c.val, c.err = fn(ctx)
}
