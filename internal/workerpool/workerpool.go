// Package workerpool implementa o contrato pool.WorkerPool (pkg/pool) com fila
// limitada e backpressure: a submissão nunca espera além do que o chamador
// aceitou, toda tarefa aceita é executada, panic em tarefa não derruba o
// worker e o encerramento espera o que já foi aceito.
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/pool"
)

var _ pool.WorkerPool = (*Pool)(nil)

// errTimeout é pool.ErrTaskTimeout e pool.ErrQueueFull ao mesmo tempo: quem
// trata "fila cheia" de forma única (503, descarte) não precisa saber qual
// variante de Submit foi usada.
var errTimeout = fmt.Errorf("%w: %w", pool.ErrTaskTimeout, pool.ErrQueueFull)

// Pool é um worker pool de tamanho fixo com fila limitada.
type Pool struct {
	log     *slog.Logger
	tasks   chan pool.Task
	ctx     context.Context    // entregue às tarefas
	cancel  context.CancelFunc // chamado quando o prazo do Shutdown estoura
	running atomic.Int32
	wg      sync.WaitGroup
	drained chan struct{} // fecha quando o último worker sai

	// mu protege closed e o envio em tasks: quem envia segura RLock e o
	// Shutdown só fecha o canal com Lock, então nunca há envio em canal fechado.
	mu       sync.RWMutex
	closed   bool
	quit     chan struct{} // fecha no início do Shutdown e acorda quem espera vaga
	stopOnce sync.Once
}

// New sobe cfg.Workers workers com fila de cfg.QueueCapacity tarefas. Valores
// <= 0 caem nos mesmos padrões do pool.SimplePool (4 workers, fila de 64), para
// que a mesma pool.Config dimensione as duas implementações do mesmo jeito.
// log recebe os panics das tarefas; nil usa slog.Default().
func New(cfg pool.Config, log *slog.Logger) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 64
	}
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		log:     log,
		tasks:   make(chan pool.Task, cfg.QueueCapacity),
		ctx:     ctx,
		cancel:  cancel,
		drained: make(chan struct{}),
		quit:    make(chan struct{}),
	}
	p.wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go p.work()
	}
	go func() {
		p.wg.Wait()
		close(p.drained)
	}()
	return p
}

// work consome a fila até ela ser fechada e esvaziada pelo Shutdown.
func (p *Pool) work() {
	defer p.wg.Done()
	for task := range p.tasks {
		p.run(task)
	}
}

// run executa uma tarefa isolando panic: ele é logado com a stack e o worker
// segue para a próxima tarefa, em vez de morrer e levar o processo junto.
func (p *Pool) run(task pool.Task) {
	p.running.Add(1)
	defer p.running.Add(-1)
	defer func() {
		if rec := recover(); rec != nil {
			p.log.Error("panic em tarefa do worker pool", "panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
		}
	}()
	task(p.ctx)
}

// Submit enfileira sem esperar: com a fila cheia devolve pool.ErrQueueFull na
// hora, e cabe ao chamador descartar ou responder 503.
func (p *Pool) Submit(task pool.Task) error {
	return p.submit(context.Background(), task, false)
}

// SubmitWithTimeout espera vaga na fila por até timeout (<= 0 não espera). Sem
// vaga, devolve um erro que é pool.ErrTaskTimeout e pool.ErrQueueFull.
func (p *Pool) SubmitWithTimeout(task pool.Task, timeout time.Duration) error {
	if timeout <= 0 {
		if err := p.Submit(task); !errors.Is(err, pool.ErrQueueFull) {
			return err
		}
		return errTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := p.SubmitContext(ctx, task)
	if errors.Is(err, context.DeadlineExceeded) {
		return errTimeout
	}
	return err
}

// SubmitContext espera vaga na fila enquanto ctx durar — com o contexto da
// requisição, o cliente que desiste libera a espera. ctx limita só a espera:
// a tarefa recebe o contexto do pool. ctx já encerrado devolve ctx.Err() sem
// enfileirar; encerrado durante a espera, um erro que é pool.ErrQueueFull e
// ctx.Err().
func (p *Pool) SubmitContext(ctx context.Context, task pool.Task) error {
	return p.submit(ctx, task, true)
}

// submit tenta a vaga na hora e, com wait, espera por ela enquanto ctx durar.
func (p *Pool) submit(ctx context.Context, task pool.Task, wait bool) error {
	if task == nil {
		return pool.ErrNilTask
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return pool.ErrPoolClosed
	}
	if !wait {
		select {
		case p.tasks <- task:
			return nil
		default:
			return pool.ErrQueueFull
		}
	}
	select {
	case p.tasks <- task:
		return nil
	case <-p.quit:
		return pool.ErrPoolClosed
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", pool.ErrQueueFull, ctx.Err())
	}
}

// RunningWorkers devolve quantos workers estão executando uma tarefa agora.
func (p *Pool) RunningWorkers() int {
	return int(p.running.Load())
}

// Shutdown fecha a entrada — novas submissões e quem esperava vaga recebem
// pool.ErrPoolClosed — e espera os workers executarem tudo o que já foi
// aceito, inclusive o que estava na fila. Se ctx acabar antes, cancela o
// contexto das tarefas e devolve ctx.Err() sem esperar as que ignoram o
// cancelamento; as que ainda estavam na fila são chamadas mesmo assim, com o
// contexto já cancelado, para que nenhuma tarefa aceita suma sem ser vista.
// Pode ser chamado de novo (devolve nil depois que tudo terminou), mas não de
// dentro de uma tarefa: ela esperaria por si mesma.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.stopOnce.Do(func() {
		close(p.quit) // solta quem segura RLock esperando vaga
		p.mu.Lock()
		p.closed = true
		close(p.tasks)
		p.mu.Unlock()
	})
	select {
	case <-p.drained:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		return ctx.Err()
	}
}

// Stop implementa pool.WorkerPool: Shutdown sem prazo.
func (p *Pool) Stop() {
	_ = p.Shutdown(context.Background())
}
