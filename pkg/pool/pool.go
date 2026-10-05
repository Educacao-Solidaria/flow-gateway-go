package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed   = errors.New("worker pool já encerrado")
	ErrQueueFull    = errors.New("fila do worker pool está cheia")
	ErrTaskTimeout  = errors.New("tempo limite para submissao de tarefa expirado")
	ErrNilTask      = errors.New("tarefa nao pode ser nula")
)

// Task define uma unidade de trabalho assíncrona executada por um worker da pool.
type Task func(ctx context.Context)

// WorkerPool especifica o contrato canônico para despacho de tarefas concorrentes.
type WorkerPool interface {
	Submit(task Task) error
	SubmitWithTimeout(task Task, timeout time.Duration) error
	RunningWorkers() int
	Stop()
}

// Config define os parâmetros de dimensionamento da pool.
type Config struct {
	Workers       int
	QueueCapacity int
}

// SimplePool implementa WorkerPool usando goroutines e canais bufferizados com backpressure.
type SimplePool struct {
	tasks          chan Task
	workers        int
	runningWorkers int32
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	closed         int32
}

// NewSimplePool instancia e inicia os workers concorrentes da pool.
func NewSimplePool(cfg Config) *SimplePool {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 64
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &SimplePool{
		tasks:   make(chan Task, cfg.QueueCapacity),
		workers: cfg.Workers,
		ctx:     ctx,
		cancel:  cancel,
	}

	p.start()
	return p
}

func (p *SimplePool) start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case <-p.ctx.Done():
					return
				case task, ok := <-p.tasks:
					if !ok {
						return
					}
					if task != nil {
						atomic.AddInt32(&p.runningWorkers, 1)
						task(p.ctx)
						atomic.AddInt32(&p.runningWorkers, -1)
					}
				}
			}
		}()
	}
}

// Submit adiciona uma tarefa à fila sem bloquear caso esteja cheia.
func (p *SimplePool) Submit(task Task) error {
	if task == nil {
		return ErrNilTask
	}
	if atomic.LoadInt32(&p.closed) == 1 {
		return ErrPoolClosed
	}

	select {
	case p.tasks <- task:
		return nil
	default:
		return ErrQueueFull
	}
}

// SubmitWithTimeout tenta enfileirar a tarefa respeitando uma janela de espera máxima.
func (p *SimplePool) SubmitWithTimeout(task Task, timeout time.Duration) error {
	if task == nil {
		return ErrNilTask
	}
	if atomic.LoadInt32(&p.closed) == 1 {
		return ErrPoolClosed
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case p.tasks <- task:
		return nil
	case <-timer.C:
		return ErrTaskTimeout
	case <-p.ctx.Done():
		return ErrPoolClosed
	}
}

// RunningWorkers retorna o número de workers ativamente processando tarefas no momento.
func (p *SimplePool) RunningWorkers() int {
	return int(atomic.LoadInt32(&p.runningWorkers))
}

// Stop encerra a pool graciosamente, aguardando os workers terminarem suas tarefas em execução.
func (p *SimplePool) Stop() {
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		p.cancel()
		close(p.tasks)
		p.wg.Wait()
	}
}
