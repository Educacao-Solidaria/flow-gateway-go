package pool_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/pool"
)

func TestSimplePool_ConcurrentExecution(t *testing.T) {
	p := pool.NewSimplePool(pool.Config{
		Workers:       4,
		QueueCapacity: 50,
	})
	defer p.Stop()

	var counter int64
	var wg sync.WaitGroup
	totalTasks := 30

	for i := 0; i < totalTasks; i++ {
		wg.Add(1)
		err := p.Submit(func(ctx context.Context) {
			defer wg.Done()
			atomic.AddInt64(&counter, 1)
			time.Sleep(2 * time.Millisecond)
		})
		if err != nil {
			t.Fatalf("falha ao submeter tarefa %d: %v", i, err)
		}
	}

	wg.Wait()
	if atomic.LoadInt64(&counter) != int64(totalTasks) {
		t.Fatalf("esperava %d tarefas executadas, obteve %d", totalTasks, atomic.LoadInt64(&counter))
	}
}

func TestSimplePool_QueueFullAndTimeout(t *testing.T) {
	// Pool com 1 worker e fila de tamanho 1
	p := pool.NewSimplePool(pool.Config{
		Workers:       1,
		QueueCapacity: 1,
	})
	defer p.Stop()

	blockerStarted := make(chan struct{})
	unblock := make(chan struct{})

	// Tarefa 1: ocupa o worker
	_ = p.Submit(func(ctx context.Context) {
		close(blockerStarted)
		<-unblock
	})

	<-blockerStarted

	// Tarefa 2: ocupa o buffer da fila
	err := p.Submit(func(ctx context.Context) {})
	if err != nil {
		t.Fatalf("tarefa 2 deveria caber na fila: %v", err)
	}

	// Tarefa 3: deve falhar imediatamente com ErrQueueFull
	err = p.Submit(func(ctx context.Context) {})
	if err != pool.ErrQueueFull {
		t.Fatalf("esperava ErrQueueFull, obteve %v", err)
	}

	// Tarefa 4 com timeout curto: deve falhar com ErrTaskTimeout
	err = p.SubmitWithTimeout(func(ctx context.Context) {}, 10*time.Millisecond)
	if err != pool.ErrTaskTimeout {
		t.Fatalf("esperava ErrTaskTimeout, obteve %v", err)
	}

	close(unblock)
}

func TestSimplePool_NilTaskAndStop(t *testing.T) {
	p := pool.NewSimplePool(pool.Config{Workers: 2, QueueCapacity: 10})

	if err := p.Submit(nil); err != pool.ErrNilTask {
		t.Fatalf("esperava ErrNilTask, obteve %v", err)
	}

	p.Stop()

	// Tentativa apos stop
	err := p.Submit(func(ctx context.Context) {})
	if err != pool.ErrPoolClosed {
		t.Fatalf("esperava ErrPoolClosed, obteve %v", err)
	}
}
