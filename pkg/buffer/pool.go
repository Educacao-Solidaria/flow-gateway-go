package buffer

import (
	"bytes"
	"sync"
	"sync/atomic"
)

const (
	DefaultInitialCap = 4096       // 4 KB inicial
	DefaultMaxCap     = 1024 * 1024 // 1 MB teto para não reter na pool
)

// Pool gerencia a reciclagem eficiente de instâncias de bytes.Buffer com sync.Pool.
type Pool struct {
	pool       sync.Pool
	initialCap int
	maxCap     int
	gets       int64
	puts       int64
	drops      int64
}

// Config customiza os limites de capacidade da buffer pool.
type Config struct {
	InitialCapacity int
	MaxCapacity     int
}

// NewPool instancia uma nova pool de buffers reutilizáveis.
func NewPool(cfg Config) *Pool {
	initCap := cfg.InitialCapacity
	if initCap <= 0 {
		initCap = DefaultInitialCap
	}
	maxCap := cfg.MaxCapacity
	if maxCap <= 0 || maxCap < initCap {
		maxCap = DefaultMaxCap
	}

	p := &Pool{
		initialCap: initCap,
		maxCap:     maxCap,
	}

	p.pool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, p.initialCap))
		},
	}

	return p
}

// GlobalPool é a instância compartilhada default para todo o serviço.
var GlobalPool = NewPool(Config{})

// Get obtém um buffer limpo e pronto para uso da pool.
func (p *Pool) Get() *bytes.Buffer {
	atomic.AddInt64(&p.gets, 1)
	buf := p.pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// Put limpa e devolve o buffer à pool, descartando se ultrapassar a capacidade máxima.
func (p *Pool) Put(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > p.maxCap {
		atomic.AddInt64(&p.drops, 1)
		// Deixa o garbage collector coletar buffers gigantes
		return
	}

	buf.Reset()
	atomic.AddInt64(&p.puts, 1)
	p.pool.Put(buf)
}

// Stats retorna as métricas atômicas de reaproveitamento de buffers.
func (p *Pool) Stats() (gets, puts, drops int64) {
	return atomic.LoadInt64(&p.gets), atomic.LoadInt64(&p.puts), atomic.LoadInt64(&p.drops)
}

// GetBytes obtém um buffer da GlobalPool.
func GetBytes() *bytes.Buffer {
	return GlobalPool.Get()
}

// PutBytes devolve um buffer à GlobalPool.
func PutBytes(buf *bytes.Buffer) {
	GlobalPool.Put(buf)
}
