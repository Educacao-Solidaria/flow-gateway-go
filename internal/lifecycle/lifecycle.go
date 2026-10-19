// Package lifecycle supervisiona as goroutines de fundo do gateway (worker
// pool, limpezas periódicas, consumidores): cada uma nasce com um nome e com o
// contexto do supervisor, falha e panic são logados com esse nome, e o
// encerramento cancela todas e espera até um prazo, dizendo quem continua viva.
//
// O contexto do supervisor não deriva do sinal de encerramento, de propósito:
// enquanto server.Run drena as requisições em curso, elas ainda dependem das
// goroutines de fundo. A ordem no main é server.Run voltar (HTTP drenado) e só
// então Supervisor.Shutdown, com o próprio prazo. O sinal continua sendo
// tratado só no main e o drain HTTP só no server: nada aqui duplica os dois.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
)

// ErrClosed é devolvido por Go depois que o Shutdown começou.
var ErrClosed = errors.New("lifecycle: supervisor encerrado")

// Supervisor rastreia goroutines nomeadas e coordena o encerramento delas.
type Supervisor struct {
	log    *slog.Logger
	ctx    context.Context // entregue às goroutines; cancelado no Shutdown
	cancel context.CancelFunc
	done   chan struct{} // fecha quando alive esvazia depois do Shutdown

	// mu protege closed e alive. Com closed verdadeiro nada entra em alive, então
	// quem o esvazia (a última goroutine, ou o Shutdown se já estava vazio)
	// fecha done uma vez só.
	mu     sync.Mutex
	closed bool
	nextID uint64
	alive  map[uint64]string
}

// New cria um supervisor vazio; log recebe falhas e panics (nil usa
// slog.Default()).
func New(log *slog.Logger) *Supervisor {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Supervisor{log: log, ctx: ctx, cancel: cancel, done: make(chan struct{}), alive: map[uint64]string{}}
}

// Go roda fn numa goroutine rastreada com o nome name. O ctx de fn é cancelado
// no Shutdown, e fn deve voltar logo depois disso. Erro devolvido por fn
// (exceto context.Canceled durante o Shutdown) e panic são logados com o nome;
// a goroutine não é reiniciada. Depois do Shutdown, devolve ErrClosed sem
// rodar fn.
func (s *Supervisor) Go(name string, fn func(ctx context.Context) error) error {
	if fn == nil {
		return fmt.Errorf("lifecycle: goroutine %q sem função", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.nextID++
	id := s.nextID
	s.alive[id] = name
	go s.run(id, name, fn)
	return nil
}

func (s *Supervisor) run(id uint64, name string, fn func(context.Context) error) {
	defer s.exit(id)
	defer func() {
		if rec := recover(); rec != nil {
			s.log.Error("panic em goroutine supervisionada", "goroutine", name, "panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
		}
	}()
	err := fn(s.ctx)
	if err == nil || (errors.Is(err, context.Canceled) && s.ctx.Err() != nil) {
		return // terminou, ou devolveu o próprio cancelamento do Shutdown
	}
	s.log.Error("goroutine supervisionada falhou", "goroutine", name, "err", err)
}

// exit tira a goroutine do rastreamento (também em panic ou runtime.Goexit).
func (s *Supervisor) exit(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.alive, id)
	if s.closed && len(s.alive) == 0 {
		close(s.done)
	}
}

// Active devolve, em ordem alfabética, os nomes das goroutines ainda vivas
// (um nome aparece uma vez por goroutine).
func (s *Supervisor) Active() []string {
	s.mu.Lock()
	names := make([]string, 0, len(s.alive))
	for _, n := range s.alive {
		names = append(names, n)
	}
	s.mu.Unlock()
	sort.Strings(names)
	return names
}

// Shutdown recusa novas goroutines, cancela o contexto de todas e espera elas
// voltarem até ctx acabar. Estourado o prazo, devolve um erro (que envolve
// ctx.Err()) com as que continuam vivas: Go não tem como matar goroutine, então
// cabe ao chamador logar e sair do processo. Pode ser chamado de novo para
// esperar mais; de dentro de uma goroutine supervisionada, só volta pelo prazo,
// porque ela mesma conta como viva.
func (s *Supervisor) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if len(s.alive) == 0 {
			close(s.done)
		}
	}
	s.mu.Unlock()
	s.cancel()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		alive := s.Active()
		if len(alive) == 0 {
			return nil // a última voltou junto com o prazo
		}
		return fmt.Errorf("lifecycle: %d goroutine(s) viva(s) no fim do prazo [%s]: %w",
			len(alive), strings.Join(alive, ", "), ctx.Err())
	}
}
