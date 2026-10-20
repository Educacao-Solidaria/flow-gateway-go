// Package cachewriter grava no semantic-cache, em segundo plano, as respostas
// que o gateway gerou, sem segurar quem as recebe: o stream passa adiante
// pedaço a pedaço enquanto o texto é acumulado, e só a resposta que terminou
// bem vira um STORE_REQ numa fila limitada (internal/workerpool). Fila cheia
// descarta e conta: o usuário nunca espera pelo cache.
//
// Uso previsto: na rota de LLM, depois do Lookup que deu miss,
//
//	chunks, errs = w.Tee(ctx, prompt, embedding, chunks, errs)
//
// e, no main, w := cachewriter.New(udscache.New(cfg.Cache.SocketPath), opts, log)
// com sup.Go("cachewriter", w.Run), para o Shutdown do supervisor drenar a fila.
package cachewriter

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/workerpool"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/pool"
)

// Options dimensiona o Writer; zero (ou negativo) vale o padrão de cada campo.
type Options struct {
	Workers, QueueCapacity int           // fila do workerpool: padrão 4 workers e 64 vagas
	StoreTimeout           time.Duration // prazo de cada gravação; padrão 2 s
	DrainTimeout           time.Duration // prazo do Run para drenar a fila; padrão 5 s
	TTL                    time.Duration // validade no cache, arredondada para cima em segundos; 0 = sem TTL
	MaxResponseBytes       int           // resposta maior não é gravada; padrão 1 MiB
}

// Stats conta o destino das respostas aceitas pelo Writer.
type Stats struct {
	Stored  uint64 // gravadas
	Failed  uint64 // o cache recusou ou não respondeu no prazo
	Dropped uint64 // fila cheia ou Writer encerrado: descartadas sem tentar
}

// Writer grava respostas no cache em segundo plano; seguro para uso concorrente.
type Writer struct {
	cache                   cache.Client
	pool                    *workerpool.Pool
	log                     *slog.Logger
	storeTimeout, drain     time.Duration
	ttl, maxBytes           int
	stored, failed, dropped atomic.Uint64
	failing                 atomic.Bool // loga só a primeira falha de uma sequência
}

// New sobe os workers da fila de gravação em c; log nil usa slog.Default().
func New(c cache.Client, o Options, log *slog.Logger) *Writer {
	if log == nil {
		log = slog.Default()
	}
	def := func(d, padrao time.Duration) time.Duration {
		if d <= 0 {
			return padrao
		}
		return d
	}
	w := &Writer{cache: c, log: log, storeTimeout: def(o.StoreTimeout, 2*time.Second),
		drain: def(o.DrainTimeout, 5*time.Second), maxBytes: o.MaxResponseBytes}
	if o.TTL > 0 {
		w.ttl = int(math.Ceil(o.TTL.Seconds())) // 1,5 s vira 2 e 1 ms vira 1, nunca 0 (= sem TTL)
	}
	if w.maxBytes <= 0 {
		w.maxBytes = 1 << 20
	}
	w.pool = workerpool.New(pool.Config{Workers: o.Workers, QueueCapacity: o.QueueCapacity}, log)
	return w
}

// Store enfileira a gravação de uma resposta inteira sem esperar (o caminho
// sem stream). Devolve false se ela não vai ser gravada: vazia, acima de
// MaxResponseBytes, fila cheia ou Writer encerrado (os dois últimos contam em
// Dropped). Quem chama só passa resposta que terminou bem.
func (w *Writer) Store(prompt, response string, embedding []float32) bool {
	return w.submit(prompt, response, slices.Clone(embedding))
}

func (w *Writer) submit(prompt, response string, emb []float32) bool {
	if response == "" || len(response) > w.maxBytes {
		return false
	}
	if err := w.pool.Submit(func(ctx context.Context) { w.store(ctx, prompt, response, emb) }); err != nil {
		w.dropped.Add(1)
		return false
	}
	return true
}

func (w *Writer) store(ctx context.Context, prompt, response string, emb []float32) {
	ctx, cancel := context.WithTimeout(ctx, w.storeTimeout)
	defer cancel()
	if err := w.cache.Store(ctx, prompt, response, emb, w.ttl); err != nil {
		w.failed.Add(1)
		if !w.failing.Swap(true) { // o erro do cliente não carrega prompt nem resposta
			w.log.Warn("cachewriter: gravação no semantic-cache falhou; as falhas seguintes só contam", "err", err)
		}
		return
	}
	w.stored.Add(1)
	if w.failing.Swap(false) {
		w.log.Info("cachewriter: gravação no semantic-cache voltou")
	}
}

// Tee repassa o stream (in, inErr) sem mudar nada e, se ele termina bem,
// grava a resposta inteira com Store. Termina bem = inErr fecha sem erro e o
// último pedaço é o único com finish_reason, que é "stop": erro, cancelamento,
// resposta cortada (length, content_filter...) ou com mais de uma escolha não
// vão para o cache. O contrato é o do stream de openrouter.Client: in fecha
// quando ctx acaba, e quem lê vai até o fim ou cancela ctx. O embedding é
// copiado na chamada.
func (w *Writer) Tee(ctx context.Context, prompt string, embedding []float32, in <-chan domain.StreamChunk, inErr <-chan error) (<-chan domain.StreamChunk, <-chan error) {
	out, outErr := make(chan domain.StreamChunk), make(chan error, 1)
	emb := slices.Clone(embedding)
	go func() {
		defer close(out)
		defer close(outErr)
		var text strings.Builder
		var last domain.StreamChunk
		ends, fits := 0, true
		for c := range in {
			if c.FinishReason != "" {
				ends++
			}
			if fits = fits && text.Len()+len(c.DeltaContent) <= w.maxBytes; fits {
				text.WriteString(c.DeltaContent) // o acumulado nunca passa do teto
			}
			last = c
			select {
			case out <- c:
			case <-ctx.Done():
				outErr <- ctx.Err()
				return
			}
		}
		select { // fechado é sucesso; nil (fora do contrato) espera o ctx
		case err := <-inErr:
			if err != nil {
				outErr <- err
				return
			}
		case <-ctx.Done():
			outErr <- ctx.Err()
			return
		}
		if fits && ends == 1 && last.FinishReason == "stop" {
			w.submit(prompt, text.String(), emb)
		}
	}()
	return out, outErr
}

// Stats devolve os contadores até agora.
func (w *Writer) Stats() Stats {
	return Stats{Stored: w.stored.Load(), Failed: w.failed.Load(), Dropped: w.dropped.Load()}
}

// Run é para o lifecycle.Supervisor (sup.Go("cachewriter", w.Run)): espera o
// Shutdown dele, recusa novas gravações e drena a fila por até DrainTimeout.
// Estourado o prazo, cancela as gravações restantes (contam em Failed) e
// devolve o erro, que o supervisor loga. Não fecha o cache.Client: ele é o
// mesmo do Lookup, e quem o criou o fecha depois do supervisor.
func (w *Writer) Run(ctx context.Context) error {
	<-ctx.Done()
	dctx, cancel := context.WithTimeout(context.Background(), w.drain)
	defer cancel()
	if err := w.pool.Shutdown(dctx); err != nil {
		return fmt.Errorf("cachewriter: a fila não drenou em %s: %w", w.drain, err)
	}
	return nil
}
