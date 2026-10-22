package gatewaytest

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/cachewriter"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/httpclient"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/raghttp"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/resilience"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/server"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/udscache"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
)

// HitPrefix marca o prompt que o semantic-cache de mentira do Start responde
// como hit; o resto é miss e vai ao upstream.
const HitPrefix = "hit:"

// hits é esse semantic-cache: o Store não guarda nada, e a memória fica
// constante (o cache.InMemoryClient cresce a cada Store e percorre tudo a cada
// Lookup). O ServeCache não chama Stats nem Close.
type hits struct{ cache.Client }

func (hits) Lookup(_ context.Context, prompt string, _ []float32, _ float32) (*cache.LookupResult, error) {
	if !strings.HasPrefix(prompt, HitPrefix) {
		return &cache.LookupResult{}, nil
	}
	return &cache.LookupResult{Hit: true, Score: 1, CachedResponse: "resposta do cache"}, nil
}

func (hits) Store(context.Context, string, string, []float32, int) error { return nil }

// Fakes regula a OpenRouter de mentira do Start (ver Stream).
type Fakes struct {
	TTFT, Gap time.Duration
	Chunks    int
}

// Stack é o gateway de pé sobre os fakes.
type Stack struct {
	URL     string              // base do gateway; a rota é POST URL+"/v1/chat/completions"
	Writer  *cachewriter.Writer // Stats: o destino das gravações no cache
	hc      *http.Client        // o do gateway para o upstream e o RAG
	closers []func() error      // o Close roda do último para o primeiro
}

// CloseIdleConnections fecha as conexões ociosas do gateway com o upstream e o
// RAG: quem conta goroutines antes e depois de uma carga compara sem o pool.
func (s *Stack) CloseIdleConnections() { s.hc.CloseIdleConnections() }

// Start sobe os fakes e, em cfg.Server.Addr, a Route atrás do server.New
// (middlewares, /metrics com a credencial de cfg.Server, encerramento), com o
// httpclient do gateway para o upstream e o RAG e o udscache para o cache.
func Start(cfg *config.Config, f Fakes, log *slog.Logger) (*Stack, error) {
	if f.Chunks < 1 || f.TTFT < 0 || f.Gap < 0 {
		return nil, errors.New("gatewaytest: Chunks >= 1, TTFT e Gap >= 0")
	}
	s := new(Stack)
	if err := s.start(cfg, f, log); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	return s, nil
}

func (s *Stack) start(cfg *config.Config, f Fakes, log *slog.Logger) error {
	dir, err := os.MkdirTemp("", "gwt") // o sun_path do macOS tem 104 bytes
	if err != nil {
		return err
	}
	s.closers = append(s.closers, func() error { return os.RemoveAll(dir) })
	sock := filepath.Join(dir, "c.sock")
	cln, err := net.Listen("unix", sock)
	if err != nil {
		return err
	}
	s.closers = append(s.closers, cln.Close)
	go ServeCache(cln, hits{})
	llm, brain := httptest.NewServer(Stream(f.TTFT, f.Gap, f.Chunks)), httptest.NewServer(BrainRouter("trecho do RAG"))
	s.closers = append(s.closers, func() error { llm.Close(); brain.Close(); return nil })
	hc := httpclient.New(cfg.OpenRouter)
	s.hc = hc
	uc := udscache.New(sock, -1, log)
	s.closers = append(s.closers, func() error { hc.CloseIdleConnections(); return uc.Close() })
	rc, err1 := raghttp.New(brain.URL, hc)
	retry, err2 := resilience.NewRetry(Upstream{llm.URL, hc}, resilience.RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond})
	llmc, err3 := resilience.New(retry, resilience.Options{AttemptTimeout: 5 * time.Second})
	srv, err4 := server.New(cfg.Server, log)
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		return err
	}
	s.Writer = cachewriter.New(uc, cachewriter.Options{}, log)
	s.closers = append(s.closers, run(s.Writer.Run))
	srv.Handle(http.MethodPost, "/v1/chat/completions", Route{Cache: uc, RAG: rc, LLM: llmc, Writer: s.Writer})
	gln, err := net.Listen("tcp", cfg.Server.Addr)
	if err != nil {
		return err
	}
	s.URL = "http://" + gln.Addr().String()
	s.closers = append(s.closers, run(func(ctx context.Context) error { return srv.Serve(ctx, gln) }))
	return nil
}

// run roda fn numa goroutine e devolve o closer que a cancela e a espera.
func run(fn func(context.Context) error) func() error {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	return func() error { cancel(); return <-done }
}

// Close encerra na ordem inversa da subida: o gateway (espera as requisições em
// curso), o cachewriter (drena a fila), os clientes e os fakes.
func (s *Stack) Close() error {
	var errs []error
	for i := len(s.closers) - 1; i >= 0; i-- {
		errs = append(errs, s.closers[i]())
	}
	s.closers = nil
	return errors.Join(errs...)
}
