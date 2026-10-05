// Package server monta o servidor HTTP base do gateway: roteador chi,
// middlewares de IP real, request id, log e recover, e encerramento gracioso.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/health"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/middleware"
)

// Server é o servidor HTTP do gateway.
type Server struct {
	http            *http.Server
	log             *slog.Logger
	shutdownTimeout time.Duration
	shuttingDown    chan struct{}
}

type shuttingDownKey struct{}

// ShuttingDown devolve um canal que fecha quando o servidor começa a encerrar.
// Handlers longos (SSE, streaming) devem selecionar nele para fechar a
// resposta; o shutdown só espera por eles até ShutdownTimeout. Requisições
// comuns não precisam: o contexto delas não é cancelado e o shutdown as
// espera terminar. Fora de uma requisição servida por Server, devolve nil.
func ShuttingDown(ctx context.Context) <-chan struct{} {
	ch, _ := ctx.Value(shuttingDownKey{}).(<-chan struct{})
	return ch
}

// New cria o servidor com as rotas base: /livez (processo vivo) e /healthz
// (probes das dependências, ver health.Checker). Falha se cfg.TrustedProxies
// tiver entrada que não seja IP nem CIDR.
func New(cfg config.ServerConfig, log *slog.Logger, probes ...health.Probe) (*Server, error) {
	trusted, err := middleware.ParseTrustedProxies(cfg.TrustedProxies)
	if err != nil {
		return nil, err
	}
	r := chi.NewRouter()
	r.Use(middleware.RealIP(trusted), middleware.RequestID(trusted), requestLog(log), middleware.Recoverer(log))
	hc := health.New(log, cfg.HealthTimeout, probes...)
	r.Get("/livez", hc.Live)
	r.Get("/healthz", hc.Ready)

	shuttingDown := make(chan struct{})
	base := context.WithValue(context.Background(), shuttingDownKey{}, (<-chan struct{})(shuttingDown))
	return &Server{
		http: &http.Server{
			Addr:              cfg.Addr,
			Handler:           r,
			ReadHeaderTimeout: cfg.ReadTimeout,
			ReadTimeout:       cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       2 * time.Minute,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
			BaseContext:       func(net.Listener) context.Context { return base },
		},
		log:             log,
		shutdownTimeout: cfg.ShutdownTimeout,
		shuttingDown:    shuttingDown,
	}, nil
}

// Run escuta em cfg.Addr e serve até ctx ser cancelado (ver Serve).
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.http.Addr, err)
	}
	return s.Serve(ctx, ln)
}

// Serve atende em ln até ctx ser cancelado; então fecha o canal de
// ShuttingDown, para de aceitar conexões e espera as requisições em curso por
// até ShutdownTimeout. Estourado o prazo, fecha as conexões restantes e
// devolve o erro do prazo. Serve só pode ser chamado uma vez por Server.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.http.Serve(ln) }()
	s.log.Info("servidor http no ar", "addr", ln.Addr().String())

	select {
	case err := <-errCh:
		return fmt.Errorf("server: %w", err)
	case <-ctx.Done():
	}

	s.log.Info("encerrando servidor", "timeout", s.shutdownTimeout)
	close(s.shuttingDown) // avisa os handlers longos (ver ShuttingDown)
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.shutdownTimeout)
	defer cancel()
	if err := s.http.Shutdown(shutdownCtx); err != nil {
		_ = s.http.Close()
		return fmt.Errorf("server: shutdown: %w", err)
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: %w", err)
	}
	s.log.Info("servidor encerrado")
	return nil
}

func requestLog(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			status := ww.Status()
			if status == 0 { // handler não escreveu nada: o net/http responde 200
				status = http.StatusOK
			}
			log.InfoContext(r.Context(), "requisição http",
				"method", r.Method, "path", r.URL.Path, "status", status,
				"client_ip", middleware.ClientIP(r.Context()).String(),
				"bytes", ww.BytesWritten(), "duration", time.Since(start))
		})
	}
}
