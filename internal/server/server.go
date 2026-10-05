// Package server monta o servidor HTTP base do gateway: roteador chi,
// middlewares de trace, log e recover, e encerramento gracioso.
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
	"github.com/go-chi/chi/v5/middleware"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
)

// TraceHeader devolve ao cliente o trace_id da requisição.
const TraceHeader = "X-Trace-Id"

// Server é o servidor HTTP do gateway.
type Server struct {
	http            *http.Server
	log             *slog.Logger
	shutdownTimeout time.Duration
}

// New cria o servidor com as rotas base (/healthz).
func New(cfg config.ServerConfig, log *slog.Logger) *Server {
	r := chi.NewRouter()
	r.Use(traceID, requestLog(log), recoverer(log))
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	return &Server{
		http: &http.Server{
			Addr:              cfg.Addr,
			Handler:           r,
			ReadHeaderTimeout: cfg.ReadTimeout,
			ReadTimeout:       cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       2 * time.Minute,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
		},
		log:             log,
		shutdownTimeout: cfg.ShutdownTimeout,
	}
}

// Run escuta em cfg.Addr e serve até ctx ser cancelado (ver Serve).
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.http.Addr, err)
	}
	return s.Serve(ctx, ln)
}

// Serve atende em ln até ctx ser cancelado; então para de aceitar conexões e
// espera as requisições em curso por até ShutdownTimeout. Estourado o prazo,
// fecha as conexões restantes e devolve o erro do prazo.
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

func traceID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := logger.NewTraceID()
		w.Header().Set(TraceHeader, id)
		next.ServeHTTP(w, r.WithContext(logger.WithTraceID(r.Context(), id)))
	})
}

func recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
						panic(rec)
					}
					log.ErrorContext(r.Context(), "panic no handler", "panic", fmt.Sprint(rec), "path", r.URL.Path)
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func requestLog(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			status := ww.Status()
			if status == 0 { // handler não escreveu nada: o net/http responde 200
				status = http.StatusOK
			}
			log.InfoContext(r.Context(), "requisição http",
				"method", r.Method, "path", r.URL.Path, "status", status,
				"bytes", ww.BytesWritten(), "duration", time.Since(start))
		})
	}
}
