package server

import (
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/middleware"
)

// NewAdmin cria o listener administrativo do /debug/pprof em cfg.PprofAddr, ou
// devolve nil com cfg.PprofEnabled falso. É um http.Server à parte do público,
// com mux próprio: o init do net/http/pprof (que o chi/middleware já importa)
// registra as rotas no http.DefaultServeMux, e nenhum servidor do gateway serve
// o DefaultServeMux. Com a basic auth do /metrics configurada, exige a mesma.
// Saem o índice, os perfis nomeados (heap, goroutine, allocs, block, mutex,
// threadcreate), o de CPU e o trace; o cmdline (os.Args) e o expvar ficam de
// fora. Sem WriteTimeout: o perfil de CPU dura o ?seconds= pedido.
func NewAdmin(cfg config.ServerConfig, log *slog.Logger) *Server {
	if !cfg.PprofEnabled {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	var h http.Handler = mux
	if cfg.MetricsUsername != "" {
		h = middleware.BasicAuth("flow-gateway pprof", cfg.MetricsUsername, cfg.MetricsPassword)(mux)
	}
	return &Server{
		http: &http.Server{
			Addr:              cfg.PprofAddr,
			Handler:           middleware.RealIP(nil)(requestLog(log)(h)), // quem tirou perfil fica no log
			ReadHeaderTimeout: cfg.ReadTimeout,
			IdleTimeout:       2 * time.Minute, // sem ele (e sem ReadTimeout), keep-alive ocioso não expira
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
		},
		log:             log,
		shutdownTimeout: cfg.ShutdownTimeout,
		shuttingDown:    make(chan struct{}),
	}
}
