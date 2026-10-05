package server

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/benchutil"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/health"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
)

// BenchmarkRouter mede o custo fixo que toda requisição paga: IP real,
// request id, log estruturado, recover e roteamento do chi.
func BenchmarkRouter(b *testing.B) {
	log, err := logger.New(io.Discard, "info", "json")
	if err != nil {
		b.Fatal(err)
	}
	cfg := config.ServerConfig{Addr: "127.0.0.1:0", HealthTimeout: time.Second, TrustedProxies: []string{"10.0.0.0/8"}}
	probe := health.NewProbe("noop", func(context.Context) error { return nil })
	s, err := New(cfg, log, probe)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("livez", func(b *testing.B) { benchutil.Handler(b, s.http.Handler, http.MethodGet, "/livez", nil) })
	b.Run("healthz-1-probe", func(b *testing.B) { benchutil.Handler(b, s.http.Handler, http.MethodGet, "/healthz", nil) })
}
