package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/health"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/middleware"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil"
)

func newServer(t *testing.T, shutdown time.Duration) (*Server, *testutil.LogBuffer) {
	t.Helper()
	cfg := config.ServerConfig{Addr: "127.0.0.1:0", ReadTimeout: time.Second, ShutdownTimeout: shutdown, HealthTimeout: time.Second}
	log, logs := testutil.NewLogger(t)
	s, err := New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	return s, logs
}

func TestNewRejectsInvalidTrustedProxy(t *testing.T) {
	cfg := config.ServerConfig{Addr: "127.0.0.1:0", TrustedProxies: []string{"não-é-ip"}}
	if _, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("esperava erro para proxy confiável inválido")
	}
}

func TestHealthz(t *testing.T) {
	s, _ := newServer(t, time.Second)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Body.String(), `{"status":"ok","uptime_seconds":`) {
		t.Fatalf("resposta inesperada: %d %s", rec.Code, rec.Body)
	}
	if len(rec.Header().Get(middleware.RequestIDHeader)) != 32 {
		t.Errorf("header %s ausente ou fora do formato: %q", middleware.RequestIDHeader, rec.Header().Get(middleware.RequestIDHeader))
	}
}

func TestHealthzUsesProbes(t *testing.T) {
	cfg := config.ServerConfig{Addr: "127.0.0.1:0", ReadTimeout: time.Second, ShutdownTimeout: time.Second, HealthTimeout: time.Second}
	down := health.NewProbe("cache", func(context.Context) error { return errors.New("fora do ar") })
	s, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), down)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"cache":{"status":"fail"`) {
		t.Fatalf("resposta inesperada: %d %s", rec.Code, rec.Body)
	}
}

func TestLivez(t *testing.T) {
	s, _ := newServer(t, time.Second)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("resposta inesperada: %d %s", rec.Code, rec.Body)
	}
}

func TestRequestLogCarriesTraceID(t *testing.T) {
	s, logs := newServer(t, time.Second)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nao-existe", nil))

	id := rec.Header().Get(middleware.RequestIDHeader)
	out := logs.String()
	if !strings.Contains(out, `"trace_id":"`+id+`"`) || !strings.Contains(out, `"status":404`) ||
		!strings.Contains(out, `"client_ip":"192.0.2.1"`) {
		t.Errorf("log da requisição sem trace_id/status/client_ip: %s", out)
	}
}

func TestRecoverer(t *testing.T) {
	s, logs := newServer(t, time.Second)
	s.http.Handler.(*chi.Mux).Get("/panic", func(http.ResponseWriter, *http.Request) { panic("boom") })
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/panic", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if out := logs.String(); !strings.Contains(out, "boom") || !strings.Contains(out, `"status":500`) {
		t.Errorf("panic não registrado no log: %s", out)
	}
}

// serveSlow sobe o servidor com uma rota que só responde quando release fecha
// e devolve o canal com o resultado de Serve.
func serveSlow(t *testing.T, s *Server, release <-chan struct{}) (cancel context.CancelFunc, addr string, done <-chan error) {
	t.Helper()
	started := make(chan struct{})
	s.http.Handler.(*chi.Mux).Get("/slow", func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		_, _ = io.WriteString(w, "fim")
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve(ctx, ln) }()
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String() + "/slow")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-started
	return cancel, ln.Addr().String(), errCh
}

func TestGracefulShutdownWaitsInFlight(t *testing.T) {
	s, _ := newServer(t, 2*time.Second)
	release := make(chan struct{})
	cancel, addr, done := serveSlow(t, s, release)

	cancel()
	select {
	case err := <-done:
		t.Fatalf("Serve retornou antes da requisição em curso terminar: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
		t.Error("servidor ainda aceita conexões durante o shutdown")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

func TestGracefulShutdownTimeout(t *testing.T) {
	s, _ := newServer(t, 50*time.Millisecond)
	release := make(chan struct{})
	defer close(release)
	cancel, _, done := serveSlow(t, s, release)

	cancel()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("esperava estouro do prazo, veio %v", err)
	}
}

func TestShutdownSignalsLongHandlers(t *testing.T) {
	s, _ := newServer(t, 5*time.Second)
	s.http.Handler.(*chi.Mux).Get("/sse", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "data: oi\n\n")
		w.(http.Flusher).Flush()
		<-ShuttingDown(r.Context())
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ln) }()

	resp, err := http.Get("http://" + ln.Addr().String() + "/sse")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown esperou o handler longo em vez de sinalizá-lo")
	}
}
