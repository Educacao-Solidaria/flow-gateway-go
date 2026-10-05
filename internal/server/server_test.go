package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
)

// syncBuffer evita data race entre o servidor escrevendo log e o teste lendo.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newServer(t *testing.T, shutdown time.Duration) (*Server, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	cfg := config.ServerConfig{Addr: "127.0.0.1:0", ReadTimeout: time.Second, ShutdownTimeout: shutdown}
	log, err := logger.New(logs, "info", "json")
	if err != nil {
		t.Fatal(err)
	}
	return New(cfg, log), logs
}

func TestHealthz(t *testing.T) {
	s, _ := newServer(t, time.Second)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK || rec.Body.String() != `{"status":"ok"}` {
		t.Fatalf("resposta inesperada: %d %s", rec.Code, rec.Body)
	}
	if len(rec.Header().Get(TraceHeader)) != 32 {
		t.Errorf("header %s ausente ou fora do formato: %q", TraceHeader, rec.Header().Get(TraceHeader))
	}
}

func TestRequestLogCarriesTraceID(t *testing.T) {
	s, logs := newServer(t, time.Second)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nao-existe", nil))

	id := rec.Header().Get(TraceHeader)
	out := logs.String()
	if !strings.Contains(out, `"trace_id":"`+id+`"`) || !strings.Contains(out, `"status":404`) {
		t.Errorf("log da requisição sem trace_id/status: %s", out)
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

func TestRecovererAfterResponseStarted(t *testing.T) {
	s, logs := newServer(t, time.Second)
	s.http.Handler.(*chi.Mux).Get("/panic-tarde", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "parcial")
		panic("boom")
	})
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/panic-tarde", nil))

	if rec.Code != http.StatusOK || rec.Body.String() != "parcial" {
		t.Errorf("recoverer escreveu depois da resposta começar: %d %q", rec.Code, rec.Body)
	}
	if out := logs.String(); !strings.Contains(out, "boom") {
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
