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
	if rec.Header().Get("Vary") != "Accept-Encoding" {
		t.Errorf("compressão não montada: Vary = %q", rec.Header().Get("Vary"))
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

func TestMetricsMounted(t *testing.T) {
	s, _ := newServer(t, time.Second)
	s.http.Handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/livez", nil))
	var b strings.Builder
	if err := s.Metrics().WriteText(&b); err != nil || !strings.Contains(b.String(), `flow_gateway_http_requests_total{route="/livez",method="GET",code="200"} 1`) {
		t.Fatalf("middleware de métricas não montado: %v\n%s", err, b.String())
	}
}

func TestMetricsEndpoint(t *testing.T) {
	const pass = "s3gredo-bem-longo"
	log, logs := testutil.NewLogger(t)
	s, err := New(config.ServerConfig{Addr: "127.0.0.1:0", ReadTimeout: time.Second, ShutdownTimeout: time.Second,
		HealthTimeout: time.Second, MetricsUsername: "prom", MetricsPassword: pass}, log)
	if err != nil {
		t.Fatal(err)
	}
	get := func(s *Server, user, pass string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		rec := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(rec, req)
		return rec
	}
	for _, rec := range []*httptest.ResponseRecorder{get(s, "", ""), get(s, "prom", "errada")} {
		if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
			t.Fatalf("sem credencial certa: %d %q", rec.Code, rec.Header())
		}
	}
	rec := get(s, "prom", pass)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" ||
		!strings.Contains(rec.Body.String(), `flow_gateway_http_requests_total{route="/metrics",method="GET",code="401"} 2`) {
		t.Fatalf("raspagem: %d %q\n%s", rec.Code, rec.Header(), rec.Body)
	}
	if strings.Contains(logs.String(), "s3gredo") || strings.Contains(logs.String(), "cHJvbTpz") { // base64 de "prom:s"
		t.Fatalf("credencial no log:\n%s", logs)
	}
	off, _ := newServer(t, time.Second) // sem credencial configurada
	if rec := get(off, "prom", pass); rec.Code != http.StatusNotFound {
		t.Fatalf("sem credencial configurada o /metrics deveria ser 404, veio %d", rec.Code)
	}
}

// TestPublicServerHasNoPprof prova a premissa (o init do net/http/pprof, que
// o chi/middleware importa, registrou no DefaultServeMux) e que o servidor
// público não a herda: ele serve o próprio roteador.
func TestPublicServerHasNoPprof(t *testing.T) {
	if _, pattern := http.DefaultServeMux.Handler(httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)); pattern == "" {
		t.Fatal("premissa: o pprof deveria estar no DefaultServeMux")
	}
	s, _ := newServer(t, time.Second)
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/cmdline", "/debug/vars"} {
		rec := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s no servidor público: %d", path, rec.Code)
		}
	}
}

func TestAdmin(t *testing.T) {
	log, _ := testutil.NewLogger(t)
	cfg := config.ServerConfig{ReadTimeout: time.Second, ShutdownTimeout: time.Second, PprofAddr: "127.0.0.1:0"}
	if NewAdmin(cfg, log) != nil {
		t.Fatal("sem pprof_enabled o admin não existe")
	}
	cfg.PprofEnabled = true
	open := NewAdmin(cfg, log)
	cfg.MetricsUsername, cfg.MetricsPassword = "prom", "s3gredo-bem-longo"
	locked := NewAdmin(cfg, log)
	for _, tc := range []struct {
		s    *Server
		path string
		auth bool
		want int
	}{
		{open, "/debug/pprof/", false, http.StatusOK},
		{open, "/debug/pprof/goroutine?debug=1", false, http.StatusOK},
		{open, "/debug/pprof/cmdline", false, http.StatusNotFound}, // os.Args fica de fora
		{open, "/debug/vars", false, http.StatusNotFound},
		{open, "/metrics", false, http.StatusNotFound},
		{locked, "/debug/pprof/heap", false, http.StatusUnauthorized},
		{locked, "/debug/pprof/heap", true, http.StatusOK},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		if tc.auth {
			req.SetBasicAuth("prom", "s3gredo-bem-longo")
		}
		rec := httptest.NewRecorder()
		tc.s.http.Handler.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("%s (auth %v): %d, queria %d", tc.path, tc.auth, rec.Code, tc.want)
		}
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

func TestTraceparentJoinsRequestLog(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	log, logs := testutil.NewLogger(t)
	s, err := New(config.ServerConfig{ReadTimeout: time.Second, ShutdownTimeout: time.Second, HealthTimeout: time.Second,
		TrustedProxies: []string{"192.0.2.1"}}, log) // o RemoteAddr do httptest
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	req.Header.Set("traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if id := rec.Header().Get(middleware.RequestIDHeader); id != traceID || !strings.Contains(logs.String(), `"trace_id":"`+traceID+`"`) {
		t.Errorf("trace W3C não chegou ao X-Request-Id (%q) e ao log: %s", id, logs.String())
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
