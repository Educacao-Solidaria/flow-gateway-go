package middleware

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
)

func mustTrusted(t *testing.T, list ...string) TrustedProxies {
	t.Helper()
	tp, err := ParseTrustedProxies(list)
	if err != nil {
		t.Fatal(err)
	}
	return tp
}

func TestParseTrustedProxies(t *testing.T) {
	tp := mustTrusted(t, "10.0.0.0/8", " 192.168.1.7 ", "2001:db8::/32")
	if len(tp) != 3 || tp[1].String() != "192.168.1.7/32" {
		t.Fatalf("parse inesperado: %v", tp)
	}
	if _, err := ParseTrustedProxies([]string{"10.0.0.0/8", "proxy.local"}); err == nil {
		t.Fatal("esperava erro para entrada que não é IP nem CIDR")
	}
}

func TestRealIP(t *testing.T) {
	trusted := []string{"10.0.0.0/8"}
	cases := []struct {
		name    string
		trusted []string
		remote  string
		xff     []string
		xRealIP string
		want    string
	}{
		{"sem proxy confiável ignora cabeçalhos", nil, "203.0.113.9:5000", []string{"1.1.1.1"}, "2.2.2.2", "203.0.113.9"},
		{"peer não confiável ignora cabeçalhos", trusted, "203.0.113.9:5000", []string{"1.1.1.1"}, "", "203.0.113.9"},
		{"proxy confiável: último salto não confiável", trusted, "10.0.0.1:5000", []string{"6.6.6.6, 1.1.1.1, 10.0.0.2"}, "", "1.1.1.1"},
		{"várias linhas de X-Forwarded-For", trusted, "10.0.0.1:5000", []string{"6.6.6.6", "1.1.1.1"}, "", "1.1.1.1"},
		{"X-Real-IP sem X-Forwarded-For", trusted, "10.0.0.1:5000", nil, "1.1.1.1", "1.1.1.1"},
		{"X-Forwarded-For vence X-Real-IP", trusted, "10.0.0.1:5000", []string{"1.1.1.1"}, "2.2.2.2", "1.1.1.1"},
		{"salto ilegível para no proxy", trusted, "10.0.0.1:5000", []string{"lixo, 10.0.0.3"}, "", "10.0.0.3"},
		{"todos confiáveis: o mais à esquerda", trusted, "10.0.0.1:5000", []string{"10.0.0.4, 10.0.0.3"}, "", "10.0.0.4"},
		{"IPv4 mapeado em IPv6", trusted, "[::ffff:10.0.0.1]:5000", []string{"1.1.1.1"}, "", "1.1.1.1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got string
			h := RealIP(mustTrusted(t, c.trusted...))(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				got = ClientIP(r.Context()).String()
			}))
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = c.remote
			for _, v := range c.xff {
				r.Header.Add("X-Forwarded-For", v)
			}
			if c.xRealIP != "" {
				r.Header.Set("X-Real-IP", c.xRealIP)
			}
			h.ServeHTTP(httptest.NewRecorder(), r)
			if got != c.want {
				t.Errorf("ClientIP = %s, want %s", got, c.want)
			}
		})
	}
}

func TestRequestID(t *testing.T) {
	cases := []struct {
		name    string
		remote  string
		inbound string
		keep    bool
	}{
		{"sem cabeçalho gera novo", "10.0.0.1:1", "", false},
		{"proxy confiável repassa o id", "10.0.0.1:1", "lb-abc.123:x_Y", true},
		{"cliente direto não escolhe o id", "203.0.113.9:1", "forjado", false},
		{"id com espaço é descartado", "10.0.0.1:1", "a b", false},
		{"id longo demais é descartado", "10.0.0.1:1", strings.Repeat("a", maxRequestIDLen+1), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var ctxID string
			h := RequestID(mustTrusted(t, "10.0.0.0/8"))(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				ctxID = logger.TraceID(r.Context())
			}))
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = c.remote
			if c.inbound != "" {
				r.Header.Set(RequestIDHeader, c.inbound)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)

			got := rec.Header().Get(RequestIDHeader)
			if got != ctxID {
				t.Errorf("cabeçalho %q diverge do trace_id do contexto %q", got, ctxID)
			}
			if c.keep && got != c.inbound {
				t.Errorf("id = %q, want %q", got, c.inbound)
			}
			if !c.keep && len(got) != 32 {
				t.Errorf("esperava id gerado de 32 hex, veio %q", got)
			}
		})
	}
}

func TestRecovererAfterResponseStarted(t *testing.T) {
	var logs bytes.Buffer
	h := Recoverer(slog.New(slog.NewJSONHandler(&logs, nil)))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "parcial")
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK || rec.Body.String() != "parcial" {
		t.Errorf("recoverer escreveu depois da resposta começar: %d %q", rec.Code, rec.Body)
	}
	if !strings.Contains(logs.String(), "boom") {
		t.Errorf("panic não registrado no log: %s", logs.String())
	}
}

func TestRecovererRepanicsAbortHandler(t *testing.T) {
	h := Recoverer(slog.New(slog.NewJSONHandler(io.Discard, nil)))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer func() {
		if rec := recover(); rec != http.ErrAbortHandler { //nolint:errorlint // o valor do panic é o próprio sentinel
			t.Errorf("esperava repanic com ErrAbortHandler, veio %v", rec)
		}
	}()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}
