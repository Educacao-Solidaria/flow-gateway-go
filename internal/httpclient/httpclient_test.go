package httpclient

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
)

var base = config.OpenRouterConfig{DialTimeout: 5 * time.Second, TLSHandshakeTimeout: 5 * time.Second,
	ResponseHeaderTimeout: 5 * time.Second, IdleConnTimeout: time.Minute, TCPKeepAlive: 30 * time.Second, MaxIdleConnsPerHost: 3} // o padrão do Go é 2

func recv(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(10 * time.Second): // só evita travar a suíte
		t.Fatal("nada chegou em 10s")
	}
}

func TestReusesConnectionsUpToThePoolSize(t *testing.T) {
	var conns atomic.Int32
	inside, release := make(chan struct{}, 4), make(chan struct{}, 4)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/segura" { // fica em voo até o teste liberar
			inside <- struct{}{}
			<-release
		}
		_, _ = io.WriteString(w, "ok")
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	c := New(base)
	get := func(path string) {
		resp, err := c.Get(srv.URL + path)
		if assert.NoError(t, err) {
			_, _ = io.Copy(io.Discard, resp.Body) // até o EOF: só então a conexão volta ao pool
			assert.NoError(t, resp.Body.Close())
		}
	}
	burst := func() { // quatro em voo ao mesmo tempo pedem quatro conexões
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				get("/segura")
			}()
		}
		for i := 0; i < 4; i++ {
			recv(t, inside)
		}
		for i := 0; i < 4; i++ {
			release <- struct{}{}
		}
		wg.Wait()
	}

	for i := 0; i < 10; i++ {
		get("/")
	}
	assert.EqualValues(t, 1, conns.Load(), "chamadas em sequência usam a mesma conexão")
	burst()
	assert.EqualValues(t, 4, conns.Load(), "a ociosa serve uma; as outras três são novas")
	burst()
	assert.EqualValues(t, 5, conns.Load(), "só MaxIdleConnsPerHost (3) ficaram no pool para a segunda rajada")
}

func TestHeaderTimeoutWithoutATotalTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // nunca manda os cabeçalhos
	}))
	defer srv.Close()
	cfg := base
	cfg.ResponseHeaderTimeout = 20 * time.Millisecond
	c := New(cfg)
	assert.Zero(t, c.Timeout, "um prazo total cortaria o stream SSE longo; a duração é do ctx")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // só evita travar a suíte
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := c.Do(req)
	if err == nil {
		resp.Body.Close()
	}
	var ne net.Error
	require.ErrorAs(t, err, &ne)
	assert.True(t, ne.Timeout())
	assert.ErrorContains(t, err, "awaiting response headers", "o prazo é o da fase, não o do ctx")
}
