package gatewaytest

import (
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil"
)

// A Route fica atrás do servidor de verdade: o miss passa pelo upstream e vai
// para o cache, o prefixo de hit sai do cache, o /metrics conta a rota pelo
// template. Falha na subida (até na última etapa, escutar no endereço do
// gateway) desfaz o que já subiu, e o Close não deixa goroutine.
func TestStartAndClose(t *testing.T) {
	before := runtime.NumGoroutine()
	log, _ := testutil.NewLogger(t)
	cfg := &config.Config{Server: config.ServerConfig{Addr: "127.0.0.1:99999", ReadTimeout: 5 * time.Second,
		ShutdownTimeout: 5 * time.Second, HealthTimeout: time.Second, MetricsUsername: "k6", MetricsPassword: "segredo"}}
	_, err := Start(cfg, Fakes{Chunks: 1}, log) // a porta passa no server.New e só falha no Listen
	require.Error(t, err)
	cfg.Server.Addr = "127.0.0.1:0"
	for _, f := range []Fakes{{}, {Chunks: 1, TTFT: -1}, {Chunks: 1, Gap: -1}} {
		_, err := Start(cfg, f, log)
		require.ErrorContains(t, err, "Chunks >= 1", "%+v", f)
	}
	st, err := Start(cfg, Fakes{Chunks: 3, Gap: time.Millisecond}, log)
	require.NoError(t, err)
	hc := &http.Client{Transport: &http.Transport{}}
	get := func(method, path, body string) (string, http.Header) {
		req, err := http.NewRequest(method, st.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		req.SetBasicAuth("k6", "segredo")
		resp, err := hc.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return string(raw), resp.Header
	}
	chat := func(prompt string) string {
		raw, h := get(http.MethodPost, "/v1/chat/completions", `{"model":"m","messages":[{"role":"user","content":"`+prompt+`"}]}`)
		assert.NotEmpty(t, h.Get("X-Request-Id"), "passa pelos middlewares do servidor")
		return raw
	}

	miss := chat("oi")
	assert.Contains(t, miss, "id: 3\nevent: chunk\n"+`data: {"id":"gen-load","model":"m","delta_content":"pedaço 2 ","finish_reason":"stop"`)
	assert.Contains(t, miss, `"source":"upstream"`)
	assert.Contains(t, chat(HitPrefix+" oi"), `"source":"cache"`)
	for deadline := time.Now().Add(10 * time.Second); st.Writer.Stats().Stored != 1; time.Sleep(time.Millisecond) {
		require.True(t, time.Now().Before(deadline), "o miss não foi gravado: %+v", st.Writer.Stats())
	}
	metrics, _ := get(http.MethodGet, "/metrics", "")
	assert.Contains(t, metrics, `flow_gateway_http_requests_total{route="/v1/chat/completions",method="POST",code="200"} 2`)

	hc.CloseIdleConnections()
	require.NoError(t, st.Close())
	if runtime.GOOS != "windows" { // lá o AF_UNIX às vezes não entrega ao fake o fechamento da conexão (CI do PR 73)
		testutil.NoGoroutineLeak(t, before)
	}
}
