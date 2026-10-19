package ratelimit

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/middleware"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil"
)

// newTestLimiter troca o relógio por um contador que o teste avança.
func newTestLimiter(t *testing.T, cfg Config) (*Limiter, *atomic.Int64) {
	t.Helper()
	l, err := New(cfg)
	require.NoError(t, err)
	clock := new(atomic.Int64)
	l.now = clock.Load
	return l, clock
}

func allow(l *Limiter, key string) bool {
	ok, _ := l.Allow(key)
	return ok
}

func TestNewValidates(t *testing.T) {
	for _, cfg := range []Config{
		{Rate: 0, Burst: 1, MaxKeys: 1},
		{Rate: -1, Burst: 1, MaxKeys: 1},
		{Rate: math.NaN(), Burst: 1, MaxKeys: 1},
		{Rate: math.Inf(1), Burst: 1, MaxKeys: 1},
		{Rate: 2e9, Burst: 1, MaxKeys: 1},   // menos de 1ns por requisição
		{Rate: 1e-12, Burst: 1, MaxKeys: 1}, // intervalo estoura int64
		{Rate: 1, Burst: 0, MaxKeys: 1},
		{Rate: 1, Burst: 1, MaxKeys: 0},
	} {
		_, err := New(cfg)
		assert.Error(t, err, "%+v", cfg)
	}
	_, err := New(Config{Rate: 10, Burst: 3, MaxKeys: 100})
	assert.NoError(t, err)
}

func TestAllowBurstAndRefill(t *testing.T) {
	l, clock := newTestLimiter(t, Config{Rate: 10, Burst: 3, MaxKeys: 10}) // recarga de 1 a cada 100ms
	for i := 0; i < 3; i++ {
		require.True(t, allow(l, "a"), "rajada %d", i)
	}
	ok, wait := l.Allow("a")
	assert.False(t, ok)
	assert.Equal(t, 100*time.Millisecond, wait)
	assert.True(t, allow(l, "b"), "cada chave tem o próprio balde")

	clock.Add(int64(50 * time.Millisecond))
	ok, wait = l.Allow("a")
	assert.False(t, ok)
	assert.Equal(t, 50*time.Millisecond, wait)
	clock.Add(int64(50 * time.Millisecond))
	assert.True(t, allow(l, "a"), "recarregou uma")
	assert.False(t, allow(l, "a"))

	clock.Add(int64(10 * time.Second)) // ocioso por muito tempo: enche só até o Burst
	for i := 0; i < 3; i++ {
		require.True(t, allow(l, "a"))
	}
	assert.False(t, allow(l, "a"), "não acumula além do Burst")
}

// Com o relógio parado, passa exatamente o Burst, por mais goroutines que
// disputem a mesma chave: o CAS não deixa duas consumirem a mesma vaga.
func TestAllowConcurrentSameKey(t *testing.T) {
	l, _ := newTestLimiter(t, Config{Rate: 1, Burst: 50, MaxKeys: 10})
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if allow(l, "k") {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 50, allowed.Load())
}

func TestMaxKeysAndSweep(t *testing.T) {
	l, clock := newTestLimiter(t, Config{Rate: 10, Burst: 1, MaxKeys: 2})
	require.True(t, allow(l, "a"))
	require.True(t, allow(l, "b"))
	ok, wait := l.Allow("c")
	assert.False(t, ok, "teto atingido e nada ocioso: chave nova é recusada")
	assert.Equal(t, sweepEvery, wait)
	assert.Equal(t, 2, l.Len())

	clock.Store(int64(sweepEvery) - 1) // a e b já cheios, mas a limpeza ainda não pode rodar
	assert.False(t, allow(l, "c"), "no máximo uma limpeza por sweepEvery")
	clock.Store(int64(sweepEvery))
	assert.True(t, allow(l, "c"), "a e b ociosos foram limpos e liberaram vaga")
	assert.Equal(t, 1, l.Len())
	assert.True(t, allow(l, "a"), "balde recriado cheio: a limpeza não mudou a decisão")
}

func TestDeadBucketIsReplaced(t *testing.T) {
	l, _ := newTestLimiter(t, Config{Rate: 10, Burst: 1, MaxKeys: 10})
	require.True(t, allow(l, "a"))
	// Estado de quem pegou o balde no mapa logo antes de a limpeza marcá-lo:
	// marcado e descontado, ainda não removido.
	v, _ := l.buckets.Load("a")
	v.(*bucket).tat.Store(dead)
	l.keys.Add(-1)
	assert.True(t, allow(l, "a"), "balde morto é trocado por um novo")
	assert.Equal(t, 1, l.Len())
}

func TestByClientIP(t *testing.T) {
	trusted, err := middleware.ParseTrustedProxies([]string{"10.0.0.0/8"})
	require.NoError(t, err)
	for _, c := range []struct{ remote, xff, want string }{
		{"203.0.113.9:4000", "", "ip:203.0.113.9"},
		{"203.0.113.9:4000", "198.51.100.1", "ip:203.0.113.9"}, // peer não confiável: cabeçalho ignorado
		{"10.0.0.2:4000", "198.51.100.7", "ip:198.51.100.7"},   // via proxy confiável
		{"[2001:db8:1:2:aaaa::1]:4000", "", "ip:2001:db8:1:2::/64"},
		{"[2001:db8:1:2:bbbb::9]:4000", "", "ip:2001:db8:1:2::/64"}, // mesma /64, mesmo balde
		{"[2001:db8:1:3::1]:4000", "", "ip:2001:db8:1:3::/64"},
	} {
		var got string
		var ok bool
		h := middleware.RealIP(trusted)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got, ok = ByClientIP(r) }))
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = c.remote
		if c.xff != "" {
			req.Header.Set("X-Forwarded-For", c.xff)
		}
		h.ServeHTTP(httptest.NewRecorder(), req)
		assert.True(t, ok, c.remote)
		assert.Equal(t, c.want, got, c.remote)
	}
	_, ok := ByClientIP(httptest.NewRequest(http.MethodGet, "/", nil))
	assert.False(t, ok, "sem RealIP na cadeia não há IP confiável")
}

func TestByAPIKey(t *testing.T) {
	const secret = "sk-or-v1-segredo-de-teste"
	key := ByAPIKey(func(r *http.Request) (string, bool) {
		k := r.Header.Get("X-Test-Key")
		return k, k != ""
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Test-Key", secret)
	k1, ok := key(req)
	require.True(t, ok)
	assert.True(t, strings.HasPrefix(k1, "key:"))
	assert.Len(t, k1, len("key:")+32)
	assert.NotContains(t, k1, secret)
	k2, _ := key(req)
	assert.Equal(t, k1, k2, "mesma API key, mesma chave")
	req.Header.Set("X-Test-Key", secret+"2")
	k3, _ := key(req)
	assert.NotEqual(t, k1, k3)

	req.Header.Del("X-Test-Key")
	_, ok = key(req)
	assert.False(t, ok)
	_, ok = ByAPIKey(func(*http.Request) (string, bool) { return "", true })(req)
	assert.False(t, ok, "API key vazia não vira chave")
}

func TestMiddleware(t *testing.T) {
	l, _ := newTestLimiter(t, Config{Rate: 10, Burst: 1, MaxKeys: 10})
	ok204 := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h := middleware.RealIP(nil)(Middleware(l, ByClientIP)(ok204))
	req := func() *http.Request {
		r := testutil.NewRequest(t, http.MethodPost, "/v1/chat/completions", nil)
		r.RemoteAddr = "203.0.113.9:4000"
		return r
	}
	assert.Equal(t, http.StatusNoContent, testutil.Serve(h, req()).Code)
	rec := testutil.Serve(h, req())
	assert.Equal(t, "1", rec.Header().Get("Retry-After"), "100ms arredonda para cima, em segundos")
	testutil.AssertJSON(t, rec, http.StatusTooManyRequests, `{"code":"RATE_LIMIT_EXCEEDED",
		"message":"taxa de requisicoes excedida","http_status":429,"jsonrpc_code":-32005,
		"details":{"retry_after_seconds":1}}`)

	unkeyed := Middleware(l, func(*http.Request) (string, bool) { return "", false })(ok204)
	for i := 0; i < 3; i++ {
		assert.Equal(t, http.StatusNoContent, testutil.Serve(unkeyed, req()).Code, "sem chave, passa")
	}
}
