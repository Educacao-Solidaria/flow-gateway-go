package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/middleware"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

// scrape devolve as amostras (série → valor) e os "# TYPE" na ordem do texto;
// falha com série repetida ou linha sem valor.
func scrape(t *testing.T, g *Gateway) (samples map[string]string, types []string) {
	t.Helper()
	var b bytes.Buffer
	require.NoError(t, g.WriteText(&b))
	samples = map[string]string{}
	for _, line := range strings.Split(strings.TrimSuffix(b.String(), "\n"), "\n") {
		if typ, ok := strings.CutPrefix(line, "# TYPE "); ok {
			types = append(types, typ)
		} else if !strings.HasPrefix(line, "# HELP ") {
			i := strings.LastIndexByte(line, ' ')
			require.Positive(t, i, line)
			require.NotContains(t, samples, line[:i], "série repetida")
			samples[line[:i]] = line[i+1:]
		}
	}
	return samples, types
}

func TestPromBucketsAreBounds(t *testing.T) {
	for _, le := range promBuckets {
		assert.Contains(t, bounds[:], le, "o bucket cumulativo só sai exato sobre um limite do Latency")
	}
}

func TestWriteTextEmpty(t *testing.T) {
	samples, types := scrape(t, new(Gateway))
	assert.Equal(t, []string{
		"flow_gateway_http_requests_total counter", "flow_gateway_http_request_duration_seconds histogram",
		"flow_gateway_http_requests_in_flight gauge", "flow_gateway_llm_ttft_seconds histogram",
		"flow_gateway_llm_requests_total counter", "flow_gateway_llm_tokens_total counter",
		"flow_gateway_cache_lookups_total counter",
	}, types, "toda família sai, mesmo sem tráfego")
	assert.Equal(t, map[string]string{
		"flow_gateway_http_requests_in_flight":             "0",
		`flow_gateway_cache_lookups_total{result="hit"}`:   "0",
		`flow_gateway_cache_lookups_total{result="miss"}`:  "0",
		`flow_gateway_cache_lookups_total{result="error"}`: "0",
	}, samples)
}

func TestMiddlewareLabels(t *testing.T) {
	g := new(Gateway)
	inside, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r := chi.NewRouter()
	r.Use(g.Middleware, middleware.Recoverer(slog.New(slog.NewTextHandler(io.Discard, nil))))
	r.Get("/v1/models/{id}", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	r.Get("/silent", func(http.ResponseWriter, *http.Request) {})
	r.Get("/boom", func(http.ResponseWriter, *http.Request) { panic("boom") })
	r.Get("/slow", func(http.ResponseWriter, *http.Request) { inside <- struct{}{}; <-release })
	serve := func(method, path string) { r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil)) }

	go func() { serve("GET", "/slow"); close(done) }()
	<-inside
	samples, _ := scrape(t, g)
	assert.Equal(t, "1", samples["flow_gateway_http_requests_in_flight"])
	close(release)
	<-done
	for _, p := range []string{"/v1/models/a", "/v1/models/b", "/silent", "/boom", "/nope"} {
		serve("GET", p)
	}
	serve("FOO", "/v1/models/a")

	samples, _ = scrape(t, g)
	assert.Equal(t, "0", samples["flow_gateway_http_requests_in_flight"])
	for series, want := range map[string]string{
		`{route="/v1/models/{id}",method="GET",code="200"}`: "2", // o template, não o path
		`{route="/silent",method="GET",code="200"}`:         "1",
		`{route="/boom",method="GET",code="500"}`:           "1",
		`{route="unmatched",method="GET",code="404"}`:       "1",
		`{route="unmatched",method="other",code="405"}`:     "1",
		`{route="/slow",method="GET",code="200"}`:           "1",
	} {
		assert.Equal(t, want, samples["flow_gateway_http_requests_total"+series], series)
	}
	h := "flow_gateway_http_request_duration_seconds"
	assert.Equal(t, "2", samples[h+`_count{route="/v1/models/{id}",method="GET"}`])
	assert.Equal(t, "2", samples[h+`_bucket{route="/v1/models/{id}",method="GET",le="+Inf"}`])
}

func TestLLMAndCache(t *testing.T) {
	g := new(Gateway)
	const m = `{model="openai/gpt-4o"`
	for _, d := range []time.Duration{300 * time.Millisecond, 900 * time.Millisecond, time.Minute} {
		g.ObserveTTFT("openai/gpt-4o", d)
	}
	for _, err := range []error{nil, nil, fmt.Errorf("x: %w", context.Canceled), context.DeadlineExceeded,
		fmt.Errorf("x: %w", gwerrors.ErrUpstreamTimeout), errors.New("boom")} {
		g.LLMRequest("openai/gpt-4o", err)
	}
	g.LLMTokens("openai/gpt-4o", 10, 32)
	g.LLMTokens("openai/gpt-4o", -5, 8)
	g.CacheLookup(true, nil)
	g.CacheLookup(false, nil)
	g.CacheLookup(false, nil)
	g.CacheLookup(true, errors.New("fora"))

	samples, _ := scrape(t, g)
	for le, want := range map[string]string{"0.25": "0", "0.5": "1", "0.8": "1", "1": "2", "30": "2", "+Inf": "3"} {
		assert.Equal(t, want, samples["flow_gateway_llm_ttft_seconds_bucket"+m+`,le="`+le+`"}`], "le=%s", le)
	}
	assert.Equal(t, "3", samples["flow_gateway_llm_ttft_seconds_count"+m+"}"])
	assert.Equal(t, "61.2", samples["flow_gateway_llm_ttft_seconds_sum"+m+"}"])
	for o, want := range map[string]string{"ok": "2", "canceled": "1", "timeout": "2", "error": "1"} {
		assert.Equal(t, want, samples["flow_gateway_llm_requests_total"+m+`,outcome="`+o+`"}`], o)
	}
	assert.Equal(t, "10", samples["flow_gateway_llm_tokens_total"+m+`,direction="input"}`], "negativo não conta")
	assert.Equal(t, "40", samples["flow_gateway_llm_tokens_total"+m+`,direction="output"}`])
	for r, want := range map[string]string{"hit": "1", "miss": "2", "error": "1"} {
		assert.Equal(t, want, samples[`flow_gateway_cache_lookups_total{result="`+r+`"}`], r)
	}
}

// TestModelCardinality roda com -race no CI: 100 nomes disputam as 50 vagas
// enquanto o texto é lido; nenhuma chamada se perde.
func TestModelCardinality(t *testing.T) {
	g := new(Gateway)
	g.LLMRequest("q\"\\\nz", nil) // escapado no texto
	const workers, each = 8, 200
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				g.LLMRequest(fmt.Sprintf("m%d", (w*each+i)%100), nil)
				if i%10 == 0 {
					assert.NoError(t, g.WriteText(io.Discard))
				}
			}
		}()
	}
	wg.Wait()
	for _, name := range []string{"", "other", strings.Repeat("x", maxModelLen+1), "\xff"} {
		g.LLMRequest(name, nil)
	}

	samples, _ := scrape(t, g)
	total, models := 0, 0
	for series, v := range samples {
		if strings.HasPrefix(series, "flow_gateway_llm_requests_total{") && strings.HasSuffix(series, `outcome="ok"}`) {
			var n int
			_, err := fmt.Sscan(v, &n)
			require.NoError(t, err)
			total, models = total+n, models+1
		}
	}
	assert.Equal(t, MaxModels+1, models, "50 modelos + other")
	assert.Equal(t, workers*each+5, total)
	assert.Equal(t, "1", samples[`flow_gateway_llm_requests_total{model="q\"\\\nz",outcome="ok"}`])
}
