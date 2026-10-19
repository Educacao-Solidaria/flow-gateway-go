package quota

import (
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ratelimit"
)

var (
	now = time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	ms  = strconv.FormatInt(now.Add(30*time.Second).UnixMilli(), 10) // como a OpenRouter manda o Reset

	_ Holder = (*ratelimit.Limiter)(nil)
)

func header(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

func TestParse(t *testing.T) {
	in30s := now.Add(30 * time.Second)
	for _, tc := range []struct {
		name string
		h    http.Header
		body string
		want Quota
	}{
		{"nada informado", nil, "", Quota{Remaining: -1}},
		{"epoch em ms (OpenRouter)", header("X-RateLimit-Limit", "20", "X-RateLimit-Remaining", "0", "X-RateLimit-Reset", ms), "", Quota{Limit: 20, Reset: in30s}},
		{"epoch em s", header("X-RateLimit-Reset", strconv.FormatInt(in30s.Unix(), 10)), "", Quota{Remaining: -1, Reset: in30s}},
		{"segundos até o reset", header("X-RateLimit-Reset", "30"), "", Quota{Remaining: -1, Reset: in30s}},
		{"duração com unidade", header("X-RateLimit-Reset", "30s"), "", Quota{Remaining: -1, Reset: in30s}},
		{"malformados e negativos", header("X-RateLimit-Limit", "muitas", "X-RateLimit-Remaining", "-3", "X-RateLimit-Reset", "-1", "Retry-After", "logo"), "", Quota{Remaining: -1}},
		{"restante acima do limite", header("X-RateLimit-Limit", "10", "X-RateLimit-Remaining", "99"), "", Quota{Limit: 10, Remaining: 10}},
		{"Retry-After em segundos", header("Retry-After", "7"), "", Quota{Remaining: -1, RetryAfter: 7 * time.Second}},
		{"Retry-After em data HTTP", header("Retry-After", now.Add(2*time.Minute).Format(http.TimeFormat)), "", Quota{Remaining: -1, RetryAfter: 2 * time.Minute}},
		{"Retry-After no passado", header("Retry-After", now.Add(-time.Hour).Format(http.TimeFormat)), "", Quota{Remaining: -1}},
		{"Retry-After gigante", header("Retry-After", strconv.FormatInt(math.MaxInt64, 10)), "", Quota{Remaining: -1, RetryAfter: maxWait}},
		{"ecoado no JSON de erro", nil, `{"error":{"code":429,"metadata":{"headers":{"X-RateLimit-Limit":"20","X-RateLimit-Remaining":"0","X-RateLimit-Reset":"` + ms + `"}}}}`, Quota{Limit: 20, Reset: in30s}},
		{"o header vence o eco", header("X-RateLimit-Remaining", "3"), `{"error":{"metadata":{"headers":{"X-RateLimit-Remaining":"0"}}}}`, Quota{Remaining: 3}},
		{"JSON ruim é ignorado", header("X-RateLimit-Remaining", "3"), `{"error":`, Quota{Remaining: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Parse(tc.h, []byte(tc.body), now)
			assert.True(t, got.Reset.Equal(tc.want.Reset), "reset %v, queria %v", got.Reset, tc.want.Reset)
			got.Reset, tc.want.Reset = time.Time{}, time.Time{}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestWait(t *testing.T) {
	in30s := now.Add(30 * time.Second)
	for _, tc := range []struct {
		q    Quota
		want time.Duration
	}{
		{Quota{Remaining: -1}, 0},
		{Quota{Remaining: -1, Reset: in30s}, 0}, // sem saber o que resta, não segura
		{Quota{Remaining: 0, Reset: in30s}, 30 * time.Second},
		{Quota{Remaining: 5, Reset: in30s}, 0},                 // ainda há cota
		{Quota{Remaining: 0, Reset: now.Add(-time.Second)}, 0}, // a janela já recomeçou
		{Quota{Remaining: 0}, 0},                               // sem Reset, não há até quando segurar
		{Quota{Remaining: 3, RetryAfter: 5 * time.Second}, 5 * time.Second},
		{Quota{Remaining: 0, Reset: in30s, RetryAfter: time.Minute}, time.Minute}, // o maior dos dois
		{Quota{Remaining: 0, Reset: time.UnixMilli(math.MaxInt64)}, maxWait},
	} {
		assert.Equal(t, tc.want, tc.q.Wait(now), "%+v", tc.q)
	}
}

// holder anota as esperas pedidas.
type holder map[string]time.Duration

func (h holder) Hold(key string, d time.Duration) { h[key] = d }

func TestObserve(t *testing.T) {
	h := holder{}
	q := Observe(h, "openrouter:free", header("X-RateLimit-Remaining", "0", "X-RateLimit-Reset", ms), nil, now)
	assert.Zero(t, q.Remaining)
	assert.Equal(t, holder{"openrouter:free": 30 * time.Second}, h)

	Observe(h, "outra", header("X-RateLimit-Remaining", "19", "X-RateLimit-Reset", ms), nil, now)
	Observe(h, "outra", nil, []byte("não é json"), now)
	assert.NotContains(t, h, "outra", "com cota sobrando, ou sem nada legível, ninguém espera")
}
