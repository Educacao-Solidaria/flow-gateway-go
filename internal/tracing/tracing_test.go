package tracing

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	tid    = "4bf92f3577b34da6a3ce929d0e0e4736"
	parent = "00f067aa0ba902b7"
	valid  = "00-" + tid + "-" + parent + "-01"
)

func headers(parents []string, state ...string) http.Header {
	h := http.Header{}
	for _, p := range parents {
		h.Add(TraceparentHeader, p)
	}
	for _, s := range state {
		h.Add(TracestateHeader, s)
	}
	return h
}

func TestFromHeadersContinuesValidTrace(t *testing.T) {
	for _, tc := range []struct {
		parent  string
		sampled bool
	}{
		{valid, true},
		{"00-" + tid + "-" + parent + "-00", false},
		{"00-" + tid + "-" + parent + "-ff", true}, // flags desconhecidos zerados, sampled fica
		{"00-" + tid + "-" + parent + "-02", false},
		{"01-" + tid + "-" + parent + "-01", true},                 // versão futura
		{"cc-" + tid + "-" + parent + "-01-campo-do-futuro", true}, // versão futura com campo novo
	} {
		sc := FromHeaders(headers([]string{tc.parent}, "a=1"))
		assert.Equal(t, tid, sc.TraceID.String(), tc.parent)
		assert.NotEqual(t, parent, sc.SpanID.String(), "%s: o span id deste salto é novo", tc.parent)
		assert.NotEqual(t, SpanID{}, sc.SpanID, tc.parent)
		assert.Equal(t, tc.sampled, sc.Sampled, tc.parent)
		assert.Equal(t, "a=1", sc.State, tc.parent)
	}
}

func TestFromHeadersRestartsInvalidTrace(t *testing.T) {
	zeros32, zeros16 := strings.Repeat("0", 32), strings.Repeat("0", 16)
	for name, parents := range map[string][]string{
		"ausente":                    nil,
		"repetido":                   {valid, valid},
		"curto":                      {valid[:54]},
		"versão 00 com campo a mais": {valid + "-x"},
		"versão futura sem traço":    {"01-" + tid + "-" + parent + "-01x"},
		"versão ff":                  {"ff" + valid[2:]},
		"versão não hex":             {"0x" + valid[2:]},
		"hex maiúsculo":              {strings.ToUpper(valid)},
		"trace id não hex":           {"00-" + tid[:31] + "g-" + parent + "-01"},
		"trace id zerado":            {"00-" + zeros32 + "-" + parent + "-01"},
		"parent id zerado":           {"00-" + tid + "-" + zeros16 + "-01"},
		"separador errado":           {"00_" + valid[3:]},
		"flags não hex":              {valid[:53] + "0g"},
	} {
		sc := FromHeaders(headers(parents, "a=1"))
		assert.NotEqual(t, tid, sc.TraceID.String(), name)
		assert.NotEqual(t, TraceID{}, sc.TraceID, name)
		assert.True(t, sc.Sampled, "%s: trace novo sai amostrado", name)
		assert.Empty(t, sc.State, "%s: tracestate só acompanha traceparent válido", name)
	}
}

func TestParseState(t *testing.T) {
	members := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("k%d=v", i)
		}
		return out
	}
	join := func(m []string) string { return strings.Join(m, ",") }
	for _, tc := range []struct {
		name  string
		lines []string
		want  string
	}{
		{"vazio", nil, ""},
		{"espaço opcional e membros vazios", []string{" a=1 ,, b@sys=x y\t", ","}, "a=1,b@sys=x y"},
		{"linhas do header viram uma lista", []string{"a=1", "b=2"}, "a=1,b=2"},
		{"tenant pode começar por dígito", []string{"1a@b=1"}, "1a@b=1"},
		{"valor de 256", []string{"a=" + strings.Repeat("v", 256)}, "a=" + strings.Repeat("v", 256)},
		{"32 membros", members(32), join(members(32))},
		{"33 membros", members(33), ""},
		{"chave com maiúscula", []string{"a=1,B=2"}, ""},
		{"chave simples começando por dígito", []string{"1a=1"}, ""},
		{"chave simples de 257", []string{strings.Repeat("k", 257) + "=1"}, ""},
		{"system id de 15", []string{"a@" + strings.Repeat("b", 15) + "=1"}, ""},
		{"tenant id de 242", []string{strings.Repeat("t", 242) + "@b=1"}, ""},
		{"chave vazia", []string{"=1"}, ""},
		{"sem =", []string{"a"}, ""},
		{"valor vazio", []string{"a="}, ""},
		{"valor com =", []string{"a=1=2"}, ""},
		{"valor não ASCII", []string{"a=ç"}, ""},
		{"valor de 257", []string{"a=" + strings.Repeat("v", 257)}, ""},
		{"chave repetida", []string{"a=1", "a=2"}, ""},
	} {
		assert.Equal(t, tc.want, parseState(tc.lines), tc.name)
	}
}

func TestParseStateTruncatesTo512(t *testing.T) {
	fill := make([]string, 12)
	for i := range fill {
		fill[i] = fmt.Sprintf("m%02d=%s", i, strings.Repeat("y", 40)) // 44 caracteres
	}
	long := "longo=" + strings.Repeat("x", 200)
	// 205 + 10×44 + 10 vírgulas = 655: sai o longo, mesmo no começo, e o resto cabe.
	assert.Equal(t, strings.Join(fill[:10], ","), parseState(append([]string{long}, fill[:10]...)))
	// 12×44 + 11 = 539, sem membro longo: saem os do fim até caber.
	got := parseState(fill)
	assert.Equal(t, strings.Join(fill[:11], ","), got)
	assert.LessOrEqual(t, len(got), maxStateLen)
}

func TestNewIDs(t *testing.T) {
	a, b := New(), New()
	assert.NotEqual(t, a.TraceID, b.TraceID)
	assert.NotEqual(t, a.SpanID, b.SpanID)
	assert.True(t, a.Sampled)
	assert.Empty(t, a.State)

	orig := randRead
	defer func() { randRead = orig }()
	randRead = func([]byte) (int, error) { return 0, errors.New("sem entropia") }
	sc := New()
	assert.NotEqual(t, TraceID{}, sc.TraceID, "a falha do crypto/rand cai no math/rand")
	assert.NotEqual(t, SpanID{}, sc.SpanID)

	calls := 0
	randRead = func(b []byte) (int, error) { // a 1ª leitura vem toda zero: id inválido
		if calls++; calls > 1 {
			b[0] = 7
		}
		return len(b), nil
	}
	sc = New()
	assert.Equal(t, byte(7), sc.TraceID[0], "id só de zeros é sorteado de novo")
}

func TestInjectRoundTrip(t *testing.T) {
	h := http.Header{}
	h.Set(TracestateHeader, "alheio=1")
	Inject(context.Background(), h)
	assert.Equal(t, "alheio=1", h.Get(TracestateHeader), "sem trace no contexto, nada muda")
	assert.Empty(t, h.Get(TraceparentHeader))

	sc := FromHeaders(headers([]string{valid}, "a=1"))
	out := http.Header{}
	Inject(WithContext(context.Background(), sc), out)
	assert.Equal(t, "00-"+tid+"-"+sc.SpanID.String()+"-01", out.Get(TraceparentHeader))
	assert.Equal(t, "a=1", out.Get(TracestateHeader))
	next := FromHeaders(out)
	assert.Equal(t, sc.TraceID, next.TraceID, "o próximo salto continua o mesmo trace")
	assert.Equal(t, "a=1", next.State)

	sc.State, sc.Sampled = "", false
	Inject(WithContext(context.Background(), sc), h)
	assert.Empty(t, h.Values(TracestateHeader), "tracestate alheio não acompanha o trace")
	assert.Equal(t, "00-"+tid+"-"+sc.SpanID.String()+"-00", h.Get(TraceparentHeader))
}
