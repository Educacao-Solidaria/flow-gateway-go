// Package tracing gera e propaga o trace context do W3C (headers traceparent e
// tracestate, https://www.w3.org/TR/trace-context/) no formato que o
// OpenTelemetry propaga, sem depender dele: o propagator do OTel (Fase 4) lê e
// escreve os mesmos headers. Cada requisição é um salto do trace: o gateway
// continua o trace recebido (ou começa um) com um span id próprio, que vai como
// parent-id nas chamadas de saída (Inject).
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	mrand "math/rand/v2"
	"net/http"
	"slices"
	"strings"
)

// Nomes dos headers do W3C Trace Context.
const (
	TraceparentHeader = "traceparent"
	TracestateHeader  = "tracestate"
)

const (
	maxStateMembers = 32
	maxStateLen     = 512 // tamanho do tracestate propagado, vírgulas incluídas
	longMember      = 128 // no corte, membros maiores que isso saem primeiro
)

type (
	// TraceID identifica o trace inteiro; SpanID, um salto dele.
	TraceID [16]byte
	SpanID  [8]byte
)

func (t TraceID) String() string { return hex.EncodeToString(t[:]) }
func (s SpanID) String() string  { return hex.EncodeToString(s[:]) }

// SpanContext é o trace context de uma requisição no gateway.
type SpanContext struct {
	TraceID TraceID
	SpanID  SpanID // deste salto: vai como parent-id nas chamadas de saída
	Sampled bool   // o único flag do nível 1 da spec; os outros saem zerados
	State   string // tracestate a repassar, já validado e dentro dos limites
}

// New começa um trace novo, amostrado, como o sampler padrão do OTel
// (ParentBased(AlwaysOn)) faz com o trace que começa no serviço.
// ponytail: sem sampler configurável; a Fase 4 troca pelo do SDK do OTel.
func New() SpanContext {
	sc := SpanContext{Sampled: true}
	randomID(sc.TraceID[:])
	randomID(sc.SpanID[:])
	return sc
}

// FromHeaders continua o trace do traceparent de h ou, se ele faltar, vier
// repetido ou for inválido, começa um novo (New): lixo nunca é propagado. O
// tracestate só acompanha um traceparent válido. O span id é sempre novo.
func FromHeaders(h http.Header) SpanContext {
	parents := h.Values(TraceparentHeader)
	if len(parents) != 1 {
		return New()
	}
	tid, flags, ok := parseParent(parents[0])
	if !ok {
		return New()
	}
	sc := SpanContext{TraceID: tid, Sampled: flags&1 != 0, State: parseState(h.Values(TracestateHeader))}
	randomID(sc.SpanID[:])
	return sc
}

type ctxKey struct{}

// WithContext devolve um contexto que carrega sc.
func WithContext(ctx context.Context, sc SpanContext) context.Context {
	return context.WithValue(ctx, ctxKey{}, sc)
}

// FromContext devolve o trace context guardado por WithContext.
func FromContext(ctx context.Context) (SpanContext, bool) {
	sc, ok := ctx.Value(ctxKey{}).(SpanContext)
	return sc, ok
}

// Inject escreve em h, o header de uma requisição de saída, o traceparent
// (versão 00, com o span id deste salto como parent-id) e o tracestate do trace
// em ctx, apagando um tracestate que não seja o dele. Sem trace em ctx, não
// mexe em h.
func Inject(ctx context.Context, h http.Header) {
	sc, ok := FromContext(ctx)
	if !ok {
		return
	}
	flags := "-00"
	if sc.Sampled {
		flags = "-01"
	}
	h.Set(TraceparentHeader, "00-"+sc.TraceID.String()+"-"+sc.SpanID.String()+flags)
	h.Del(TracestateHeader)
	if sc.State != "" {
		h.Set(TracestateHeader, sc.State)
	}
}

// randRead é a fonte de entropia dos ids; variável para o teste simular falha.
var randRead = rand.Read

// randomID enche b de bytes aleatórios, nunca todos zero (id inválido na spec).
func randomID(b []byte) {
	for {
		if _, err := randRead(b); err != nil {
			// Antes do Go 1.24 crypto/rand.Read pode falhar (o go.mod aceita
			// 1.23). Id de trace não é segredo: math/rand/v2 basta.
			for i := range b {
				b[i] = byte(mrand.Uint32())
			}
		}
		if slices.ContainsFunc(b, func(c byte) bool { return c != 0 }) {
			return
		}
	}
}

// parseParent lê "versão-traceid-parentid-flags" em hex minúsculo. A versão 00
// tem exatamente 55 caracteres; versão futura pode trazer campos depois deles,
// após um "-", que são ignorados, como manda a spec. A versão ff e os ids só de
// zeros são inválidos.
func parseParent(h string) (TraceID, byte, bool) {
	var ver, flags [1]byte
	var tid TraceID
	var parent SpanID
	ok := len(h) >= 55 && h[2] == '-' && h[35] == '-' && h[52] == '-' &&
		parseHex(ver[:], h[:2]) && ver[0] != 0xff &&
		(len(h) == 55 || ver[0] != 0 && h[55] == '-') &&
		parseHex(tid[:], h[3:35]) && tid != TraceID{} &&
		parseHex(parent[:], h[36:52]) && parent != SpanID{} &&
		parseHex(flags[:], h[53:55])
	return tid, flags[0], ok
}

// parseHex decodifica s em dst; a spec só aceita hex minúsculo.
func parseHex(dst []byte, s string) bool {
	if len(s) != 2*len(dst) || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.Decode(dst, []byte(s))
	return err == nil
}

// parseState valida o tracestate (as linhas do header são uma lista só) e o
// devolve sem espaço opcional nem membro vazio. Chave ou valor fora da
// gramática, chave repetida ou mais de 32 membros descartam o header inteiro,
// como a spec permite. Acima de 512 caracteres, corta como ela manda: primeiro
// os membros maiores que 128 caracteres, depois os do fim.
func parseState(lines []string) string {
	var members []string
	keys := map[string]bool{}
	for _, line := range lines {
		for rest := line; rest != ""; {
			var m string
			m, rest, _ = strings.Cut(rest, ",")
			if m = strings.Trim(m, " \t"); m == "" {
				continue
			}
			k, v, ok := strings.Cut(m, "=")
			if !ok || keys[k] || !validKey(k) || !validValue(v) || len(members) == maxStateMembers {
				return ""
			}
			keys[k] = true
			members = append(members, m)
		}
	}
	for len(strings.Join(members, ",")) > maxStateLen { // só passa daqui com header gigante
		i := len(members) - 1
		for j := i; j >= 0; j-- {
			if len(members[j]) > longMember {
				i = j
				break
			}
		}
		members = slices.Delete(members, i, i+1)
	}
	return strings.Join(members, ",")
}

// validKey segue a gramática: simple-key (até 256) ou tenant-id@system-id
// (até 241 e 14).
func validKey(k string) bool {
	tenant, system, multi := strings.Cut(k, "@")
	if !multi {
		return len(k) <= 256 && keyChars(k, true)
	}
	return len(tenant) <= 241 && len(system) <= 14 && keyChars(tenant, false) && keyChars(system, true)
}

// keyChars: lcalpha (ou dígito, sem letterFirst) e depois lcalpha, dígito, "_", "-", "*" e "/".
func keyChars(s string, letterFirst bool) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c >= 'a' && c <= 'z' || c >= '0' && c <= '9' && (i > 0 || !letterFirst) ||
			i > 0 && (c == '_' || c == '-' || c == '*' || c == '/')
		if !ok {
			return false
		}
	}
	return s != ""
}

// validValue: 1 a 256 caracteres ASCII imprimíveis, sem "," nem "=". O espaço
// no fim, que a gramática proíbe, já saiu com o espaço opcional do membro.
func validValue(v string) bool {
	if v == "" || len(v) > 256 {
		return false
	}
	for i := 0; i < len(v); i++ {
		if c := v[i]; c < 0x20 || c > 0x7e || c == ',' || c == '=' {
			return false
		}
	}
	return true
}
