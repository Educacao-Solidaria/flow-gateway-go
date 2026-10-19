// Package quota lê a cota que o upstream informa (X-RateLimit-Limit,
// X-RateLimit-Remaining, X-RateLimit-Reset e Retry-After) e a converte na
// espera preventiva do limitador (ratelimit.Limiter.Hold): o gateway recusa
// sozinho em vez de insistir num 429.
//
// Formato conferido em 19/10/2026. A doc da OpenRouter (api/reference/limits)
// diz que os X-RateLimit-* vêm só no 429 de limite da plataforma e não diz a
// unidade do Reset; os 429 reais trazem epoch em milissegundos
// ("X-RateLimit-Reset":"1785658140000"), muitas vezes só ecoados no JSON de
// erro (error.metadata.headers). O Retry-After segue a RFC 9110: segundos ou
// data HTTP.
package quota

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxWait limita a espera que um valor do upstream pode impor: o limite diário
// da OpenRouter recomeça à meia-noite UTC, e nada legítimo passa disso.
const maxWait = 24 * time.Hour

// Quota é a cota informada; valor ausente ou malformado fica no zero (Remaining
// em -1), nunca negativo.
type Quota struct {
	Limit      int64         // requisições na janela; 0 = não informado
	Remaining  int64         // quantas restam; -1 = não informado
	Reset      time.Time     // quando a janela recomeça; zero = não informado
	RetryAfter time.Duration // espera pedida; 0 = não informada
}

// Parse lê a cota de h e completa o que faltar com o que a OpenRouter ecoa no
// JSON de erro; body pode ser nil. now é o relógio de parede (time.Now()).
func Parse(h http.Header, body []byte, now time.Time) Quota {
	h = withEcho(h, body)
	q := Quota{Limit: max(count(h.Get("X-RateLimit-Limit")), 0), Remaining: count(h.Get("X-RateLimit-Remaining"))}
	if q.Limit > 0 && q.Remaining > q.Limit {
		q.Remaining = q.Limit
	}
	q.Reset = reset(h.Get("X-RateLimit-Reset"), now)
	if n := count(h.Get("Retry-After")); n >= 0 {
		q.RetryAfter = time.Duration(min(n, int64(maxWait/time.Second))) * time.Second
	} else if t, err := http.ParseTime(h.Get("Retry-After")); err == nil {
		q.RetryAfter = max(min(t.Sub(now), maxWait), 0)
	}
	return q
}

// Wait é quanto segurar antes da próxima requisição: o Retry-After ou, com a
// cota zerada, o tempo até o Reset (o maior dos dois); 0 quando nada pede
// espera. Nunca passa de maxWait.
func (q Quota) Wait(now time.Time) time.Duration {
	w := q.RetryAfter
	if q.Remaining == 0 && !q.Reset.IsZero() {
		w = max(w, q.Reset.Sub(now)) // Sub satura em vez de estourar
	}
	return max(min(w, maxWait), 0)
}

// Holder é o encaixe do limitador preventivo; *ratelimit.Limiter o satisfaz.
type Holder interface {
	Hold(key string, d time.Duration)
}

// Observe lê a cota de uma resposta do upstream e, se ela pede espera, segura
// key em l por esse tempo. Devolve a cota lida, para log e métrica.
func Observe(l Holder, key string, h http.Header, body []byte, now time.Time) Quota {
	q := Parse(h, body, now)
	if w := q.Wait(now); w > 0 {
		l.Hold(key, w)
	}
	return q
}

// count lê um inteiro não negativo; -1 se ausente ou malformado.
func count(v string) int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// reset lê o X-RateLimit-Reset, cuja unidade não é padrão: pela grandeza, epoch
// em ms (OpenRouter), epoch em s ou segundos até o reset; texto com unidade
// ("6m0s") também é duração.
func reset(v string, now time.Time) time.Time {
	switch n := count(v); {
	case n >= 1e12:
		return time.UnixMilli(n)
	case n >= 1e9:
		return time.Unix(n, 0)
	case n >= 0:
		return now.Add(time.Duration(n) * time.Second) // n < 1e9: não estoura
	}
	if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil && d >= 0 {
		return now.Add(d)
	}
	return time.Time{}
}

// withEcho completa h com os valores que a OpenRouter ecoa no JSON de erro
// (error.metadata.headers); o header de verdade vence. JSON que não casa é
// ignorado.
func withEcho(h http.Header, body []byte) http.Header {
	var e struct {
		Error struct {
			Metadata struct {
				Headers map[string]string `json:"headers"`
			} `json:"metadata"`
		} `json:"error"`
	}
	if len(body) == 0 || json.Unmarshal(body, &e) != nil || len(e.Error.Metadata.Headers) == 0 {
		return h
	}
	out := h.Clone()
	if out == nil {
		out = http.Header{}
	}
	for k, v := range e.Error.Metadata.Headers {
		if out.Get(k) == "" {
			out.Set(k, v)
		}
	}
	return out
}
