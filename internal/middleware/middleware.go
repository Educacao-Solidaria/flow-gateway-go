// Package middleware reúne os middlewares HTTP de correlação e resiliência do
// gateway: request id, IP real do cliente e recuperação de panic.
package middleware

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
)

// RequestIDHeader carrega o request id na entrada (vindo de proxy confiável) e
// na resposta.
const RequestIDHeader = "X-Request-Id"

// maxRequestIDLen limita o id aceito de fora: ele vai para todo log da requisição.
const maxRequestIDLen = 128

// TrustedProxies são as redes cujos cabeçalhos de encaminhamento
// (X-Forwarded-For, X-Real-IP, X-Request-Id) o gateway aceita. Vazio, nenhum
// cabeçalho do cliente é aceito.
type TrustedProxies []netip.Prefix

// ParseTrustedProxies aceita CIDRs ("10.0.0.0/8") e IPs avulsos ("10.0.0.1").
func ParseTrustedProxies(list []string) (TrustedProxies, error) {
	out := make(TrustedProxies, 0, len(list))
	for _, s := range list {
		s = strings.TrimSpace(s)
		p, err := netip.ParsePrefix(s)
		if err != nil {
			a, aerr := netip.ParseAddr(s)
			if aerr != nil {
				return nil, fmt.Errorf("middleware: proxy confiável %q não é IP nem CIDR", s)
			}
			p = netip.PrefixFrom(a.Unmap(), a.Unmap().BitLen())
		}
		out = append(out, p.Masked())
	}
	return out, nil
}

func (t TrustedProxies) contains(a netip.Addr) bool {
	for _, p := range t {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// peer devolve o IP da conexão TCP; zero se RemoteAddr não for IP:porta.
func peer(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	a, _ := netip.ParseAddr(host)
	return a.Unmap()
}

// clientIP percorre X-Forwarded-For da direita para a esquerda enquanto o salto
// for um proxy confiável: o primeiro endereço fora da lista é o cliente. Sem
// X-Forwarded-For, usa X-Real-IP. Entradas à esquerda do primeiro salto não
// confiável são ignoradas, porque o próprio cliente pode tê-las forjado.
func (t TrustedProxies) clientIP(r *http.Request) netip.Addr {
	ip := peer(r)
	if !t.contains(ip) {
		return ip
	}
	hops := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
	if len(hops) == 1 && strings.TrimSpace(hops[0]) == "" {
		if a, err := netip.ParseAddr(strings.TrimSpace(r.Header.Get("X-Real-IP"))); err == nil {
			return a.Unmap()
		}
		return ip
	}
	for i := len(hops) - 1; i >= 0; i-- {
		a, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			break // salto ilegível: fica o último proxy confiável
		}
		ip = a.Unmap()
		if !t.contains(ip) {
			break
		}
	}
	return ip
}

type clientIPKey struct{}

// ClientIP devolve o IP real do cliente gravado por RealIP; zero fora dele.
func ClientIP(ctx context.Context) netip.Addr {
	a, _ := ctx.Value(clientIPKey{}).(netip.Addr)
	return a
}

// RealIP grava no contexto o IP real do cliente (ver ClientIP), confiando nos
// cabeçalhos de encaminhamento só quando a conexão vem de um proxy confiável.
// r.RemoteAddr não é alterado.
func RealIP(trusted TrustedProxies) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), clientIPKey{}, trusted.clientIP(r))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequestID correlaciona a requisição: reaproveita o X-Request-Id recebido de
// proxy confiável (se bem formado) ou gera um novo, devolve-o no cabeçalho da
// resposta e o injeta no contexto como trace_id do logger.
func RequestID(trusted TrustedProxies) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(RequestIDHeader)
			if !validRequestID(id) || !trusted.contains(peer(r)) {
				id = logger.NewTraceID()
			}
			w.Header().Set(RequestIDHeader, id)
			next.ServeHTTP(w, r.WithContext(logger.WithTraceID(r.Context(), id)))
		})
	}
}

// validRequestID aceita só [A-Za-z0-9._:-]: nada de espaço, aspas ou quebra de
// linha que possa poluir log em formato texto.
func validRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}
	for _, c := range []byte(id) {
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '.' || c == '_' || c == ':' || c == '-'
		if !ok {
			return false
		}
	}
	return true
}

// Recoverer transforma panic do handler em 500 logado, sem derrubar o processo
// nem expor stack trace. http.ErrAbortHandler é repassado, como no net/http.
func Recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			defer func() {
				if rec := recover(); rec != nil {
					if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
						panic(rec)
					}
					log.ErrorContext(r.Context(), "panic no handler", "panic", fmt.Sprint(rec), "path", r.URL.Path)
					// Com a resposta já começada, o status foi enviado: um 500
					// agora só colaria texto no fim do corpo.
					if ww.Status() == 0 {
						http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
					}
				}
			}()
			next.ServeHTTP(ww, r)
		})
	}
}
