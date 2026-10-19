package ratelimit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"strconv"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/middleware"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

// KeyFunc extrai a chave de limitação da requisição; ok falso deixa a
// requisição passar sem limite (não há por onde identificá-la).
type KeyFunc func(r *http.Request) (key string, ok bool)

// ByClientIP usa o IP que middleware.RealIP gravou no contexto — resolvido
// contra os proxies confiáveis, nunca lido de cabeçalho cru —, com IPv6
// agrupado por /64: um host IPv6 costuma controlar a /64 inteira, e por
// endereço o limite cairia girando o sufixo. Sem RealIP na cadeia, ok é falso.
func ByClientIP(r *http.Request) (string, bool) {
	ip := middleware.ClientIP(r.Context())
	if !ip.IsValid() {
		return "", false
	}
	if ip.Is6() {
		p, _ := ip.Prefix(64) // não falha: 64 <= 128
		return "ip:" + p.String(), true
	}
	return "ip:" + ip.String(), true
}

// ByAPIKey limita por API key já autenticada, que apiKey extrai da requisição
// (o encaixe para o middleware de autenticação, que ainda não existe). A
// chave do mapa é um hash da API key: o limitador não retém o segredo, e ele
// não chega a log nem a resposta por aqui.
func ByAPIKey(apiKey func(r *http.Request) (string, bool)) KeyFunc {
	return func(r *http.Request) (string, bool) {
		k, ok := apiKey(r)
		if !ok || k == "" {
			return "", false
		}
		sum := sha256.Sum256([]byte(k))
		return "key:" + hex.EncodeToString(sum[:16]), true
	}
}

// Middleware recusa com 429, Retry-After e o erro RATE_LIMIT_EXCEEDED do
// gateway a requisição cuja chave estourou o limite. Com ByClientIP, monte
// depois do middleware.RealIP (o server.New já o põe primeiro).
func Middleware(l *Limiter, key KeyFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			k, ok := key(r)
			if !ok {
				next.ServeHTTP(w, r)
				return
			}
			allowed, wait := l.Allow(k)
			if allowed {
				next.ServeHTTP(w, r)
				return
			}
			secs := max(int64(math.Ceil(wait.Seconds())), 1) // Retry-After é inteiro e >= 1
			gerr := gwerrors.NewRateLimitError(map[string]interface{}{"retry_after_seconds": secs})
			w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(gerr.HTTPStatus)
			_ = json.NewEncoder(w).Encode(gerr) // cliente que caiu não tem a quem avisar
		})
	}
}
