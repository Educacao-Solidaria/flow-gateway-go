package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
)

// BasicAuth exige o par user:pass (RFC 7617) e, sem ele ou com ele errado,
// responde 401 com WWW-Authenticate. Compara o SHA-256 de cada campo em tempo
// constante: o hash iguala os tamanhos (ConstantTimeCompare volta na hora com
// tamanhos diferentes e o tempo revelaria o tamanho certo) e os dois resultados
// se juntam sem curto-circuito, para o tempo não dizer qual campo errou. Com
// user ou pass vazio, recusa tudo. Nada da credencial vai para log ou resposta.
func BasicAuth(realm, user, pass string) func(http.Handler) http.Handler {
	wantUser, wantPass := sha256.Sum256([]byte(user)), sha256.Sum256([]byte(pass))
	configured := user != "" && pass != ""
	challenge := `Basic realm="` + realm + `", charset="UTF-8"`
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			u, p, ok := r.BasicAuth()
			gotUser, gotPass := sha256.Sum256([]byte(u)), sha256.Sum256([]byte(p))
			match := subtle.ConstantTimeCompare(gotUser[:], wantUser[:]) & subtle.ConstantTimeCompare(gotPass[:], wantPass[:])
			if !ok || !configured || match != 1 {
				w.Header().Set("WWW-Authenticate", challenge)
				http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
