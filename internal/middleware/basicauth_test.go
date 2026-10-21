package middleware

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBasicAuth(t *testing.T) {
	const pass = "s3gredo-bem-longo"
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	b64 := func(s string) string { return "Basic " + base64.StdEncoding.EncodeToString([]byte(s)) }
	for name, tc := range map[string]struct {
		h      http.Handler
		header string
		want   int
	}{
		"certa":                 {BasicAuth("r", "prom", pass)(next), b64("prom:" + pass), http.StatusNoContent},
		"sem header":            {BasicAuth("r", "prom", pass)(next), "", http.StatusUnauthorized},
		"senha errada":          {BasicAuth("r", "prom", pass)(next), b64("prom:s3gredo-bem-lonGo"), http.StatusUnauthorized},
		"usuário errado":        {BasicAuth("r", "prom", pass)(next), b64("Prom:" + pass), http.StatusUnauthorized},
		"senha mais curta":      {BasicAuth("r", "prom", pass)(next), b64("prom:s3gredo"), http.StatusUnauthorized},
		"senha mais longa":      {BasicAuth("r", "prom", pass)(next), b64("prom:" + pass + "x"), http.StatusUnauthorized},
		"usuário mais longo":    {BasicAuth("r", "prom", pass)(next), b64("prometheus:" + pass), http.StatusUnauthorized},
		"par vazio":             {BasicAuth("r", "prom", pass)(next), b64(":"), http.StatusUnauthorized},
		"sem dois-pontos":       {BasicAuth("r", "prom", pass)(next), b64("prom" + pass), http.StatusUnauthorized},
		"outro esquema":         {BasicAuth("r", "prom", pass)(next), "Bearer " + pass, http.StatusUnauthorized},
		"base64 inválido":       {BasicAuth("r", "prom", pass)(next), "Basic ***", http.StatusUnauthorized},
		"configurada vazia":     {BasicAuth("r", "", "")(next), b64(":"), http.StatusUnauthorized},
		"só usuário no config":  {BasicAuth("r", "prom", "")(next), b64("prom:"), http.StatusUnauthorized},
		"dois-pontos na senha":  {BasicAuth("r", "prom", "a:b")(next), b64("prom:a:b"), http.StatusNoContent},
		"esquema em minúsculas": {BasicAuth("r", "prom", pass)(next), "basic " + strings.TrimPrefix(b64("prom:"+pass), "Basic "), http.StatusNoContent},
	} {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		if tc.header != "" {
			req.Header.Set("Authorization", tc.header)
		}
		rec := httptest.NewRecorder()
		tc.h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("%s: status %d, queria %d", name, rec.Code, tc.want)
		}
		if tc.want == http.StatusUnauthorized {
			if got := rec.Header().Get("WWW-Authenticate"); got != `Basic realm="r", charset="UTF-8"` {
				t.Errorf("%s: WWW-Authenticate = %q", name, got)
			}
			if strings.Contains(rec.Body.String(), "s3gredo") {
				t.Errorf("%s: a resposta ecoou a credencial: %q", name, rec.Body)
			}
		}
	}
}
