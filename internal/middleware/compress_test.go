package middleware

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/klauspost/compress/gzhttp"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// served é o que o cliente receberia: headers do momento em que o status saiu.
type served struct {
	Header     http.Header
	StatusCode int
}

// serveCompressed passa uma requisição pelo Compress e devolve a resposta com o
// corpo ainda codificado.
func serveCompressed(t *testing.T, method, accept string, h http.HandlerFunc) (served, []byte) {
	t.Helper()
	req := httptest.NewRequest(method, "/", nil)
	if accept != "" {
		req.Header.Set("Accept-Encoding", accept)
	}
	rec := httptest.NewRecorder()
	Compress()(h).ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return served{res.Header, res.StatusCode}, body
}

// decoder devolve o leitor que desfaz a codificação enc.
func decoder(t *testing.T, enc string, r io.Reader) io.Reader {
	t.Helper()
	switch enc {
	case "":
		return r
	case "gzip":
		zr, err := gzip.NewReader(r)
		require.NoError(t, err)
		return zr
	case "zstd":
		zr, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
		require.NoError(t, err)
		t.Cleanup(zr.Close)
		return zr
	}
	t.Fatalf("codificação inesperada %q", enc)
	return nil
}

func decoded(t *testing.T, enc string, body []byte) []byte {
	t.Helper()
	out, err := io.ReadAll(decoder(t, enc, bytes.NewReader(body)))
	require.NoError(t, err)
	return out
}

func respond(contentType string, body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(body)
	}
}

func TestCompressNegotiation(t *testing.T) {
	big := bytes.Repeat([]byte(`{"delta":"ola"}`), 300)
	for _, tc := range []struct{ accept, want string }{
		{"", ""},
		{"gzip", "gzip"},
		{"GZIP", "gzip"},
		{"zstd", "zstd"},
		{"gzip, zstd", "zstd"}, // q iguais: zstd
		{"gzip;q=1, zstd;q=0.5", "gzip"},
		{"gzip;q=0", ""}, // q=0 proíbe
		{"gzip;q=0, zstd;q=0", ""},
		{"zstd;q=0, gzip", "gzip"},
		{"br, deflate", ""}, // nada que o gateway fale
		{"*", ""},
		{"identity", ""},
	} {
		res, body := serveCompressed(t, http.MethodGet, tc.accept, respond("application/json", big))
		enc := res.Header.Get("Content-Encoding")
		assert.Equal(t, tc.want, enc, "Accept-Encoding %q", tc.accept)
		assert.Contains(t, res.Header.Values("Vary"), "Accept-Encoding", "Accept-Encoding %q", tc.accept)
		assert.Equal(t, big, decoded(t, enc, body), "Accept-Encoding %q", tc.accept)
	}
}

func TestCompressThreshold(t *testing.T) {
	for _, tc := range []struct {
		name          string
		size          int
		contentLength bool // o handler declara Content-Length
		want          string
	}{
		{"vazio", 0, false, ""},
		{"1 KB", CompressMinSize, false, ""},
		{"1 KB + 1", CompressMinSize + 1, false, "gzip"},
		{"1 KB com Content-Length", CompressMinSize, true, ""},
		{"8 KB com Content-Length", 8 << 10, true, "gzip"},
	} {
		body := bytes.Repeat([]byte("a"), tc.size)
		res, got := serveCompressed(t, http.MethodGet, "gzip", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			if tc.contentLength {
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			}
			_, _ = w.Write(body)
		})
		enc := res.Header.Get("Content-Encoding")
		assert.Equal(t, tc.want, enc, tc.name)
		if enc != "" {
			assert.Empty(t, res.Header.Get("Content-Length"), "%s: o tamanho comprimido é outro", tc.name)
		}
		assert.Equal(t, body, decoded(t, enc, got), tc.name)
	}
}

func TestCompressOnlyTextTypes(t *testing.T) {
	big := bytes.Repeat([]byte("texto "), 500)
	for ct, want := range map[string]string{
		"application/json; charset=utf-8": "zstd",
		"application/problem+json":        "zstd",
		"text/event-stream":               "",
		"image/svg+xml":                   "zstd",
		"":                                "zstd", // detectado pelo conteúdo: text/plain
		"image/png":                       "",
		"application/zip":                 "",
		"application/gzip":                "",
		"application/octet-stream":        "",
		"text/plain; charset":             "zstd", // só o parâmetro está malformado
		"tipo;;inválido":                  "",
	} {
		res, body := serveCompressed(t, http.MethodGet, "gzip, zstd", func(w http.ResponseWriter, _ *http.Request) {
			if ct != "" {
				w.Header().Set("Content-Type", ct)
			}
			_, _ = w.Write(big)
		})
		assert.Equal(t, want, res.Header.Get("Content-Encoding"), "Content-Type %q", ct)
		assert.Equal(t, big, decoded(t, want, body), "Content-Type %q", ct)
	}
}

func TestCompressLeavesEncodedAndBodylessResponses(t *testing.T) {
	big := bytes.Repeat([]byte("a"), 4<<10)
	cases := map[string]http.HandlerFunc{
		"Content-Encoding do handler": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Encoding", "br")
			_, _ = w.Write(big)
		},
		"Content-Range": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Range", "bytes 0-4095/8192")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(big)
		},
		"header de desligar": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(gzhttp.HeaderNoCompression, "1")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(big)
		},
	}
	for name, h := range cases {
		res, body := serveCompressed(t, http.MethodGet, "gzip", h)
		assert.NotEqual(t, "gzip", res.Header.Get("Content-Encoding"), name)
		assert.Empty(t, res.Header.Get(gzhttp.HeaderNoCompression), "%s: o header não chega ao cliente", name)
		assert.Equal(t, big, body, name)
	}

	res, _ := serveCompressed(t, http.MethodHead, "gzip", respond("application/json", big))
	assert.Empty(t, res.Header.Get("Content-Encoding"), "HEAD")
	for _, code := range []int{http.StatusNoContent, http.StatusNotModified} {
		res, body := serveCompressed(t, http.MethodGet, "gzip", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
		})
		assert.Equal(t, code, res.StatusCode)
		assert.Empty(t, res.Header.Get("Content-Encoding"), "%d", code)
		assert.Empty(t, body, "%d", code)
	}
}

// TestCompressStreamsSSEEventByEvent prova que SSE passa sem compressão e sem
// ficar retido no buffer: o handler só escreve o próximo evento depois que o
// cliente leu o anterior. Um evento retido estoura o prazo do cliente em vez de
// travar o teste.
func TestCompressStreamsSSEEventByEvent(t *testing.T) {
	for _, accept := range []string{"gzip", "zstd", "gzip, zstd"} {
		t.Run(accept, func(t *testing.T) {
			const events = 3
			read := make(chan struct{}, events) // com buffer: o cliente nunca espera o handler
			srv := httptest.NewServer(Compress()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// SSE longo estende o WriteTimeout do servidor através do middleware.
				if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(time.Minute)); err != nil {
					t.Error(err)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				for i := 0; i < events; i++ {
					_, _ = fmt.Fprintf(w, "data: %d\n\n", i)
					w.(http.Flusher).Flush()
					select {
					case <-read:
					case <-r.Context().Done():
						return
					}
				}
			})))
			defer srv.Close()

			req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
			require.NoError(t, err)
			req.Header.Set("Accept-Encoding", accept)
			res, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
			require.NoError(t, err)
			defer res.Body.Close()
			require.Empty(t, res.Header.Get("Content-Encoding"))

			lines := bufio.NewReader(res.Body)
			for i := 0; i < events; i++ {
				line, err := lines.ReadString('\n')
				require.NoError(t, err, "evento %d", i)
				assert.Equal(t, fmt.Sprintf("data: %d\n", i), line)
				_, err = lines.ReadString('\n') // linha em branco que fecha o evento
				require.NoError(t, err)
				read <- struct{}{}
			}
		})
	}
}
