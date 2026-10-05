// Package testutil reúne helpers de teste para simular requisições e
// respostas HTTP e capturar o log estruturado do gateway.
package testutil

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
)

// NewRequest monta uma requisição de teste. body nil não manda corpo; string e
// []byte vão como estão; qualquer outro valor é serializado em JSON. Com
// corpo, Content-Type vira application/json.
func NewRequest(t testing.TB, method, target string, body any) *http.Request {
	t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		r = strings.NewReader(b)
	case []byte:
		r = bytes.NewReader(b)
	default:
		data, err := json.Marshal(b)
		require.NoError(t, err, "serializando corpo da requisição")
		r = bytes.NewReader(data)
	}
	req := httptest.NewRequest(method, target, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

// Serve passa req por h e devolve a resposta gravada.
func Serve(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// DecodeJSON decodifica o corpo de rec em T, falhando o teste se não for JSON.
func DecodeJSON[T any](t testing.TB, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v), "corpo: %s", rec.Body)
	return v
}

// AssertJSON confere status e corpo; o corpo é comparado como JSON, então
// ordem de campos e espaços não importam.
func AssertJSON(t testing.TB, rec *httptest.ResponseRecorder, status int, want string) bool {
	t.Helper()
	return assert.Equal(t, status, rec.Code, "status; corpo: %s", rec.Body) &&
		assert.JSONEq(t, want, rec.Body.String())
}

// LogBuffer é um io.Writer seguro para concorrência: o servidor escreve log
// numa goroutine enquanto o teste lê em outra.
type LogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *LogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Entries decodifica cada linha JSON do log num mapa de campos.
func (b *LogBuffer) Entries(t testing.TB) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(strings.NewReader(b.String()))
	for sc.Scan() {
		var e map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &e), "linha de log: %s", sc.Text())
		out = append(out, e)
	}
	return out
}

// NewLogger cria o logger real do gateway (JSON, nível debug, trace_id e
// mascaramento) escrevendo num LogBuffer.
func NewLogger(t testing.TB) (*slog.Logger, *LogBuffer) {
	t.Helper()
	buf := &LogBuffer{}
	log, err := logger.New(buf, "debug", "json")
	require.NoError(t, err)
	return log, buf
}
