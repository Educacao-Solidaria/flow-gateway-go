package raghttp

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

// raw é a resposta HTTP inteira como o uvicorn manda: status, cabeçalhos e corpo.
func raw(status, body string) string {
	return "HTTP/1.1 " + status + "\r\nContent-Type: application/json\r\nContent-Length: " +
		strconv.Itoa(len(body)) + "\r\nConnection: close\r\n\r\n" + body
}

// cut é o brain-router que morre no meio da resposta: só os primeiros k bytes
// de resp saem (status e cabeçalhos inclusive), e a conexão fecha.
func cut(w http.ResponseWriter, resp string, k int) {
	nc, _, _ := w.(http.Hijacker).Hijack()
	_, _ = io.WriteString(nc, resp[:k])
	_ = nc.Close()
}

// O corte em qualquer byte da resposta (status, cabeçalho ou corpo) é falha de
// transporte: a busca, idempotente, repete e acerta na conexão seguinte.
func TestSearchRetriesAResponseCutAtAnyByte(t *testing.T) {
	resp := raw("200 OK", searchOK)
	c, calls := fake(t, func(n int32, _ string, w http.ResponseWriter, _ *http.Request) {
		k := len(resp)
		if n%2 == 1 {
			k = int(n / 2) // chamadas 1, 3, 5...: cortadas nos bytes 0, 1, 2...
		}
		cut(w, resp, k)
	})
	c.searchTimeout = 2 * time.Second // o padrão: um runner lento não vira prazo vencido no meio de mil pedidos
	before := runtime.NumGoroutine()
	for k := range len(resp) {
		got, err := c.HybridSearch(context.Background(), "q", 5, 0.5)
		require.NoError(t, err, "corte no byte %d", k)
		require.Len(t, got.Hits, 2)
		require.EqualValues(t, 2*(k+1), calls.Load(), "corte no byte %d: uma repetição só", k)
	}
	c.hc.CloseIdleConnections()
	testutil.NoGoroutineLeak(t, before)
}

// Com o brain-router cortando toda resposta, a busca falha sem resultado
// parcial (a rota segue sem contexto), e o disjuntor abre no 5º corte seguido:
// dali em diante, falha na hora sem chamar ninguém. A indexação cortada não
// repete: a tentativa pode ter indexado.
func TestCutOnEveryAttemptDegradesToNoContext(t *testing.T) {
	resp := raw("200 OK", searchOK)
	c, calls := fake(t, func(_ int32, _ string, w http.ResponseWriter, _ *http.Request) {
		cut(w, resp, len(resp)-len(searchOK)/2)
	})
	got, err := c.HybridSearch(context.Background(), "q", 5, 0.5)
	require.ErrorIs(t, err, errBodyRead)
	assert.Nil(t, got)
	assert.EqualValues(t, 3, calls.Load())
	for range 2 {
		_, err = c.HybridSearch(context.Background(), "q", 5, 0.5)
		require.ErrorIs(t, err, gwerrors.ErrCircuitOpen)
	}
	assert.EqualValues(t, 5, calls.Load())

	idx := raw("201 Created", `{"document_id":"d1","chunks_created":3,"success":true,"message":"ok"}`)
	c, calls = fake(t, func(_ int32, _ string, w http.ResponseWriter, _ *http.Request) { cut(w, idx, len(idx)-2) })
	_, err = c.IndexDocument(context.Background(), "t", "conteúdo", "file:///a.md")
	require.ErrorIs(t, err, errBodyRead)
	assert.EqualValues(t, 1, calls.Load())
}
