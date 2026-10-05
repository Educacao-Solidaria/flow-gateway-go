package testutil

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
)

// echo devolve método, Content-Type e corpo recebidos como JSON.
var echo = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"method":"`+r.Method+`","ct":"`+r.Header.Get("Content-Type")+`","body":`+string(body)+`}`)
})

func TestNewRequestBodies(t *testing.T) {
	cases := []struct {
		name string
		body any
		want string
	}{
		{"struct vira JSON", struct {
			Model string `json:"model"`
		}{"x"}, `{"method":"POST","ct":"application/json","body":{"model":"x"}}`},
		{"string vai como está", `{"a":1}`, `{"method":"POST","ct":"application/json","body":{"a":1}}`},
		{"[]byte vai como está", []byte(`[1,2]`), `{"method":"POST","ct":"application/json","body":[1,2]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := Serve(echo, NewRequest(t, http.MethodPost, "/", c.body))
			AssertJSON(t, rec, http.StatusOK, c.want)
		})
	}
}

func TestNewRequestWithoutBody(t *testing.T) {
	req := NewRequest(t, http.MethodGet, "/x?q=1", nil)
	assert.Empty(t, req.Header.Get("Content-Type"))
	assert.Equal(t, "1", req.URL.Query().Get("q"))
}

func TestDecodeJSON(t *testing.T) {
	rec := Serve(echo, NewRequest(t, http.MethodPut, "/", map[string]int{"n": 7}))
	got := DecodeJSON[struct {
		Method string         `json:"method"`
		Body   map[string]int `json:"body"`
	}](t, rec)
	assert.Equal(t, http.MethodPut, got.Method)
	assert.Equal(t, 7, got.Body["n"])
}

func TestNewLoggerCapturesEntries(t *testing.T) {
	log, buf := NewLogger(t)
	ctx := logger.WithTraceID(context.Background(), "abc")

	var wg sync.WaitGroup
	for range 10 { // escritas concorrentes: o -race pega se o buffer não travar
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.InfoContext(ctx, "evento", "api_key", "sk-segredo")
		}()
	}
	wg.Wait()

	entries := buf.Entries(t)
	require.Len(t, entries, 10)
	assert.Equal(t, "abc", entries[0][logger.TraceIDKey])
	assert.Equal(t, logger.Redacted, entries[0]["api_key"])
}
