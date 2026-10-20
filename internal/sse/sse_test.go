package sse_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/sse"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil/openroutertest"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

// collect lê os dois canais até o fim, como o consumidor do contrato.
func collect(t *testing.T, chunks <-chan domain.StreamChunk, errc <-chan error) ([]domain.StreamChunk, error) {
	t.Helper()
	var got []domain.StreamChunk
	for c := range chunks {
		got = append(got, c)
	}
	return got, <-errc
}

func decode(t *testing.T, r io.Reader) ([]domain.StreamChunk, error) {
	chunks, errc := sse.Decode(context.Background(), io.NopCloser(r), 1<<10)
	return collect(t, chunks, errc)
}

// mockStream devolve os bytes crus do stream SSE do openroutertest.
func mockStream(t *testing.T, chunks ...string) []byte {
	srv := openroutertest.New(t)
	srv.Enqueue(openroutertest.Response{Chunks: chunks})
	resp, err := http.Post(srv.URL+"/chat/completions", "application/json",
		strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"oi"}]}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return raw
}

func TestDecodeMockStreamFragmentedAtEveryByte(t *testing.T) {
	raw := mockStream(t, "Olá", ", ", "mundo")
	want := []domain.StreamChunk{
		{ID: "gen-mock-1", Model: "m", DeltaContent: "Olá"},
		{ID: "gen-mock-1", Model: "m", DeltaContent: ", "},
		{ID: "gen-mock-1", Model: "m", DeltaContent: "mundo", FinishReason: "stop", IsLast: true},
	}
	for _, eol := range []string{"\n", "\r\n", "\r"} {
		stream := bytes.ReplaceAll(raw, []byte("\n"), []byte(eol))
		for cut := 0; cut <= len(stream); cut++ { // o corte cai em todo byte, CRLF partido inclusive
			got, err := decode(t, io.MultiReader(bytes.NewReader(stream[:cut]), bytes.NewReader(stream[cut:])))
			require.NoError(t, err, "eol %q, corte %d", eol, cut)
			require.Equal(t, want, got, "eol %q, corte %d", eol, cut)
		}
		got, err := decode(t, iotest.OneByteReader(bytes.NewReader(stream)))
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestReaderFollowsSpec(t *testing.T) {
	stream := ": OPENROUTER PROCESSING\n\n" + // comentário sozinho não despacha
		"event: x\nid: 7\nretry: 10\ndata:sem espaço\ndata: segunda linha\n\n" +
		"data\n\n" + // campo sem dois-pontos: data vazio, mas despacha
		"event: só nome\n\ndata: y\n\n" + // sem data não despacha, e o nome não vaza para o próximo
		"data: cortado sem a linha vazia"
	want := []sse.Event{{Event: "x", Data: "sem espaço\nsegunda linha"}, {Data: ""}, {Data: "y"}}
	for _, eol := range []string{"\n", "\r\n", "\r"} {
		raw := strings.ReplaceAll(stream, "\n", eol)
		for cut := 0; cut <= len(raw); cut++ {
			r := sse.NewReader(io.MultiReader(strings.NewReader(raw[:cut]), strings.NewReader(raw[cut:])), 64)
			var got []sse.Event
			ev, err := r.Next()
			for ; err == nil; ev, err = r.Next() {
				got = append(got, ev)
			}
			require.ErrorIs(t, err, io.EOF)
			require.Equal(t, want, got, "eol %q, corte %d", eol, cut)
		}
	}
}

func TestLineCeiling(t *testing.T) {
	for _, eol := range []string{"\n", "\r\n", "\r"} {
		line := "data: " + strings.Repeat("a", 58) // 64 bytes: exatamente o teto
		_, err := sse.NewReader(strings.NewReader(line+eol+eol), 64).Next()
		require.NoError(t, err, "eol %q", eol)
		_, err = sse.NewReader(strings.NewReader(line+"a"+eol+eol), 64).Next()
		require.ErrorIs(t, err, sse.ErrLineTooLong, "eol %q", eol)
	}
	// Linhas data dentro do teto, mas um evento que passa dele, e teto degenerado.
	_, err := sse.NewReader(strings.NewReader(strings.Repeat("data: "+strings.Repeat("b", 30)+"\n", 3)+"\n"), 64).Next()
	require.ErrorIs(t, err, sse.ErrLineTooLong)
	for _, max := range []int{-1, 0, math.MaxInt} {
		_, err = sse.NewReader(strings.NewReader("data: x\n\n"), max).Next()
		assert.Equal(t, max <= 0, errors.Is(err, sse.ErrLineTooLong), "teto %d", max)
	}
	// Sem terminador nenhum, o erro vem logo depois do teto, sem ler o resto.
	endless := iotest.OneByteReader(io.LimitReader(repeat('a'), 1<<30))
	_, err = sse.NewReader(endless, 64).Next()
	require.ErrorIs(t, err, sse.ErrLineTooLong)
}

type repeat byte

func (b repeat) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(b)
	}
	return len(p), nil
}

func TestStreamFailures(t *testing.T) {
	ok := `data: {"id":"g","model":"m","choices":[{"delta":{"content":"a"}}]}` + "\n\n"
	for _, tc := range []struct {
		name, stream string
		check        func(t *testing.T, err error)
	}{
		{"erro do provedor no meio", ok + `data: {"id":"g","error":{"code":"server_error","message":"Provider disconnected"},"choices":[{"delta":{"content":""},"finish_reason":"error"}]}` + "\n\n",
			func(t *testing.T, err error) {
				var ge *gwerrors.GatewayError
				require.ErrorAs(t, err, &ge)
				assert.Equal(t, http.StatusBadGateway, ge.HTTPStatus)
				assert.Equal(t, "openrouter: server_error: Provider disconnected", ge.Message)
			}},
		{"fim sem [DONE]", ok, func(t *testing.T, err error) { require.ErrorIs(t, err, io.ErrUnexpectedEOF) }},
		{"evento ilegível", ok + "data: {nada\n\n", func(t *testing.T, err error) { require.ErrorContains(t, err, "ilegível") }},
		{"linha acima do teto", ok + "data: " + strings.Repeat("x", 2<<10) + "\n\n", func(t *testing.T, err error) { require.ErrorIs(t, err, sse.ErrLineTooLong) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decode(t, strings.NewReader(tc.stream))
			tc.check(t, err)
			require.Len(t, got, 1, "o que veio antes da falha é entregue")
			assert.False(t, got[0].IsLast)
		})
	}

	// Pedaço sem texto nem fim (o 1º, só com o role) é pulado; o de usage não tem choices.
	got, err := decode(t, strings.NewReader(`data: {"id":"g","choices":[{"delta":{"role":"assistant","content":""}}]}`+"\n\n"+ok+
		`data: {"id":"g","choices":[],"usage":{"total_tokens":3}}`+"\n\ndata: [DONE]\n\n"))
	require.NoError(t, err)
	assert.Len(t, got, 1)
}

func TestCancelStopsSendAndRead(t *testing.T) {
	for _, tc := range []struct{ name, pending string }{
		{"presa no envio, sem leitor", `data: {"id":"g","choices":[{"delta":{"content":"a"}}]}` + "\n\n"},
		{"presa no Read, sem dado", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pr, pw := io.Pipe()
			ctx, cancel := context.WithCancel(context.Background())
			chunks, errc := sse.Decode(ctx, pr, 1<<10)
			_, err := io.WriteString(pw, tc.pending) // volta quando a goroutine leu
			require.NoError(t, err)
			cancel()
			select { // sem ler os pedaços: só o cancelamento solta a goroutine
			case err = <-errc:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(10 * time.Second):
				t.Fatal("a goroutine não saiu com o cancelamento")
			}
			for range chunks { // fecha no fim
			}
			_, err = pw.Write([]byte("x"))
			require.ErrorIs(t, err, io.ErrClosedPipe, "o body foi fechado")
		})
	}
}
