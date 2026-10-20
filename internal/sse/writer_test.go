package sse_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/middleware"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/sse"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
)

// FuzzAppendChunkMatchesEncodingJSON prova o escape à mão contra o
// encoding/json (SetEscapeHTML(false)) e o evento contra o próprio Reader. As
// sementes, que rodam em todo go test, cobrem o que mais quebra: controles,
// aspas e barra, U+2028/2029, UTF-8 inválido ou cortado e o U+FFFD legítimo.
func FuzzAppendChunkMatchesEncodingJSON(f *testing.F) {
	f.Add("c1", "deepseek-v3", "Olá mundo", "", false, uint64(0))
	f.Add("gen-1", "m", "\"aspas\" e \\barra\\ <html> & 'x'", "stop", true, uint64(7))
	f.Add("\x00\x01\x1f\x7f", "\b\f\n\r\t", "linha\nnova\r\nCR\r", "length", false, uint64(math.MaxUint64))
	f.Add("\u2028", "\u2029", "js \u2028 quebra \u2029", "", true, uint64(1))
	f.Add("0", "\xd8", "\x80", "0", false, uint64(81)) // juntos, \xd8\x80 seria UTF-8 válido
	f.Add("\xff", "a\xc3", "inválido \xed\xa0\x80 e cortado \xe2\x82", "\xc0\xaf", false, uint64(2))
	f.Add("\ufffd", "😀", "emoji 😀 e U+FFFD legítimo \ufffd", "stop", true, uint64(3))
	f.Fuzz(func(t *testing.T, id, model, delta, finish string, last bool, seq uint64) {
		c := domain.StreamChunk{ID: id, Model: model, DeltaContent: delta, FinishReason: finish, IsLast: last}
		var want bytes.Buffer
		enc := json.NewEncoder(&want)
		enc.SetEscapeHTML(false)
		require.NoError(t, enc.Encode(c))

		head := "antes|event: chunk\ndata: "
		if seq > 0 {
			head = "antes|id: " + strconv.FormatUint(seq, 10) + "\nevent: chunk\ndata: "
		}
		ev := sse.AppendChunk([]byte("antes|"), seq, c)
		require.True(t, bytes.HasPrefix(ev, []byte(head)) && bytes.HasSuffix(ev, []byte("\n\n")), "%q", ev)
		data := ev[len(head) : len(ev)-2]
		got, err := sse.NewReader(bytes.NewReader(ev[len("antes|"):]), len(ev)).Next()
		require.NoError(t, err)
		require.Equal(t, sse.Event{Event: "chunk", Data: string(data)}, got, "um evento, uma linha data")

		if utf8.ValidString(id) && utf8.ValidString(model) && utf8.ValidString(delta) && utf8.ValidString(finish) {
			require.Equal(t, strings.TrimSuffix(want.String(), "\n"), string(data))
			return
		}
		// UTF-8 inválido: o Go 1.26 (encoding/json v1) escreve \ufffd e o 1.27
		// (sobre o JSONv2) o U+FFFD cru. Os bytes divergem; o valor, não.
		var mine, theirs domain.StreamChunk
		require.NoError(t, json.Unmarshal(data, &mine))
		require.NoError(t, json.Unmarshal(want.Bytes(), &theirs))
		require.Equal(t, theirs, mine)
	})
}

func TestAppendChunkIsWhatTheStudioParses(t *testing.T) {
	// A mesma linha do tests/sseTypes.test.ts do agent-studio-ts.
	ev := sse.AppendChunk(nil, 0, domain.StreamChunk{ID: "c1", Model: "deepseek-v3", DeltaContent: "Olá mundo"})
	assert.Equal(t, "event: chunk\n"+`data: {"id":"c1","model":"deepseek-v3","delta_content":"Olá mundo","is_last":false}`+"\n\n", string(ev))
}

// recorder conta Writes e Flushes; err faz o Write falhar.
type recorder struct {
	header  http.Header
	writes  []string
	flushes int
	err     error
}

func (r *recorder) Header() http.Header { return r.header }
func (r *recorder) WriteHeader(int)     {}
func (r *recorder) Flush()              { r.flushes++ }
func (r *recorder) Write(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	r.writes = append(r.writes, string(p))
	return len(p), nil
}

func TestWriterOneWriteAndOneFlushPerEvent(t *testing.T) {
	rec := &recorder{header: http.Header{}}
	w := sse.NewWriter(rec)
	defer w.Close()
	assert.Equal(t, "text/event-stream", rec.header.Get("Content-Type"))
	assert.Equal(t, "no-cache", rec.header.Get("Cache-Control"))

	first := domain.StreamChunk{ID: "c1", Model: "m", DeltaContent: "um pedaço mais longo que o seguinte"}
	c := domain.StreamChunk{ID: "c1", Model: "m", DeltaContent: "<b>oi</b>", FinishReason: "stop", IsLast: true}
	require.NoError(t, w.Chunk(4, first))
	require.NoError(t, w.Chunk(5, c))
	require.NoError(t, w.Event("done", map[string]any{"finish_reason": "<stop>&", "total_tokens": 3, "duration_ms": 1.5}))
	assert.Equal(t, []string{
		string(sse.AppendChunk(nil, 4, first)),
		string(sse.AppendChunk(nil, 5, c)), // nada do evento anterior no buffer
		"event: done\n" + `data: {"duration_ms":1.5,"finish_reason":"<stop>&","total_tokens":3}` + "\n\n", // sem escape de HTML, como o Chunk
	}, rec.writes)
	assert.Equal(t, 3, rec.flushes)

	for _, name := range []string{"", "done\ndata: falso", "x\r"} {
		assert.Error(t, w.Event(name, 1), "%q", name)
	}
	assert.Error(t, w.Event("metadata", math.NaN()), "JSON impossível não sai")
	assert.Len(t, rec.writes, 3, "evento recusado não escreve nada")

	rec.err = errors.New("cliente foi embora")
	assert.ErrorIs(t, w.Chunk(6, c), rec.err)
	assert.Equal(t, 3, rec.flushes, "sem Flush depois de Write que falhou")
}

func TestWriterWithoutFlusherFails(t *testing.T) {
	w := sse.NewWriter(struct{ http.ResponseWriter }{httptest.NewRecorder()}) // esconde o Flush
	defer w.Close()
	assert.ErrorIs(t, w.Chunk(1, domain.StreamChunk{}), http.ErrNotSupported)
}

// discard é o ResponseWriter mais barato possível: mede só o Writer.
type discard struct{}

func (discard) Header() http.Header         { return http.Header{} }
func (discard) WriteHeader(int)             {}
func (discard) Flush()                      {}
func (discard) Write(p []byte) (int, error) { return len(p), nil }

var hot = domain.StreamChunk{ID: "gen-1729000000-AbCdEfGh", Model: "deepseek/deepseek-chat-v3", DeltaContent: " fluxo \"quente\"\n com ç e \u2028"}

func TestChunkDoesNotAllocate(t *testing.T) {
	w := sse.NewWriter(discard{})
	defer w.Close()
	require.NoError(t, w.Chunk(1, hot)) // o buffer do pool já cabe o evento
	assert.Zero(t, testing.AllocsPerRun(200, func() { _ = w.Chunk(2, hot) }))
}

// TestWriterStreamsThroughCompressMiddleware sobe o servidor de verdade com o
// middleware de compressão do gateway: o evento chega ao cliente antes de o
// handler escrever o próximo (Flush passa pelo gzhttp) e sem Content-Encoding.
func TestWriterStreamsThroughCompressMiddleware(t *testing.T) {
	read := make(chan struct{}, 1)
	srv := httptest.NewServer(middleware.Compress()(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w := sse.NewWriter(rw)
		defer w.Close()
		_ = w.Chunk(1, domain.StreamChunk{ID: "c1", Model: "m", DeltaContent: "oi"})
		select {
		case <-read:
		case <-r.Context().Done():
			return
		}
		_ = w.Event("done", map[string]string{"finish_reason": "stop"})
	})))
	defer srv.Close()
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept-Encoding", "gzip, zstd")
	res, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	assert.Empty(t, res.Header.Get("Content-Encoding"))
	assert.Equal(t, "text/event-stream", res.Header.Get("Content-Type"))

	r := sse.NewReader(res.Body, 1<<16)
	ev, err := r.Next()
	require.NoError(t, err, "o primeiro evento chega sozinho")
	assert.Equal(t, sse.Event{Event: "chunk", Data: `{"id":"c1","model":"m","delta_content":"oi","is_last":false}`}, ev)
	read <- struct{}{}
	ev, err = r.Next()
	require.NoError(t, err)
	assert.Equal(t, sse.Event{Event: "done", Data: `{"finish_reason":"stop"}`}, ev)
}

func BenchmarkWriterChunk(b *testing.B) {
	w := sse.NewWriter(discard{})
	defer w.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = w.Chunk(uint64(i+1), hot)
	}
}

// BenchmarkSSEEventFormat é a alternativa que já existe no repo
// (domain.SSEEvent.Format: encoding/json + fmt), para comparar.
func BenchmarkSSEEventFormat(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ev := domain.SSEEvent{ID: strconv.Itoa(i + 1), Event: "chunk", Data: hot}
		out, _ := ev.Format()
		_, _ = discard{}.Write(out)
	}
}
