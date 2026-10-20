package sse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/buffer"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
)

// Writer escreve o SSE do gateway direto no http.ResponseWriter, no formato
// que o studio lê (src/types/sse.ts do agent-studio-ts): "event: chunk" com o
// JSON do domain.StreamChunk, e eventos raros (done, error, metadata) com o
// JSON que vier. Cada evento sai num Write só, seguido de Flush, montado num
// buffer do pkg/buffer que serve a todos os eventos do stream. O pedaço de
// texto (Chunk) é serializado à mão e não aloca. Como o ResponseWriter, não é
// para uso concorrente.
type Writer struct {
	w   http.ResponseWriter
	rc  *http.ResponseController
	buf *bytes.Buffer
}

// NewWriter põe os cabeçalhos do SSE, que têm de vir antes do primeiro Write:
// sem o Content-Type, o middleware de compressão detecta text/plain e segura
// os eventos até o limiar. Close devolve o buffer ao pool.
func NewWriter(w http.ResponseWriter) *Writer {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	return &Writer{w: w, rc: http.NewResponseController(w), buf: buffer.GetBytes()}
}

// Chunk escreve o pedaço como evento "chunk" e dá Flush. seq vira a linha id
// (o Seq do internal/ringbuffer, para retomar por Last-Event-ID); 0 = sem id.
// Depois que o buffer cresce até o tamanho dos eventos, não aloca.
func (w *Writer) Chunk(seq uint64, c domain.StreamChunk) error {
	w.buf.Reset()
	w.buf.Write(AppendChunk(w.buf.AvailableBuffer(), seq, c))
	return w.send()
}

// Event escreve o evento name com v em JSON (encoding/json, que aloca: é para
// os eventos de uma vez por stream) e dá Flush. Nome vazio ou com quebra de
// linha é recusado, porque a quebra viraria outro campo do SSE.
func (w *Writer) Event(name string, v any) error {
	if name == "" || strings.ContainsAny(name, "\r\n") {
		return fmt.Errorf("sse: nome de evento inválido %q", name)
	}
	w.buf.Reset()
	w.buf.WriteString("event: ")
	w.buf.WriteString(name)
	w.buf.WriteString("\ndata: ")
	enc := json.NewEncoder(w.buf)
	enc.SetEscapeHTML(false) // o mesmo escape do Chunk
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("sse: serializando o evento %s: %w", name, err)
	}
	w.buf.WriteByte('\n') // o Encode já pôs o fim da linha data; esta é a linha vazia
	return w.send()
}

func (w *Writer) send() error {
	if _, err := w.w.Write(w.buf.Bytes()); err != nil {
		return err
	}
	return w.rc.Flush()
}

// Close devolve o buffer ao pkg/buffer; o Writer não serve mais depois dele.
func (w *Writer) Close() {
	buffer.PutBytes(w.buf)
	w.buf = nil
}

// AppendChunk acrescenta a dst o evento SSE inteiro do pedaço: a linha id (se
// seq > 0), "event: chunk" e a linha data com o JSON que o encoding/json gera
// para domain.StreamChunk com SetEscapeHTML(false), byte a byte com UTF-8
// válido. O escape garante uma linha só: nada de \n ou \r cru no JSON.
func AppendChunk(dst []byte, seq uint64, c domain.StreamChunk) []byte {
	if seq > 0 {
		dst = append(dst, "id: "...)
		dst = strconv.AppendUint(dst, seq, 10)
		dst = append(dst, '\n')
	}
	dst = append(dst, "event: chunk\ndata: {\"id\":"...)
	dst = appendString(dst, c.ID)
	dst = append(dst, `,"model":`...)
	dst = appendString(dst, c.Model)
	dst = append(dst, `,"delta_content":`...)
	dst = appendString(dst, c.DeltaContent)
	if c.FinishReason != "" { // omitempty
		dst = append(dst, `,"finish_reason":`...)
		dst = appendString(dst, c.FinishReason)
	}
	dst = append(dst, `,"is_last":`...)
	dst = strconv.AppendBool(dst, c.IsLast)
	return append(dst, "}\n\n"...)
}

const hexDigits = "0123456789abcdef"

// appendString acrescenta s como string JSON com as regras do encoding/json
// sem escape de HTML: \" e \\, \b \f \n \r \t, \u00XX nos outros controles,
// U+2028 e U+2029 escapados (quebram JavaScript que avalia o JSON) e cada
// byte de UTF-8 inválido como \ufffd, como o encoding/json v1 (o do Go 1.27,
// sobre o JSONv2, põe o U+FFFD cru: os bytes mudam, o valor é o mesmo).
func appendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '"', '\\':
				dst = append(dst, '\\', b)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xF])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			dst = append(append(dst, s[start:i]...), `\ufffd`...)
			start = i + size
		case r == '\u2028' || r == '\u2029':
			dst = append(append(dst, s[start:i]...), '\\', 'u', '2', '0', '2', hexDigits[r&0xF])
			start = i + size
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
