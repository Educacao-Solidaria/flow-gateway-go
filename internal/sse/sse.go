// Package sse lê o text/event-stream da OpenRouter e escreve o do gateway.
// Reader separa os eventos como manda a spec do WHATWG (linhas em LF, CRLF ou
// CR sozinho; comentários; data em várias linhas), com teto por linha; Decode
// entrega os chat.completion.chunk no contrato de stream do pkg/openrouter; e
// Writer (writer.go) manda os eventos ao studio, um Flush por evento.
package sse

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

// ErrLineTooLong é uma linha (ou a soma das linhas data de um evento) acima do
// teto: o stream é recusado em vez de o buffer crescer sem limite.
var ErrLineTooLong = errors.New("sse: linha ou evento acima do teto")

// Event é um evento despachado: os campos data juntados por "\n".
type Event struct {
	Event string
	Data  string
}

// Reader lê eventos de um text/event-stream.
type Reader struct {
	s   *bufio.Scanner
	max int
}

// NewReader lê de r com teto de maxLine bytes (fora do terminador) por linha e
// pela soma das linhas data de um evento; maxLine fica entre 0 e 1 GiB.
func NewReader(r io.Reader, maxLine int) *Reader {
	maxLine = min(max(maxLine, 0), 1<<30)
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, min(maxLine+2, 4096)), maxLine+2) // +2: o CRLF
	s.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		i := bytes.IndexAny(data, "\r\n")
		switch {
		case i > maxLine || i < 0 && len(data) > maxLine:
			return 0, nil, ErrLineTooLong
		case i < 0:
			if atEOF && len(data) > 0 {
				return len(data), data, nil
			}
			return 0, nil, nil
		case data[i] == '\n':
			return i + 1, data[:i], nil
		case i+1 < len(data) && data[i+1] == '\n':
			return i + 2, data[:i], nil
		case i+1 < len(data) || atEOF:
			return i + 1, data[:i], nil
		}
		return 0, nil, nil // CR no fim do que chegou: pode ser a metade de um CRLF
	})
	return &Reader{s: s, max: maxLine}
}

// Next devolve o próximo evento com data. Comentários (": ...") e campos que
// não são data nem event são ignorados; linha vazia despacha. No fim do
// stream, io.EOF — e o evento cortado sem a linha vazia é descartado.
func (r *Reader) Next() (Event, error) {
	var ev Event
	var data strings.Builder
	has := false
	for r.s.Scan() {
		line := r.s.Text()
		if line == "" {
			if has {
				ev.Data = data.String()
				return ev, nil
			}
			ev = Event{}
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "data":
			if has {
				data.WriteByte('\n')
			}
			data.WriteString(value)
			has = true
			if data.Len() > r.max { // muitas linhas data sem a linha vazia
				return Event{}, ErrLineTooLong
			}
		case "event":
			ev.Event = value
		}
	}
	if err := r.s.Err(); err != nil {
		return Event{}, err
	}
	return Event{}, io.EOF
}

// chunk é o chat.completion.chunk da OpenRouter. Error vem quando o provedor
// falha depois de o stream ter começado (o status HTTP já foi 200).
type chunk struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Error *struct {
		Code    any    `json:"code"` // número ou texto, conforme o provedor
		Message string `json:"message"`
	} `json:"error"`
}

// Decode lê de body o stream de chat completions da OpenRouter e o entrega
// como o openrouter.Client.StreamChatCompletion: os pedaços em ordem (o que
// traz finish_reason com IsLast) e, no canal de erro (buffer 1; os dois
// fecham no fim), a falha — erro do provedor no meio do stream, evento
// ilegível, linha acima de maxLine, fim sem o [DONE] ou ctx.Err(). Fecha body
// no fim (Close concorrente com Read, como o de uma resposta HTTP); com o ctx
// cancelado, a goroutine sai mesmo presa num Read ou num envio sem leitor.
func Decode(ctx context.Context, body io.ReadCloser, maxLine int) (<-chan domain.StreamChunk, <-chan error) {
	out := make(chan domain.StreamChunk)
	errc := make(chan error, 1)
	stop := context.AfterFunc(ctx, func() { _ = body.Close() })
	go func() {
		defer close(out)
		defer close(errc)
		defer body.Close()
		defer stop()
		if err := decode(ctx, NewReader(body, maxLine), out); err != nil {
			if ctx.Err() != nil {
				err = ctx.Err() // a leitura caiu porque o cancelamento fechou o body
			}
			errc <- err
		}
	}()
	return out, errc
}

func decode(ctx context.Context, r *Reader, out chan<- domain.StreamChunk) error {
	for {
		ev, err := r.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("sse: stream terminou sem o [DONE]: %w", io.ErrUnexpectedEOF)
		}
		if err != nil {
			return err
		}
		if ev.Data == "[DONE]" {
			return nil
		}
		var c chunk
		if err := json.Unmarshal([]byte(ev.Data), &c); err != nil {
			return fmt.Errorf("sse: evento ilegível: %w", err)
		}
		if c.Error != nil {
			return gwerrors.New("UPSTREAM_STREAM_ERROR", fmt.Sprintf("openrouter: %v: %s", c.Error.Code, c.Error.Message), http.StatusBadGateway, 0)
		}
		for _, ch := range c.Choices {
			if ch.Delta.Content == "" && ch.FinishReason == "" {
				continue // pedaço sem texto nem fim (ex.: o do usage)
			}
			sc := domain.StreamChunk{ID: c.ID, Model: c.Model, DeltaContent: ch.Delta.Content, FinishReason: ch.FinishReason, IsLast: ch.FinishReason != ""}
			select {
			case out <- sc:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
