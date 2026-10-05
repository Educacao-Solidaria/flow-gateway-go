// Package logger monta o *slog.Logger do gateway: saída estruturada (JSON ou
// texto), trace_id propagado pelo context.Context e mascaramento de segredos.
package logger

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"strings"
)

// TraceIDKey é o nome do campo que carrega o trace_id em cada registro.
const TraceIDKey = "trace_id"

// Redacted substitui o valor de qualquer atributo sensível.
const Redacted = "[REDACTED]"

// sensitiveSuffixes indicam segredo quando terminam o nome do atributo. A
// comparação ignora caixa e separadores ("apiKey", "api_key" e "API-KEY"
// casam) e é por sufixo para não mascarar métricas como "max_tokens".
var sensitiveSuffixes = []string{"password", "passwd", "secret", "token", "apikey", "authorization", "cookie"}

type traceIDKey struct{}

// WithTraceID devolve um contexto que carrega id como trace_id.
func WithTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, id)
}

// TraceID devolve o trace_id do contexto, ou "" se não houver.
func TraceID(ctx context.Context) string {
	id, _ := ctx.Value(traceIDKey{}).(string)
	return id
}

// randRead é a fonte de entropia do trace_id; variável para o teste simular falha.
var randRead = rand.Read

// NewTraceID gera um trace_id aleatório de 128 bits em hex (formato W3C).
func NewTraceID() string {
	var b [16]byte
	if _, err := randRead(b[:]); err != nil {
		// Antes do Go 1.24 crypto/rand.Read pode falhar (o go.mod aceita 1.23).
		// trace_id não é segredo: math/rand/v2 basta para não devolver zeros.
		binary.LittleEndian.PutUint64(b[:8], mrand.Uint64())
		binary.LittleEndian.PutUint64(b[8:], mrand.Uint64())
	}
	return hex.EncodeToString(b[:])
}

// New cria um logger que escreve em w. level aceita debug, info, warn e
// error; format aceita json e text.
func New(w io.Writer, level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("logger: nível %q inválido: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl, ReplaceAttr: redact}

	var h slog.Handler
	switch format {
	case "json":
		h = slog.NewJSONHandler(w, opts)
	case "text":
		h = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("logger: formato %q inválido (json|text)", format)
	}
	return slog.New(traceHandler{h}), nil
}

// IsSensitive informa se um atributo com esse nome deve ser mascarado.
func IsSensitive(key string) bool {
	k := strings.NewReplacer("_", "", "-", "", ".", "").Replace(strings.ToLower(key))
	for _, s := range sensitiveSuffixes {
		if strings.HasSuffix(k, s) {
			return true
		}
	}
	return false
}

func redact(_ []string, a slog.Attr) slog.Attr {
	if a.Value.Kind() != slog.KindGroup && IsSensitive(a.Key) {
		return slog.String(a.Key, Redacted)
	}
	return a
}

// traceHandler acrescenta o trace_id do contexto a cada registro.
// ponytail: depois de WithGroup o trace_id sai dentro do grupo, como qualquer
// atributo do slog; se virar problema, mover o campo para um handler próprio.
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := TraceID(ctx); id != "" {
		r.AddAttrs(slog.String(TraceIDKey, id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}
