package logger

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/benchutil"
)

// BenchmarkLogger compara o slog puro com o logger do gateway (trace_id do
// contexto + mascaramento por nome de atributo) no mesmo registro de requisição.
func BenchmarkLogger(b *testing.B) {
	plain := slog.New(slog.NewJSONHandler(io.Discard, nil))
	gw, err := New(io.Discard, "info", "json")
	if err != nil {
		b.Fatal(err)
	}
	ctx := WithTraceID(context.Background(), NewTraceID())
	logWith := func(l *slog.Logger) func() {
		return func() {
			l.InfoContext(ctx, "requisição http", "method", "POST", "path", "/v1/chat/completions",
				"status", 200, "api_key", "sk-or-v1-segredo", "max_tokens", 512)
		}
	}
	benchutil.Compare(b, 0,
		benchutil.Variant{Name: "slog-json", Fn: logWith(plain)},
		benchutil.Variant{Name: "gateway", Fn: logWith(gw)},
	)
}

// TestIsSensitiveAllocs trava a regressão que o BenchmarkLogger expôs:
// IsSensitive roda para todo atributo de todo registro e deve custar no máximo
// o ToLower e o Replace da chave.
func TestIsSensitiveAllocs(t *testing.T) {
	if n := testing.AllocsPerRun(100, func() { _ = IsSensitive("Max_Tokens") }); n > 2 {
		t.Errorf("IsSensitive aloca %.0f vezes por chamada, máximo 2", n)
	}
}
