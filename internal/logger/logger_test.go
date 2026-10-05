package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func newJSON(t *testing.T, level string) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	l, err := New(&buf, level, "json")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return l, &buf
}

func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("saída não é JSON (%v): %s", err, buf)
	}
	return m
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, "trace", "json"); err == nil {
		t.Error("esperava erro para nível inválido")
	}
	if _, err := New(&bytes.Buffer{}, "info", "xml"); err == nil {
		t.Error("esperava erro para formato inválido")
	}
}

func TestLevelFiltering(t *testing.T) {
	l, buf := newJSON(t, "warn")
	l.Info("descartado")
	if buf.Len() != 0 {
		t.Fatalf("info não deveria sair em nível warn: %s", buf)
	}
	l.Warn("emitido")
	if decode(t, buf)["level"] != "WARN" {
		t.Errorf("nível errado: %s", buf)
	}
}

func TestTraceIDPropagation(t *testing.T) {
	l, buf := newJSON(t, "info")
	ctx := WithTraceID(context.Background(), "abc123")

	l.With("component", "proxy").InfoContext(ctx, "requisição", "status", 200)
	got := decode(t, buf)
	if got[TraceIDKey] != "abc123" || got["component"] != "proxy" || got["status"] != float64(200) {
		t.Errorf("campos estruturados ausentes: %v", got)
	}

	buf.Reset()
	l.Info("sem contexto")
	if _, ok := decode(t, buf)[TraceIDKey]; ok {
		t.Errorf("trace_id não deveria aparecer sem contexto: %s", buf)
	}
}

func TestRedactsSecrets(t *testing.T) {
	l, buf := newJSON(t, "info")
	l.Info("upstream",
		"api_key", "sk-or-123",
		"Authorization", "Bearer xyz",
		"max_tokens", 1024,
		slog.Group("openrouter", "apiKey", "sk-or-456", "base_url", "https://openrouter.ai"),
	)
	out := buf.String()
	for _, secret := range []string{"sk-or-123", "Bearer xyz", "sk-or-456"} {
		if strings.Contains(out, secret) {
			t.Errorf("segredo %q vazou: %s", secret, out)
		}
	}
	got := decode(t, buf)
	if got["max_tokens"] != float64(1024) {
		t.Errorf("max_tokens não deveria ser mascarado: %v", got)
	}
	if got["openrouter"].(map[string]any)["base_url"] != "https://openrouter.ai" {
		t.Errorf("campo não sensível do grupo foi alterado: %v", got)
	}
}

func TestIsSensitive(t *testing.T) {
	for key, want := range map[string]bool{
		"password": true, "DB_PASSWORD": true, "client-secret": true, "access_token": true,
		"API-KEY": true, "set_cookie": true, "max_tokens": false, "tokens_used": false, "model": false,
	} {
		if got := IsSensitive(key); got != want {
			t.Errorf("IsSensitive(%q) = %v, want %v", key, got, want)
		}
	}
}

func TestNewTraceID(t *testing.T) {
	a, b := NewTraceID(), NewTraceID()
	if len(a) != 32 || strings.Trim(a, "0123456789abcdef") != "" {
		t.Errorf("trace_id fora do formato hex de 32 caracteres: %q", a)
	}
	if a == b {
		t.Error("dois trace_ids iguais")
	}
}

func TestNewTraceIDFallbackQuandoRandFalha(t *testing.T) {
	orig := randRead
	t.Cleanup(func() { randRead = orig })
	randRead = func([]byte) (int, error) { return 0, errors.New("sem entropia") }

	a, b := NewTraceID(), NewTraceID()
	if len(a) != 32 || a == strings.Repeat("0", 32) {
		t.Errorf("fallback devolveu trace_id inválido: %q", a)
	}
	if a == b {
		t.Error("fallback gerou dois trace_ids iguais")
	}
}
