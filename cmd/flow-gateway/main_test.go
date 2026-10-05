package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"--version"}, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.HasPrefix(out.String(), "flow-gateway ") {
		t.Errorf("saída inesperada: %q", out.String())
	}
}

func TestRunUnknownFlag(t *testing.T) {
	if err := run(context.Background(), []string{"--nope"}, io.Discard); err == nil {
		t.Fatal("esperava erro para flag desconhecida")
	}
}

func TestRunInvalidConfig(t *testing.T) {
	err := run(context.Background(), []string{"--env-file", "", "--log.level", "trace"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "log.level") {
		t.Fatalf("esperava erro de validação, veio %v", err)
	}
}

func TestRunStartsAndShutsDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // sobe e encerra em seguida: exercita o caminho completo do shutdown

	var out bytes.Buffer
	args := []string{"--env-file", "", "--log.format", "json", "--server.addr", "127.0.0.1:0"}
	if err := run(ctx, args, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, msg := range []string{"configuração carregada", "servidor http no ar", "servidor encerrado"} {
		if !strings.Contains(out.String(), `"msg":"`+msg+`"`) {
			t.Errorf("log %q ausente: %s", msg, out.String())
		}
	}
}

func TestRunListenError(t *testing.T) {
	err := run(context.Background(), []string{"--env-file", "", "--server.addr", "256.0.0.1:1"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("esperava erro de listen, veio %v", err)
	}
}
