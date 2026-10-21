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

func TestRunWithPprof(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t.Setenv("FLOW_SERVER_PPROF_ENABLED", "true")
	t.Setenv("FLOW_SERVER_PPROF_ADDR", "127.0.0.1:0")
	var out bytes.Buffer
	if err := run(ctx, []string{"--env-file", "", "--server.addr", "127.0.0.1:0"}, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	if n := strings.Count(out.String(), `"msg":"servidor encerrado"`); n != 2 {
		t.Errorf("os dois servidores deveriam subir e encerrar, %d encerraram: %s", n, out.String())
	}
}

// TestRunPprofListenError: o admin que não sobe derruba o público (ctx sem
// cancelamento: sem isso o teste pendura) e o erro chega a quem chamou.
func TestRunPprofListenError(t *testing.T) {
	t.Setenv("FLOW_SERVER_PPROF_ENABLED", "true")
	t.Setenv("FLOW_SERVER_PPROF_ADDR", "127.0.0.1:99999")
	err := run(context.Background(), []string{"--env-file", "", "--server.addr", "127.0.0.1:0"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "listen 127.0.0.1:99999") {
		t.Fatalf("esperava o erro de listen do pprof, veio %v", err)
	}
}

func TestRunListenError(t *testing.T) {
	err := run(context.Background(), []string{"--env-file", "", "--server.addr", "256.0.0.1:1"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("esperava erro de listen, veio %v", err)
	}
}
