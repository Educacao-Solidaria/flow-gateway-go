package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"--version"}, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.HasPrefix(out.String(), "flow-gateway ") {
		t.Errorf("saída inesperada: %q", out.String())
	}
}

func TestRunUnknownFlag(t *testing.T) {
	if err := run([]string{"--nope"}, &bytes.Buffer{}); err == nil {
		t.Fatal("esperava erro para flag desconhecida")
	}
}

func TestRunInvalidConfig(t *testing.T) {
	err := run([]string{"--env-file", "", "--log.level", "trace"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "log.level") {
		t.Fatalf("esperava erro de validação, veio %v", err)
	}
}
