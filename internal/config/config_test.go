package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func load(t *testing.T, args ...string) (*Config, error) {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse: %v", err)
	}
	return Load(fs)
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(t, "--env-file", filepath.Join(t.TempDir(), "ausente.env"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Env != "development" || cfg.Server.Addr != ":8080" || cfg.Log.Format != "json" {
		t.Errorf("padrões inesperados: %+v", cfg)
	}
	if cfg.Server.ShutdownTimeout != 15*time.Second || cfg.Server.WriteTimeout != 0 {
		t.Errorf("timeouts inesperados: %+v", cfg.Server)
	}
}

func TestLoadPrecedence(t *testing.T) {
	yaml := writeFile(t, "config.yaml", `
server:
  addr: ":1000"
  shutdown_timeout: 30s
log:
  level: debug
  format: text
`)
	dotenv := writeFile(t, ".env", "FLOW_SERVER_ADDR=:2000\nFLOW_LOG_LEVEL=warn\nFLOW_SERVER_SHUTDOWN_TIMEOUT=5s\n")
	t.Setenv("FLOW_LOG_LEVEL", "error")

	cfg, err := load(t, "--config", yaml, "--env-file", dotenv, "--server.addr", ":3000")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"flag vence env, .env e arquivo", cfg.Server.Addr, ":3000"},
		{"env vence .env e arquivo", cfg.Log.Level, "error"},
		{".env vence arquivo", cfg.Server.ShutdownTimeout, 5 * time.Second},
		{"arquivo vence padrão", cfg.Log.Format, "text"},
		{"padrão quando ninguém define", cfg.Server.ReadTimeout, 15 * time.Second},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadRejectsUnknownKey(t *testing.T) {
	yaml := writeFile(t, "config.yaml", "server:\n  adress: \":9000\"\n")
	_, err := load(t, "--config", yaml, "--env-file", "")
	if err == nil || !strings.Contains(err.Error(), "adress") {
		t.Fatalf("esperava erro citando a chave desconhecida, veio %v", err)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"nível de log inválido", []string{"--log.level", "trace"}, nil, "log.level"},
		{"formato inválido", []string{"--log.format", "xml"}, nil, "log.format"},
		{"production sem api key", []string{"--env", "production"}, nil, "openrouter.api_key"},
		{"base url sem esquema", nil, map[string]string{"FLOW_OPENROUTER_BASE_URL": "openrouter.ai"}, "base_url"},
		{"shutdown zerado", []string{"--server.shutdown_timeout", "0s"}, nil, "shutdown_timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			_, err := load(t, append(tt.args, "--env-file", "")...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("esperava erro contendo %q, veio %v", tt.want, err)
			}
		})
	}
}

func TestProductionWithAPIKey(t *testing.T) {
	t.Setenv("FLOW_OPENROUTER_API_KEY", "sk-or-test")
	cfg, err := load(t, "--env", "production", "--env-file", "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OpenRouter.APIKey != "sk-or-test" {
		t.Errorf("api key não veio do ambiente: %q", cfg.OpenRouter.APIKey)
	}
}
