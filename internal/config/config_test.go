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
	if o := cfg.OpenRouter; o.ResponseHeaderTimeout != 60*time.Second || o.MaxIdleConnsPerHost != 64 || o.MaxConnsPerHost != 0 {
		t.Errorf("pool do upstream inesperado: %+v", o)
	}
	if cfg.Cache.SocketPath != "/tmp/semantic-cache.sock" || cfg.Cache.SocketOwnerUID != -1 {
		t.Errorf("cache inesperado: %+v", cfg.Cache)
	}
}

func TestMetricsCredentials(t *testing.T) {
	if cfg, err := load(t, "--env-file", ""); err != nil || cfg.Server.MetricsUsername != "" || cfg.Server.MetricsPassword != "" {
		t.Fatalf("sem padrão embutido: %+v %v", cfg, err)
	}
	t.Setenv("FLOW_SERVER_METRICS_USERNAME", "prom")
	if _, err := load(t, "--env-file", ""); err == nil || !strings.Contains(err.Error(), "server.metrics_password") {
		t.Fatalf("só o usuário deveria falhar, veio %v", err)
	}
	t.Setenv("FLOW_SERVER_METRICS_PASSWORD", "s3gredo-bem-longo")
	cfg, err := load(t, "--env-file", "")
	if err != nil || cfg.Server.MetricsUsername != "prom" || cfg.Server.MetricsPassword != "s3gredo-bem-longo" {
		t.Fatalf("credencial do ambiente: %v", err)
	}
	t.Setenv("FLOW_SERVER_METRICS_USERNAME", "pr:om")
	if _, err := load(t, "--env-file", ""); err == nil || !strings.Contains(err.Error(), "':'") || strings.Contains(err.Error(), "s3gredo") {
		t.Fatalf("usuário com ':' deveria falhar sem ecoar a senha, veio %v", err)
	}
}

func TestPprofConfig(t *testing.T) {
	cfg, err := load(t, "--env-file", "")
	if err != nil || cfg.Server.PprofEnabled || cfg.Server.PprofAddr != "127.0.0.1:6060" {
		t.Fatalf("o pprof nasce desligado e em 127.0.0.1:6060: %+v %v", cfg.Server, err)
	}
	t.Setenv("FLOW_SERVER_PPROF_ENABLED", "true")
	for addr, ok := range map[string]bool{"127.0.0.1:6060": true, "[::1]:6060": true, ":6060": false,
		"0.0.0.0:6060": false, "localhost:6060": false, "10.0.0.5:6060": false, "sem-porta": false} {
		t.Setenv("FLOW_SERVER_PPROF_ADDR", addr)
		if _, err := load(t, "--env-file", ""); (err == nil) != ok {
			t.Errorf("%s: erro %v", addr, err)
		}
	}
	t.Setenv("FLOW_SERVER_PPROF_ADDR", "0.0.0.0:6060")
	t.Setenv("FLOW_SERVER_METRICS_USERNAME", "prom")
	t.Setenv("FLOW_SERVER_METRICS_PASSWORD", "s3gredo-bem-longo")
	if _, err := load(t, "--env-file", ""); err != nil {
		t.Fatalf("fora do loopback com a basic auth: %v", err)
	}
}

func TestCacheSocketPathRequired(t *testing.T) {
	yaml := writeFile(t, "config.yaml", "cache:\n  socket_path: \"\"\n")
	if _, err := load(t, "--config", yaml, "--env-file", ""); err == nil || !strings.Contains(err.Error(), "cache.socket_path") {
		t.Fatalf("esperava erro de cache.socket_path vazio, veio %v", err)
	}
	t.Setenv("FLOW_CACHE_SOCKET_OWNER_UID", "-2")
	if _, err := load(t, "--env-file", ""); err == nil || !strings.Contains(err.Error(), "cache.socket_owner_uid") {
		t.Fatalf("esperava erro de cache.socket_owner_uid -2, veio %v", err)
	}
	t.Setenv("FLOW_CACHE_SOCKET_OWNER_UID", "998") // produção com usuários separados
	if cfg, err := load(t, "--env-file", ""); err != nil || cfg.Cache.SocketOwnerUID != 998 {
		t.Fatalf("socket_owner_uid pelo ambiente: %+v, %v", cfg, err)
	}
}

func TestLoadUpstreamPoolFromEnv(t *testing.T) {
	t.Setenv("FLOW_OPENROUTER_RESPONSE_HEADER_TIMEOUT", "2m")
	t.Setenv("FLOW_OPENROUTER_MAX_CONNS_PER_HOST", "128")
	cfg, err := load(t, "--env-file", "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OpenRouter.ResponseHeaderTimeout != 2*time.Minute || cfg.OpenRouter.MaxConnsPerHost != 128 {
		t.Errorf("o ambiente não chegou ao pool: %+v", cfg.OpenRouter)
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

func TestLoadTrustedProxiesFromEnv(t *testing.T) {
	t.Setenv("FLOW_SERVER_TRUSTED_PROXIES", "10.0.0.0/8,192.168.1.1")
	cfg, err := load(t, "--env-file", "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := strings.Join(cfg.Server.TrustedProxies, "|"); got != "10.0.0.0/8|192.168.1.1" {
		t.Errorf("trusted_proxies = %q", got)
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
		{"dial do upstream zerado", nil, map[string]string{"FLOW_OPENROUTER_DIAL_TIMEOUT": "0s"}, "openrouter.dial_timeout"},
		{"pool sem conexão ociosa", nil, map[string]string{"FLOW_OPENROUTER_MAX_IDLE_CONNS_PER_HOST": "0"}, "max_idle_conns_per_host"},
		{"teto de conexões negativo", nil, map[string]string{"FLOW_OPENROUTER_MAX_CONNS_PER_HOST": "-1"}, "max_conns_per_host"},
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
