// Package config carrega a configuração tipada do gateway.
//
// Precedência, da maior para a menor: flags > variáveis de ambiente > .env >
// arquivo YAML > padrões. Toda chave pode vir do ambiente como
// FLOW_<SEÇÃO>_<CHAVE> (ex.: server.addr -> FLOW_SERVER_ADDR).
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const envPrefix = "FLOW"

// Config é a configuração completa do gateway.
type Config struct {
	Env        string           `mapstructure:"env"`
	Server     ServerConfig     `mapstructure:"server"`
	Log        LogConfig        `mapstructure:"log"`
	OpenRouter OpenRouterConfig `mapstructure:"openrouter"`
	Cache      CacheConfig      `mapstructure:"cache"`
}

// ServerConfig configura o servidor HTTP.
type ServerConfig struct {
	Addr        string        `mapstructure:"addr"`
	ReadTimeout time.Duration `mapstructure:"read_timeout"`
	// WriteTimeout 0 desliga o limite: streams SSE são longos por natureza.
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	// HealthTimeout limita cada rodada de probes do /healthz.
	HealthTimeout time.Duration `mapstructure:"health_timeout"`
	// TrustedProxies lista IPs/CIDRs dos proxies cujos X-Forwarded-For,
	// X-Real-IP, X-Request-Id, traceparent e tracestate são aceitos. Vazio:
	// nenhum é aceito. No ambiente, separados por vírgula
	// (FLOW_SERVER_TRUSTED_PROXIES).
	TrustedProxies []string `mapstructure:"trusted_proxies"`
	// MetricsUsername e MetricsPassword são a basic auth do GET /metrics. Sem
	// padrão nem flag (FLOW_SERVER_METRICS_USERNAME/_PASSWORD, .env ou YAML);
	// os dois vazios desligam a rota (404).
	MetricsUsername string `mapstructure:"metrics_username"`
	MetricsPassword string `mapstructure:"metrics_password"`
	// PprofEnabled liga o /debug/pprof num listener administrativo próprio, em
	// PprofAddr (padrão 127.0.0.1:6060), nunca no público. Fora do loopback,
	// exige a basic auth do /metrics, que então o protege também.
	PprofEnabled bool   `mapstructure:"pprof_enabled"`
	PprofAddr    string `mapstructure:"pprof_addr"`
}

// LogConfig configura o logger estruturado.
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// OpenRouterConfig configura o upstream OpenRouter.
type OpenRouterConfig struct {
	BaseURL string `mapstructure:"base_url"`
	APIKey  string `mapstructure:"api_key"`
	// Prazos por fase e pool de conexões do cliente HTTP (internal/httpclient).
	// Não há prazo total: a duração de uma chamada, stream SSE inclusive, é do ctx.
	DialTimeout           time.Duration `mapstructure:"dial_timeout"`
	TLSHandshakeTimeout   time.Duration `mapstructure:"tls_handshake_timeout"`
	ResponseHeaderTimeout time.Duration `mapstructure:"response_header_timeout"`
	IdleConnTimeout       time.Duration `mapstructure:"idle_conn_timeout"`
	TCPKeepAlive          time.Duration `mapstructure:"tcp_keep_alive"`
	MaxIdleConnsPerHost   int           `mapstructure:"max_idle_conns_per_host"`
	// MaxConnsPerHost 0 desliga o teto de conexões simultâneas.
	MaxConnsPerHost int `mapstructure:"max_conns_per_host"`
}

// CacheConfig configura o cliente do semantic-cache-rs (IPC v1, docs/ipc-v1.md).
type CacheConfig struct {
	// SocketPath é o Unix domain socket do semantic-cache-rs.
	SocketPath string `mapstructure:"socket_path"`
	// SocketOwnerUID é o uid esperado do dono do socket e do processo do cache
	// ("Segurança do socket" no docs/ipc-v1.md); -1 = o uid do próprio gateway.
	SocketOwnerUID int `mapstructure:"socket_owner_uid"`
}

var defaults = map[string]any{
	"env":                     "development",
	"server.addr":             ":8080",
	"server.read_timeout":     15 * time.Second,
	"server.write_timeout":    time.Duration(0),
	"server.shutdown_timeout": 15 * time.Second,
	"server.trusted_proxies":  []string{},
	"server.health_timeout":   2 * time.Second,
	"server.metrics_username": "",
	"server.metrics_password": "",
	"server.pprof_enabled":    false,
	"server.pprof_addr":       "127.0.0.1:6060",
	"log.level":               "info",
	"log.format":              "json",
	"openrouter.base_url":     "https://openrouter.ai/api/v1",
	"openrouter.api_key":      "",
	// O 200 da OpenRouter sai quando o provedor aceita a requisição, antes do
	// primeiro token: 60s cobrem fila e cold start sem esperar a geração inteira.
	"openrouter.dial_timeout":            5 * time.Second,
	"openrouter.tls_handshake_timeout":   10 * time.Second,
	"openrouter.response_header_timeout": 60 * time.Second,
	"openrouter.idle_conn_timeout":       90 * time.Second,
	"openrouter.tcp_keep_alive":          30 * time.Second,
	"openrouter.max_idle_conns_per_host": 64, // o padrão do Go é 2: um gateway num host só reabriria conexão o tempo todo
	"openrouter.max_conns_per_host":      0,
	"cache.socket_path":                  "/tmp/semantic-cache.sock",
	"cache.socket_owner_uid":             -1,
}

// RegisterFlags declara em fs as flags aceitas por Load. Segredos (api_key)
// ficam de fora de propósito: flag aparece em `ps` e no histórico do shell.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.String("config", "", "arquivo YAML de configuração")
	fs.String("env-file", ".env", "arquivo .env (ignorado se não existir)")
	fs.String("env", defaults["env"].(string), "ambiente: development ou production")
	fs.String("server.addr", defaults["server.addr"].(string), "endereço de escuta do servidor HTTP")
	fs.Duration("server.shutdown_timeout", defaults["server.shutdown_timeout"].(time.Duration), "prazo do graceful shutdown")
	fs.String("log.level", defaults["log.level"].(string), "nível de log: debug, info, warn ou error")
	fs.String("log.format", defaults["log.format"].(string), "formato do log: json ou text")
}

// Load monta a configuração a partir de fs (já parseado por quem chama), do
// ambiente, do .env, do arquivo YAML e dos padrões, e valida o resultado.
func Load(fs *pflag.FlagSet) (*Config, error) {
	v := viper.New()
	for key, value := range defaults {
		v.SetDefault(key, value)
	}

	if path, _ := fs.GetString("config"); path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("config: lendo %s: %w", path, err)
		}
	}
	if path, _ := fs.GetString("env-file"); path != "" {
		if err := mergeDotenv(v, path); err != nil {
			return nil, err
		}
	}

	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	fs.VisitAll(func(f *pflag.Flag) {
		if _, known := defaults[f.Name]; known {
			_ = v.BindPFlag(f.Name, f) // só falha com flag nil
		}
	})

	var cfg Config
	if err := v.UnmarshalExact(&cfg); err != nil {
		return nil, fmt.Errorf("config: chave inválida: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// mergeDotenv aplica as chaves conhecidas do .env acima do arquivo YAML.
func mergeDotenv(v *viper.Viper, path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("config: lendo %s: %w", path, err)
	}
	dv := viper.New()
	dv.SetConfigType("env")
	if err := dv.ReadConfig(bytes.NewReader(data)); err != nil {
		return fmt.Errorf("config: parse de %s: %w", path, err)
	}

	values := map[string]any{}
	for key := range defaults {
		name := strings.ToLower(envPrefix + "_" + strings.ReplaceAll(key, ".", "_"))
		if !dv.IsSet(name) {
			continue
		}
		section, field, nested := strings.Cut(key, ".")
		if !nested {
			values[key] = dv.Get(name)
			continue
		}
		m, _ := values[section].(map[string]any)
		if m == nil {
			m = map[string]any{}
			values[section] = m
		}
		m[field] = dv.Get(name)
	}
	return v.MergeConfigMap(values)
}

// Validate reúne todos os problemas da configuração num único erro.
func (c *Config) Validate() error {
	var errs []error
	if c.Env != "development" && c.Env != "production" {
		errs = append(errs, fmt.Errorf("env %q inválido (development|production)", c.Env))
	}
	if c.Server.Addr == "" {
		errs = append(errs, errors.New("server.addr é obrigatório"))
	}
	if c.Server.ReadTimeout <= 0 || c.Server.ShutdownTimeout <= 0 || c.Server.HealthTimeout <= 0 {
		errs = append(errs, errors.New("server.read_timeout, server.shutdown_timeout e server.health_timeout devem ser positivos"))
	}
	if c.Server.WriteTimeout < 0 {
		errs = append(errs, errors.New("server.write_timeout não pode ser negativo"))
	}
	if (c.Server.MetricsUsername == "") != (c.Server.MetricsPassword == "") {
		errs = append(errs, errors.New("server.metrics_username e server.metrics_password vêm juntos (os dois vazios desligam o /metrics)"))
	}
	if strings.Contains(c.Server.MetricsUsername, ":") {
		errs = append(errs, errors.New("server.metrics_username não pode ter ':' (RFC 7617)"))
	}
	if c.Server.PprofEnabled {
		host, _, err := net.SplitHostPort(c.Server.PprofAddr)
		ip, ipErr := netip.ParseAddr(host)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("server.pprof_addr %q inválido: %w", c.Server.PprofAddr, err))
		case c.Server.MetricsUsername == "" && (ipErr != nil || !ip.IsLoopback()):
			errs = append(errs, fmt.Errorf("server.pprof_addr %q fora do loopback (use 127.0.0.1 ou ::1) exige server.metrics_username e server.metrics_password", c.Server.PprofAddr))
		}
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("log.level %q inválido (debug|info|warn|error)", c.Log.Level))
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		errs = append(errs, fmt.Errorf("log.format %q inválido (json|text)", c.Log.Format))
	}
	if u, err := url.Parse(c.OpenRouter.BaseURL); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		errs = append(errs, fmt.Errorf("openrouter.base_url %q não é uma URL http(s)", c.OpenRouter.BaseURL))
	}
	if o := c.OpenRouter; o.DialTimeout <= 0 || o.TLSHandshakeTimeout <= 0 || o.ResponseHeaderTimeout <= 0 || o.IdleConnTimeout <= 0 || o.TCPKeepAlive <= 0 {
		errs = append(errs, errors.New("openrouter.dial_timeout, tls_handshake_timeout, response_header_timeout, idle_conn_timeout e tcp_keep_alive devem ser positivos"))
	}
	if c.OpenRouter.MaxIdleConnsPerHost < 1 || c.OpenRouter.MaxConnsPerHost < 0 {
		errs = append(errs, errors.New("openrouter.max_idle_conns_per_host deve ser >= 1 e openrouter.max_conns_per_host >= 0 (0 = sem teto)"))
	}
	if c.Cache.SocketPath == "" {
		errs = append(errs, errors.New("cache.socket_path é obrigatório"))
	}
	if c.Cache.SocketOwnerUID < -1 {
		errs = append(errs, errors.New("cache.socket_owner_uid deve ser >= -1 (-1 = o uid do gateway)"))
	}
	if c.Env == "production" && c.OpenRouter.APIKey == "" {
		errs = append(errs, errors.New("openrouter.api_key é obrigatória em production (FLOW_OPENROUTER_API_KEY)"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("config inválida: %w", errors.Join(errs...))
	}
	return nil
}
