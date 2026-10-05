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
	// X-Real-IP e X-Request-Id são aceitos. Vazio: nenhum é aceito. No
	// ambiente, separados por vírgula (FLOW_SERVER_TRUSTED_PROXIES).
	TrustedProxies []string `mapstructure:"trusted_proxies"`
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
}

var defaults = map[string]any{
	"env":                     "development",
	"server.addr":             ":8080",
	"server.read_timeout":     15 * time.Second,
	"server.write_timeout":    time.Duration(0),
	"server.shutdown_timeout": 15 * time.Second,
	"server.trusted_proxies":  []string{},
	"server.health_timeout":   2 * time.Second,
	"log.level":               "info",
	"log.format":              "json",
	"openrouter.base_url":     "https://openrouter.ai/api/v1",
	"openrouter.api_key":      "",
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
	if c.Env == "production" && c.OpenRouter.APIKey == "" {
		errs = append(errs, errors.New("openrouter.api_key é obrigatória em production (FLOW_OPENROUTER_API_KEY)"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("config inválida: %w", errors.Join(errs...))
	}
	return nil
}
