package docker

import (
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
)

// composeDir é a pasta do compose do ecossistema, relativa à raiz do repositório.
const composeDir = "deploy/compose"

// O socket IPC no volume cache-ipc, igual nos dois lados (contrato §3).
const (
	socketDir = "/run/semantic-cache"
	socket    = socketDir + "/semantic-cache.sock"
)

// secretRef casa a única interpolação aceita no compose: ${VAR:?mensagem}, obrigatória e
// sem valor padrão (vazia, o compose recusa subir).
var secretRef = regexp.MustCompile(`\$\{(\w+):\?[^}]+\}`)

// service é o que os contratos fixam num serviço do compose.
type service struct {
	Build, Image, User string
	EnvFile            any `yaml:"env_file"`
	Environment        map[string]string
	Volumes, Tmpfs     []string
	Ports, Networks    []string
	DependsOn          map[string]struct{ Condition string } `yaml:"depends_on"`
	ReadOnly           bool                                  `yaml:"read_only"`
	CapDrop            []string                              `yaml:"cap_drop"`
	SecurityOpt        []string                              `yaml:"security_opt"`
	Healthcheck        struct{ Test []string }
}

// project é o compose como está no arquivo, sem interpolar o .env.
type project struct {
	Name     string
	Services map[string]service
	Volumes  map[string]any
	Networks map[string]struct{ Internal bool }
}

// loadCompose lê o compose do ecossistema e devolve também o texto cru.
func loadCompose(t *testing.T) (project, string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, composeDir, "compose.yaml"))
	require.NoError(t, err)
	var p project
	require.NoError(t, yaml.Unmarshal(data, &p))
	return p, string(data)
}

// TestCompose confere o compose do ecossistema contra os contratos da Fase 5 (§1–§4).
func TestCompose(t *testing.T) {
	p, raw := loadCompose(t)
	assert.Equal(t, "educacao-solidaria", p.Name)

	// Por serviço: a imagem (§2), o build na raiz de cada repo lado a lado (§1; relativo à raiz
	// deste), a porta no host só no loopback (§4) e as redes (banco e cache só na interna).
	type spec struct {
		image, build, port string
		networks           []string
	}
	contract := map[string]spec{
		"gateway": {"educacao-solidaria/flow-gateway:dev", ".", "8080", []string{"edge"}},
		"cache":   {"educacao-solidaria/semantic-cache:dev", "../semantic-cache-rs", "", []string{"internal"}},
		"rag":     {"educacao-solidaria/brain-router:dev", "../brain-router-py", "8000", []string{"edge", "internal"}},
		"studio":  {"educacao-solidaria/agent-studio:dev", "../agent-studio-ts", "3000", []string{"edge", "internal"}},
		"db":      {networks: []string{"internal"}},
	}
	require.ElementsMatch(t, slices.Collect(maps.Keys(contract)), slices.Collect(maps.Keys(p.Services)), "nomes DNS fixos")
	for name, want := range contract {
		s := p.Services[name]
		var ports []string
		if want.port != "" {
			ports = []string{"127.0.0.1:" + want.port + ":" + want.port}
		}
		assert.Equal(t, ports, s.Ports, "%s: portas no host", name)
		assert.Equal(t, want.networks, s.Networks, "%s: redes", name)
		assert.Nil(t, s.EnvFile, "%s: env_file entregaria o .env inteiro", name)
		assert.Empty(t, s.User, "%s: roda com o usuário da imagem", name)
		if name == "db" {
			continue
		}
		assert.Equal(t, want.image, s.Image)
		assert.Equal(t, want.build, path.Join(composeDir, s.Build), "%s: contexto do build", name)
		assert.True(t, s.ReadOnly && slices.Equal(s.CapDrop, []string{"ALL"}) && slices.Equal(s.SecurityOpt, []string{"no-new-privileges:true"}),
			"%s: raiz só leitura, sem capabilities e sem escalada de privilégio", name)
	}
	assert.True(t, p.Networks["internal"].Internal, "a rede interna não tem saída")
	assert.False(t, p.Networks["edge"].Internal, "as portas do host saem pela edge")
	assert.ElementsMatch(t, []string{"cache-ipc", "cache-data", "pg-data"}, slices.Collect(maps.Keys(p.Volumes)))

	// §3: o mesmo volume no mesmo ponto dos dois lados; a API HTTP do cache só na rede interna.
	gw, cache, rag, db := p.Services["gateway"], p.Services["cache"], p.Services["rag"], p.Services["db"]
	assert.Equal(t, []string{"cache-ipc:" + socketDir}, gw.Volumes)
	assert.Equal(t, []string{"cache-ipc:" + socketDir, "cache-data:/var/lib/semantic-cache"}, cache.Volumes)
	assert.Equal(t, map[string]string{
		"SEMCACHE_IPC_SOCKET_PATH": socket, "SEMCACHE_PERSISTENCE_PATH": "/var/lib/semantic-cache",
		"SEMCACHE_HTTP_ENABLED": "true", "SEMCACHE_HTTP_LISTEN": "0.0.0.0:8099",
	}, cache.Environment)
	assert.Equal(t, "service_healthy", gw.DependsOn["cache"].Condition)

	// O gateway sobe com o ambiente do compose, e cada variável chega à chave certa: um nome
	// errado seria ignorado e valeria o padrão.
	for k, v := range gw.Environment {
		t.Setenv(k, secretRef.ReplaceAllString(v, "valor-de-teste"))
	}
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	config.RegisterFlags(fs)
	cfg, err := config.Load(fs)
	require.NoError(t, err)
	assert.Equal(t, "production", cfg.Env)
	assert.Equal(t, "valor-de-teste", cfg.OpenRouter.APIKey)
	assert.Equal(t, socket, cfg.Cache.SocketPath)
	assert.Equal(t, 65532, cfg.Cache.SocketOwnerUID, "o dono do socket é o uid do cache, o mesmo do gateway")
	_, port, err := net.SplitHostPort(cfg.Server.Addr)
	require.NoError(t, err)
	assert.Equal(t, contract["gateway"].port, port, "a porta publicada é a que o gateway escuta")

	// §4: banco e papel `brain`, com a mesma senha do .env no banco e no DATABASE_URL do RAG.
	pw := db.Environment["POSTGRES_PASSWORD"]
	assert.Equal(t, map[string]string{"POSTGRES_USER": "brain", "POSTGRES_DB": "brain", "POSTGRES_PASSWORD": pw}, db.Environment)
	assert.Regexp(t, `^pgvector/pgvector:pg17@sha256:[0-9a-f]{64}$`, db.Image, "§2: a tag do contrato, fixada pelo digest")
	assert.Equal(t, []string{"pg-data:/var/lib/postgresql/data"}, db.Volumes)
	assert.Contains(t, strings.Join(db.Healthcheck.Test, " "), "pg_isready -h 127.0.0.1 ", "saúde pelo TCP, não pelo socket do initdb")
	assert.Equal(t, "postgresql+asyncpg://brain:"+pw+"@db:5432/brain", rag.Environment["DATABASE_URL"])
	assert.Equal(t, "http://gateway:8080", rag.Environment["GATEWAY_URL"])
	assert.Equal(t, "service_healthy", rag.DependsOn["db"].Condition)
	for _, name := range []string{"rag", "studio"} {
		assert.Equal(t, []string{"/tmp"}, p.Services[name].Tmpfs, "%s: /tmp gravável com a raiz só leitura", name)
	}

	// Segredos só pelo .env (fora do git): toda interpolação é ${VAR:?…} de uma chave do
	// .env.example, que só traz os marcadores vazios, e cada container recebe só os seus.
	refs := secretRef.FindAllStringSubmatch(raw, -1)
	assert.Len(t, refs, strings.Count(raw, "$"), "interpolação fora da forma ${VAR:?mensagem}")
	var keys []string
	for _, l := range lines(t, path.Join(composeDir, ".env.example")) {
		k, v, _ := strings.Cut(l, "=")
		assert.Empty(t, v, "%s: o exemplo só traz o marcador", k)
		keys = append(keys, k)
	}
	assert.ElementsMatch(t, []string{"POSTGRES_PASSWORD", "OPENROUTER_API_KEY", "FLOW_OPENROUTER_API_KEY"}, keys)
	secrets := map[string][]string{"gateway": {"FLOW_OPENROUTER_API_KEY"}, "rag": {"POSTGRES_PASSWORD", "OPENROUTER_API_KEY"}, "db": {"POSTGRES_PASSWORD"}}
	for name, s := range p.Services {
		var got []string
		for _, v := range s.Environment {
			for _, m := range secretRef.FindAllStringSubmatch(v, -1) {
				got = append(got, m[1])
			}
		}
		assert.ElementsMatch(t, secrets[name], got, "%s: segredos que o container recebe", name)
	}
	assert.Contains(t, lines(t, ".gitignore"), ".env")
}

// stack devolve o `docker compose` do ecossistema já de pé ou pula o teste: o workflow
// compose.yml constrói as imagens, sobe o compose (up --wait) e roda os TestStack* com
// COMPOSE_STACK=1.
func stack(t *testing.T) func(args ...string) string {
	t.Helper()
	if os.Getenv("COMPOSE_STACK") == "" {
		t.Skip("sem COMPOSE_STACK: as sondas do ecossistema rodam no workflow compose.yml")
	}
	file := filepath.Join(root, composeDir, "compose.yaml")
	return func(args ...string) string { return docker(t, append([]string{"compose", "-f", file}, args...)...) }
}

// TestStackServes sonda as portas do host e, pela origem do estúdio (§5), o nginx dele
// chegando a gateway, RAG e cache pelos nomes DNS do compose (502 se não chega).
func TestStackServes(t *testing.T) {
	stack(t)
	client := &http.Client{Timeout: 5 * time.Second}
	for _, url := range []string{
		"http://127.0.0.1:8080/livez", "http://127.0.0.1:8080/healthz", "http://127.0.0.1:8000/healthz",
		"http://127.0.0.1:3000/", "http://127.0.0.1:3000/health/gateway",
		"http://127.0.0.1:3000/health/rag", "http://127.0.0.1:3000/health/cache",
	} {
		res, err := client.Get(url)
		if assert.NoError(t, err) {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			assert.Equal(t, http.StatusOK, res.StatusCode, "%s: %s", url, body)
		}
	}
}

// TestStackCacheSocket pinga o cache (§3) pelo socket que o gateway enxerga: um container
// com os volumes do gateway (--volumes-from), sem rede, roda o --ping do próprio cache no
// FLOW_CACHE_SOCKET_PATH do gateway. O uid 65532 recebe o PONG (saída 0); outro uid é
// barrado pela pasta 0700 do volume (saída 1).
func TestStackCacheSocket(t *testing.T) {
	compose := stack(t)
	p, _ := loadCompose(t)
	gateway := compose("ps", "-q", "gateway")
	for user, want := range map[string]int{"65532:65532": 0, "10001:10001": 1} {
		out, err := exec.Command("docker", "run", "--rm", "--network=none", "--volumes-from="+gateway, "--user="+user,
			"--env=SEMCACHE_IPC_SOCKET_PATH="+p.Services["gateway"].Environment["FLOW_CACHE_SOCKET_PATH"],
			p.Services["cache"].Image, "--ping").CombinedOutput()
		code := 0
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) {
			code = ee.ExitCode()
		} else {
			require.NoError(t, err)
		}
		assert.Equal(t, want, code, "--ping como %s: %s", user, out)
	}
}

// ragProbe roda no container do rag com o ambiente do compose: o Settings do brain-router
// valida o DATABASE_URL, o engine dele conecta no `db` pela rede interna, e a extensão
// vector (que a migração 0001 cria) está disponível.
const ragProbe = `
import asyncio
from sqlalchemy import text
from app.db import Database
from app.settings import Settings

async def vector():
    db = Database.from_settings(Settings())
    try:
        async with db.engine.connect() as conn:
            return await conn.scalar(text("SELECT count(*) FROM pg_available_extensions WHERE name = 'vector'"))
    finally:
        await db.dispose()

raise SystemExit(0 if asyncio.run(vector()) == 1 else 3)
`

// TestStackRAGDatabase prova o RAG com o banco pelo código do próprio RAG: a fábrica da
// imagem ainda não lê o Settings, e o /healthz dela dá 200 sem olhar o banco.
func TestStackRAGDatabase(t *testing.T) {
	compose := stack(t)
	compose("exec", "-T", "rag", "python", "-c", ragProbe)
}
