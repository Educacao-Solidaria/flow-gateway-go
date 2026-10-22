package docker

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
)

// root é a raiz do repositório, onde ficam o Dockerfile e o .dockerignore.
const root = "../.."

// instruction é uma instrução do Dockerfile, com as continuações ("\") juntadas.
type instruction struct{ cmd, args string }

// lines devolve as linhas úteis de um arquivo da raiz: sem espaço nas pontas (o
// checkout do Windows traz \r), sem linha vazia e sem comentário.
func lines(t *testing.T, name string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, name))
	require.NoError(t, err)
	var out []string
	for _, l := range strings.Split(string(data), "\n") {
		if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
			out = append(out, l)
		}
	}
	return out
}

// parse lê o Dockerfile da raiz, uma entrada por instrução.
func parse(t *testing.T) []instruction {
	t.Helper()
	var out []instruction
	cur := ""
	for _, l := range lines(t, "Dockerfile") {
		if rest, ok := strings.CutSuffix(l, `\`); ok {
			cur += rest + " "
			continue
		}
		cmd, args, _ := strings.Cut(cur+l, " ")
		out = append(out, instruction{strings.ToUpper(cmd), strings.TrimSpace(args)})
		cur = ""
	}
	return out
}

// values devolve os argumentos das instruções cmd de ins, na ordem.
func values(ins []instruction, cmd string) []string {
	var out []string
	for _, in := range ins {
		if in.cmd == cmd {
			out = append(out, in.args)
		}
	}
	return out
}

func TestDockerfile(t *testing.T) {
	ins := parse(t)
	froms, last := 0, 0
	for i, in := range ins {
		if in.cmd == "FROM" {
			froms, last = froms+1, i
			assert.Regexp(t, `^(--platform=\S+ )?\S+@sha256:[0-9a-f]{64}( (?i:as) [\w.-]+)?$`, in.args, "base fixada por digest (a tag fica no comentário)")
		}
	}
	require.GreaterOrEqual(t, froms, 2, "multi-stage: compila num estágio e a imagem final só recebe o binário")
	build, final := ins[:last], ins[last:]

	assert.True(t, strings.HasPrefix(final[0].args, "gcr.io/distroless/static-debian12:nonroot@"), "imagem final: %s", final[0].args)
	assert.Equal(t, []string{"65532:65532"}, values(final, "USER"), "contrato §2: usuário numérico 65532:65532")
	var entry []string
	require.NoError(t, json.Unmarshal([]byte(strings.Join(values(final, "ENTRYPOINT"), "")), &entry), "ENTRYPOINT em forma exec: a distroless não tem shell")
	assert.Equal(t, []string{"/usr/local/bin/flow-gateway"}, entry)
	for _, c := range values(final, "COPY") {
		assert.True(t, strings.HasPrefix(c, "--from=build "), "nada do contexto vai direto para a imagem final: COPY %s", c)
	}
	assert.Empty(t, values(ins, "ADD"), "ADD baixa URL e abre tar: use COPY")

	// A porta exposta é a que o binário escuta sem configuração (server.addr).
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	config.RegisterFlags(fs)
	_, port, err := net.SplitHostPort(fs.Lookup("server.addr").DefValue)
	require.NoError(t, err)
	assert.Equal(t, "8080", port, "contrato §2: o gateway escuta em :8080")
	assert.Equal(t, []string{port}, values(final, "EXPOSE"))

	run := strings.Join(values(build, "RUN"), "\n")
	const pkg = "github.com/Educacao-Solidaria/flow-gateway-go/internal/version"
	for _, want := range []string{"CGO_ENABLED=0 ", "-trimpath", `-ldflags "-s -w `, " ./cmd/flow-gateway",
		"-X " + pkg + ".Version=${VERSION}", "-X " + pkg + ".Commit=${COMMIT}"} {
		assert.Contains(t, run, want)
	}
	for _, in := range ins {
		if in.cmd == "ARG" || in.cmd == "ENV" {
			assert.NotRegexp(t, `(?i)key|secret|token|passw|credential`, in.args, "segredo em ARG/ENV fica no histórico da imagem")
		}
	}
}

func TestDockerignore(t *testing.T) {
	rules := lines(t, ".dockerignore")
	require.NotEmpty(t, rules)
	assert.Equal(t, "*", rules[0], "lista de permissão: tudo fora, depois libera o que o build lê")
	lastAllow := 0
	for i, r := range rules {
		if strings.HasPrefix(r, "!") {
			lastAllow = i
			assert.NotRegexp(t, `\.(env|git)`, r, "liberação de .env/.git")
		}
	}
	assert.Subset(t, rules[lastAllow+1:], []string{"**/.env", "**/.env.*"}, "segredo local fica fora até dentro das pastas liberadas")
}
