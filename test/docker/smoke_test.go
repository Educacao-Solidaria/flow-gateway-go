package docker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Segredos de mentira (o smoke não chama a OpenRouter): nenhum dos dois pode aparecer no
// log do gateway.
const (
	fakeKey     = "chave-de-mentira-do-smoke"
	metricsUser = "smoke"
	metricsPass = "senha-do-smoke"
)

const (
	// stopGrace é o prazo do `docker stop` e do `docker compose down` entre o SIGTERM e o
	// SIGKILL (o padrão, 10 s): o gateway tem de sair antes, com 0.
	stopGrace = 10 * time.Second
	// killAfter é quanto o smoke espera antes de matar: o dobro, para "saiu tarde" (mais
	// que stopGrace) e "não saiu" (morto pelo SIGKILL) aparecerem separados.
	killAfter = 2 * stopGrace
)

// configs são os ambientes em que o smoke sobe o gateway: o padrão da imagem (um `docker
// run` sem nada; sem credencial, o /metrics não existe) e o do compose do ecossistema (§4:
// production, com a chave), mais a basic auth do /metrics.
var configs = map[string][]string{
	"padrão": {},
	"production": {"FLOW_ENV=production", "FLOW_OPENROUTER_API_KEY=" + fakeKey,
		"FLOW_SERVER_METRICS_USERNAME=" + metricsUser, "FLOW_SERVER_METRICS_PASSWORD=" + metricsPass},
}

// invalid é uma config que não pode subir (production sem a chave), e invalidWhy, o motivo
// que o gateway dá na saída de erro (o log do container).
const invalid, invalidWhy = "FLOW_ENV=production", "openrouter.api_key é obrigatória em production"

// gateway é um flow-gateway de pé: o container da imagem (TestImageSmoke) ou o binário
// deste checkout (TestBinarySmoke).
type gateway struct {
	base string                                // http://host:porta, já escutando
	term func() (code int, took time.Duration) // manda SIGTERM e espera a saída
	logs func() string                         // saída padrão e de erro do processo
}

// smoke confere de fora o gateway que subiu com env: /livez e /healthz 200; /metrics 404
// sem credencial configurada e, com ela, 401 sem senha ou com a errada e 200 com a certa,
// já contando os pedidos anteriores; o SIGTERM encerra com 0 antes de stopGrace; nenhum
// segredo no log.
func smoke(t *testing.T, gw gateway, env []string) {
	for _, path := range []string{"/livez", "/healthz"} {
		res := get(t, gw.base+path, "")
		assert.Equal(t, http.StatusOK, res.code, "%s: %s", path, res.body)
		assert.Contains(t, res.body, `"status":"ok"`, path)
	}
	if !slices.Contains(env, "FLOW_SERVER_METRICS_PASSWORD="+metricsPass) {
		assert.Equal(t, http.StatusNotFound, get(t, gw.base+"/metrics", "").code, "/metrics sem credencial configurada")
	} else {
		for _, pass := range []string{"", "senha-errada"} {
			res := get(t, gw.base+"/metrics", pass)
			assert.Equal(t, http.StatusUnauthorized, res.code, "/metrics com a senha %q", pass)
			assert.Equal(t, `Basic realm="flow-gateway metrics", charset="UTF-8"`, res.header.Get("WWW-Authenticate"))
		}
		res := get(t, gw.base+"/metrics", metricsPass)
		assert.Equal(t, http.StatusOK, res.code, res.body)
		assert.Equal(t, "text/plain; version=0.0.4; charset=utf-8", res.header.Get("Content-Type"))
		for _, series := range []string{`route="/healthz",method="GET",code="200"} 1`, `route="/metrics",method="GET",code="401"} 2`} {
			assert.Contains(t, res.body, "\nflow_gateway_http_requests_total{"+series+"\n", "/metrics conta os pedidos do smoke")
		}
	}

	code, took := gw.term()
	t.Logf("SIGTERM: saída %d em %s", code, took)
	assert.Equal(t, 0, code, "saída depois do SIGTERM")
	assert.Less(t, took, stopGrace, "o SIGTERM encerra antes do SIGKILL do docker stop")
	logs := gw.logs()
	assert.Contains(t, logs, `"msg":"servidor encerrado"`, "encerramento gracioso")
	for _, secret := range []string{fakeKey, metricsPass} {
		assert.NotContains(t, logs, secret, "segredo no log")
	}
}

// response é o que o smoke lê de uma resposta (o bodyclose reprova helper que devolve
// *http.Response).
type response struct {
	code   int
	header http.Header
	body   string
}

// get faz GET em url, com a basic auth do /metrics quando pass não é vazia.
func get(t *testing.T, url, pass string) response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	if pass != "" {
		req.SetBasicAuth(metricsUser, pass)
	}
	res, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return response{res.StatusCode, res.Header, string(body)}
}

// listening espera a linha do log em que o gateway diz onde escuta e devolve o endereço.
// Reprova na hora se o processo saiu antes, e em 30 s (runner lento) sem a linha.
func listening(t *testing.T, gw gateway, running func() bool) string {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		for _, line := range strings.Split(gw.logs(), "\n") {
			var e struct{ Msg, Addr string }
			if json.Unmarshal([]byte(line), &e) == nil && e.Msg == "servidor http no ar" {
				return e.Addr
			}
		}
		require.True(t, running(), "o gateway saiu antes de escutar:\n%s", gw.logs())
		require.True(t, time.Now().Before(deadline), "o gateway não escutou em 30 s:\n%s", gw.logs())
	}
}

// TestImageSmoke passa o smoke na imagem (GATEWAY_IMAGE) subida como o compose a sobe
// (raiz só leitura, sem capabilities, no-new-privileges), com a 8080 publicada no
// loopback. Também confere um processo só, com uid e gid 65532 (os do processo, pelo ps
// do host), escutando na 8080 do contrato §2; o SIGTERM vem do `docker stop`. A config
// inválida derruba a partida.
func TestImageSmoke(t *testing.T) {
	img := image(t)
	hardened := []string{"--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges"}
	for name, env := range configs {
		t.Run(name, func(t *testing.T) {
			args := append([]string{"run", "-d", "-p", "127.0.0.1::8080"}, hardened...)
			for _, kv := range env {
				args = append(args, "-e", kv)
			}
			id := docker(t, append(args, img)...)
			gw := gateway{logs: func() string {
				out, _ := exec.Command("docker", "logs", id).CombinedOutput()
				return string(out)
			}}
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("log do container:\n%s", gw.logs())
				}
				_ = exec.Command("docker", "rm", "-f", id).Run()
			})
			addr := listening(t, gw, func() bool { return docker(t, "inspect", "-f", "{{.State.Running}}", id) == "true" })
			assert.True(t, strings.HasSuffix(addr, ":8080"), "contrato §2: o gateway escuta na 8080, não em %s", addr)
			top := strings.Split(docker(t, "top", id, "-eo", "uid,gid,pid"), "\n")
			require.Len(t, top, 2, "um processo só no container: %q", top)
			assert.Regexp(t, `^65532\s+65532\s+\d+$`, strings.TrimSpace(top[1]), "uid e gid do processo")

			gw.base = "http://" + strings.Split(docker(t, "port", id, "8080/tcp"), "\n")[0]
			gw.term = func() (int, time.Duration) {
				start := time.Now()
				docker(t, "stop", "--signal=SIGTERM", "--timeout="+strconv.Itoa(int(killAfter.Seconds())), id)
				took := time.Since(start)
				code, err := strconv.Atoi(docker(t, "inspect", "-f", "{{.State.ExitCode}}", id))
				require.NoError(t, err)
				return code, took
			}
			smoke(t, gw, env)
		})
	}
	// Com prazo: se a validação regredir, o gateway sobe e não sai sozinho.
	t.Run("config inválida", func(t *testing.T) {
		id := docker(t, append(append([]string{"run", "-d", "-e", invalid}, hardened...), img)...)
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		code, err := exec.CommandContext(ctx, "docker", "wait", id).Output()
		require.NoError(t, err, "a partida não falhou em 30 s")
		var stderr strings.Builder
		logs := exec.Command("docker", "logs", id)
		logs.Stderr = &stderr
		require.NoError(t, logs.Run())
		assert.Equal(t, "1", strings.TrimSpace(string(code)), "saída da partida: %s", &stderr)
		assert.Contains(t, stderr.String(), invalidWhy)
	})
}

// TestBinarySmoke passa o mesmo smoke no binário deste checkout, sem Docker: roda na
// matriz do ci.yml (fora do Windows, que não tem SIGTERM) e localmente.
func TestBinarySmoke(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("o Windows não tem SIGTERM")
	}
	bin := filepath.Join(t.TempDir(), "flow-gateway")
	build := exec.Command("go", "build", "-o", bin, "./cmd/flow-gateway")
	build.Dir = root
	out, err := build.CombinedOutput()
	require.NoError(t, err, "%s", out)
	// Numa pasta vazia (o gateway lê o .env do cwd), numa porta livre e só com env: nada do
	// ambiente de quem testa.
	command := func(ctx context.Context, t *testing.T, env ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, bin, "--server.addr", "127.0.0.1:0")
		cmd.Dir, cmd.Env = t.TempDir(), append([]string{}, env...) // nunca nil: nil herda o ambiente
		return cmd
	}
	for name, env := range configs {
		t.Run(name, func(t *testing.T) {
			cmd := command(context.Background(), t, env...)
			log := filepath.Join(t.TempDir(), "log")
			f, err := os.Create(log)
			require.NoError(t, err)
			cmd.Stdout, cmd.Stderr = f, f
			require.NoError(t, cmd.Start())
			done := make(chan struct{})
			go func() { _ = cmd.Wait(); close(done) }()
			t.Cleanup(func() { _ = cmd.Process.Kill(); <-done; _ = f.Close() })

			gw := gateway{logs: func() string { b, _ := os.ReadFile(log); return string(b) }}
			gw.base = "http://" + listening(t, gw, func() bool {
				select {
				case <-done:
					return false
				default:
					return true
				}
			})
			gw.term = func() (int, time.Duration) {
				start := time.Now()
				require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
				select {
				case <-done:
				case <-time.After(killAfter): // como o docker stop
					_ = cmd.Process.Kill()
					<-done
				}
				return cmd.ProcessState.ExitCode(), time.Since(start)
			}
			smoke(t, gw, env)
		})
	}
	t.Run("config inválida", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := command(ctx, t, invalid).Output()
		var ee *exec.ExitError
		require.ErrorAs(t, err, &ee)
		assert.Equal(t, 1, ee.ExitCode(), "saída da partida: %s", ee.Stderr)
		assert.Contains(t, string(ee.Stderr), invalidWhy)
	})
}
