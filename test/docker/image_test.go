package docker

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// maxImageBytes é o teto do contrato §2 para a imagem do gateway: 20 MB decimais,
// a unidade do `docker images`.
const maxImageBytes = 20_000_000

// image devolve a imagem a testar (GATEWAY_IMAGE) ou pula o teste: ele precisa do
// Docker e da imagem já construída, e o workflow docker.yml faz as duas coisas.
func image(t *testing.T) string {
	t.Helper()
	img := os.Getenv("GATEWAY_IMAGE")
	if img == "" {
		t.Skip("sem GATEWAY_IMAGE: os testes da imagem rodam no workflow docker.yml")
	}
	return img
}

// docker roda o CLI do Docker e devolve a saída padrão, sem espaço nas pontas.
func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).Output()
	if ee := (*exec.ExitError)(nil); errors.As(err, &ee) {
		err = fmt.Errorf("%w: %s", err, ee.Stderr)
	}
	require.NoError(t, err, "docker %s", strings.Join(args, " "))
	return strings.TrimSpace(string(out))
}

// TestImageConfig confere a imagem construída: tamanho, usuário, porta e a versão
// injetada pelo -ldflags (um -X com caminho errado é ignorado sem aviso).
func TestImageConfig(t *testing.T) {
	img := image(t)
	var inspect []struct {
		Size   int64
		Config struct {
			User         string
			ExposedPorts map[string]struct{}
		}
	}
	require.NoError(t, json.Unmarshal([]byte(docker(t, "image", "inspect", img)), &inspect))
	require.Len(t, inspect, 1)
	got := inspect[0]
	// O .Size depende do image store: no do containerd (o Docker 29 do ubuntu-latest)
	// é o tamanho compactado até a imagem ser desempacotada. O tamanho desempacotado
	// sai do docker export, em TestImageFiles.
	t.Logf("docker image inspect: Size = %d bytes (%.1f MB)", got.Size, float64(got.Size)/1e6)
	assert.Less(t, got.Size, int64(maxImageBytes), "contrato §2: imagem do gateway < 20 MB")
	assert.Equal(t, "65532:65532", got.Config.User)
	assert.Equal(t, map[string]struct{}{"8080/tcp": {}}, got.Config.ExposedPorts)
	if want := os.Getenv("GATEWAY_IMAGE_VERSION"); want != "" {
		assert.Equal(t, want, docker(t, "run", "--rm", img, "--version"))
	}
}

// TestImageFiles lê o sistema de arquivos da imagem: o tamanho desempacotado, o
// binário onde o ENTRYPOINT aponta, sem escrita para o processo, e nenhum shell.
func TestImageFiles(t *testing.T) {
	img := image(t)
	id := docker(t, "create", img)
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	cmd := exec.Command("docker", "export", id)
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	files := map[string]*tar.Header{}
	var size int64
	for tr := tar.NewReader(out); ; {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		files[path.Clean(h.Name)] = h
		if h.Typeflag == tar.TypeReg {
			size += h.Size
		}
	}
	require.NoError(t, cmd.Wait())
	t.Logf("arquivos da imagem, desempacotados: %d bytes (%.1f MB)", size, float64(size)/1e6)
	assert.Less(t, size, int64(maxImageBytes), "contrato §2: imagem do gateway < 20 MB (desempacotada)")

	bin := files["usr/local/bin/flow-gateway"]
	require.NotNil(t, bin, "binário fora do caminho do ENTRYPOINT")
	assert.True(t, bin.Uid == 0 && bin.Mode&0o022 == 0, "binário do root e sem escrita para os outros: uid %d, modo %o", bin.Uid, bin.Mode)
	for name := range files {
		assert.NotContains(t, []string{"sh", "bash", "ash", "dash", "busybox"}, path.Base(name), "contrato §2: shell na imagem final em /%s", name)
	}
}

// TestImageBuildContext constrói só o contexto (FROM scratch + COPY . /) de uma
// árvore de mentira com o .dockerignore do repo: prova com o Docker de verdade que
// o build recebe o código, e não .env, .git, artefato local ou teste.
func TestImageBuildContext(t *testing.T) {
	image(t)
	ignore, err := os.ReadFile(filepath.Join(root, ".dockerignore"))
	require.NoError(t, err)
	ctx, out := t.TempDir(), t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(ctx, ".dockerignore"), ignore, 0o600))
	want := map[string]bool{ // caminho -> entra no contexto?
		"go.mod": true, "go.sum": true, "cmd/flow-gateway/main.go": true, "internal/config/config.go": true, "pkg/domain/domain.go": true,
		".env": false, ".env.production": false, "internal/config/.env": false, "deploy/compose/.env": false, ".git/config": false,
		"bin/flow-gateway": false, "dist/flow-gateway-linux-amd64": false, "coverage.out": false, "internal/config/config_test.go": false,
	}
	for p := range want {
		f := filepath.Join(ctx, filepath.FromSlash(p))
		require.NoError(t, os.MkdirAll(filepath.Dir(f), 0o700))
		require.NoError(t, os.WriteFile(f, []byte("x"), 0o600))
	}
	df := filepath.Join(t.TempDir(), "Dockerfile")
	require.NoError(t, os.WriteFile(df, []byte("FROM scratch\nCOPY . /\n"), 0o600))
	docker(t, "build", "-f", df, "--output", "type=local,dest="+out, ctx)
	for p, in := range want {
		_, err := os.Stat(filepath.Join(out, filepath.FromSlash(p)))
		assert.Equal(t, in, err == nil, "%s no contexto do build", p)
	}
}
