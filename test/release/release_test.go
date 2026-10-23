package release

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// root é a raiz do repositório.
const root = "../.."

// module é o caminho do módulo: o binário é o module/cmd/flow-gateway.
const module = "github.com/Educacao-Solidaria/flow-gateway-go"

// decode lê o YAML name da raiz em v; com strict, chave que v não declara reprova.
func decode(t *testing.T, name string, v any, strict bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, name))
	require.NoError(t, err)
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(strict)
	require.NoError(t, dec.Decode(v), name)
}

// platforms devolve os alvos do `make cross` (PLATFORMS no Makefile): a release empacota os mesmos.
func platforms(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "Makefile"))
	require.NoError(t, err)
	m := regexp.MustCompile(`(?m)^PLATFORMS\s*:=(.*)$`).FindSubmatch(data)
	require.NotNil(t, m, "PLATFORMS no Makefile")
	return strings.Fields(string(m[1]))
}

// TestConfig confere o .goreleaser.yaml e o release.yml contra o contrato da Fase 5 (§6): só a
// tag v* publica, só na release do GitHub deste repositório e só no job que tem escrita.
func TestConfig(t *testing.T) {
	// Seções conhecidas (KnownFields): qualquer outra (dockers, brews, nfpms, blobs, publishers,
	// announce, os hooks do before) reprova até entrar aqui. Na release, só o prerelease: um
	// github: apontaria outro repositório, um extra_files subiria outros arquivos.
	var cfg struct {
		Version          int
		ProjectName      string `yaml:"project_name"`
		Builds, Archives any
		Release          struct{ Prerelease string }
	}
	decode(t, ".goreleaser.yaml", &cfg, true)
	assert.Equal(t, 2, cfg.Version)
	assert.Equal(t, "flow-gateway", cfg.ProjectName, "nome do binário e dos pacotes")
	assert.Equal(t, "auto", cfg.Release.Prerelease, "v1.0.0-rc.1 sai como pré-release")

	type step struct {
		Uses, Run string
		With      map[string]any
		Env       map[string]string
	}
	var wf struct {
		On          map[string]any `yaml:"on"`
		Permissions map[string]string
		Jobs        map[string]struct {
			Needs, If      string
			Permissions    map[string]string
			TimeoutMinutes int `yaml:"timeout-minutes"`
			Steps          []step
		}
	}
	decode(t, ".github/workflows/release.yml", &wf, false)
	assert.Equal(t, map[string]any{"push": map[string]any{"tags": []any{"v*"}}, "pull_request": nil}, wf.On,
		"disparo: a tag v* (que publica) e o PR (que só prova)")
	assert.Equal(t, map[string]string{"contents": "read"}, wf.Permissions)
	require.ElementsMatch(t, []string{"snapshot", "publish"}, slices.Collect(maps.Keys(wf.Jobs)))

	pub := wf.Jobs["publish"]
	assert.Equal(t, "snapshot", pub.Needs, "a tag só publica depois do snapshot verde")
	assert.Equal(t, "github.ref_type == 'tag'", pub.If)
	assert.Equal(t, map[string]string{"contents": "write"}, pub.Permissions, "o único job com escrita")
	assert.Empty(t, wf.Jobs["snapshot"].Permissions, "o snapshot herda o contents: read")
	assert.True(t, slices.ContainsFunc(wf.Jobs["snapshot"].Steps, func(s step) bool {
		return s.Env["RELEASE_DIST"] != "" && strings.Contains(s.Run, "grep -q '^--- PASS: TestSnapshot '")
	}), "snapshot: o TestSnapshot roda com RELEASE_DIST, e o grep reprova o teste pulado")
	for name, job := range wf.Jobs {
		assert.Positive(t, job.TimeoutMinutes, "%s: timeout-minutes", name)
		var runs []string
		for _, s := range job.Steps {
			assert.NotContains(t, s.Run, "${{", "%s: contexto entra no script só por env:", name)
			if strings.HasPrefix(s.Uses, "actions/checkout@") {
				assert.Equal(t, false, s.With["persist-credentials"], "%s: checkout sem deixar o token no .git", name)
			}
			if strings.HasPrefix(s.Uses, "actions/setup-go@") {
				assert.Equal(t, false, s.With["cache"], "%s: workflow que publica não restaura cache", name)
			}
			if strings.HasPrefix(s.Run, "goreleaser ") {
				runs = append(runs, s.Run)
			}
		}
		want := []string{"goreleaser check", "goreleaser release --snapshot --clean"}
		if name == "publish" {
			want = []string{"goreleaser release --clean"}
		}
		assert.Equal(t, want, runs, "%s: chamadas do goreleaser", name)
	}
}

// TestSnapshot confere o dist/ de um `goreleaser release --snapshot`: um pacote por alvo do
// `make cross`, o checksums cobrindo cada um e, dentro de cada pacote, o binário certo: o
// cmd/flow-gateway do alvo, sem cgo, com -trimpath e a versão injetada. O binário da
// plataforma do teste roda: o linker ignora em silêncio um -X com caminho errado.
func TestSnapshot(t *testing.T) {
	dist := os.Getenv("RELEASE_DIST")
	if dist == "" {
		t.Skip("sem RELEASE_DIST (o dist/ de um goreleaser release --snapshot)")
	}
	if !filepath.IsAbs(dist) {
		dist = filepath.Join(root, dist)
	}
	var meta struct{ Version, Commit string }
	data, err := os.ReadFile(filepath.Join(dist, "metadata.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &meta))
	require.NotEmpty(t, meta.Version)

	data, err = os.ReadFile(filepath.Join(dist, "flow-gateway_"+meta.Version+"_checksums.txt"))
	require.NoError(t, err)
	sums := map[string]string{}
	for _, l := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		sum, name, ok := strings.Cut(l, "  ")
		require.True(t, ok, "linha do checksums: %q", l)
		sums[name] = sum
	}

	targets := map[string]string{} // pacote -> GOOS/GOARCH
	for _, p := range platforms(t) {
		goos, goarch, _ := strings.Cut(p, "/")
		ext := ".tar.gz"
		if goos == "windows" {
			ext = ".zip"
		}
		targets["flow-gateway_"+meta.Version+"_"+goos+"_"+goarch+ext] = p
	}
	require.ElementsMatch(t, slices.Collect(maps.Keys(targets)), slices.Collect(maps.Keys(sums)), "checksums: um por pacote")

	ran := false
	for name, p := range targets {
		data, err := os.ReadFile(filepath.Join(dist, name))
		require.NoError(t, err)
		sum := sha256.Sum256(data)
		assert.Equal(t, hex.EncodeToString(sum[:]), sums[name], "SHA-256 de %s", name)

		goos, goarch, _ := strings.Cut(p, "/")
		exe := "flow-gateway"
		if goos == "windows" {
			exe += ".exe"
		}
		files := unpack(t, name, data)
		assert.ElementsMatch(t, []string{exe, "README.md"}, slices.Collect(maps.Keys(files)), name)
		bin := files[exe]
		bi, err := buildinfo.Read(bytes.NewReader(bin))
		require.NoError(t, err, name)
		require.Equal(t, module+"/cmd/flow-gateway", bi.Path, name) // outro main não roda abaixo
		settings := map[string]string{}
		for _, s := range bi.Settings {
			settings[s.Key] = s.Value
		}
		for k, v := range map[string]string{"GOOS": goos, "GOARCH": goarch, "CGO_ENABLED": "0", "-trimpath": "true"} {
			assert.Equal(t, v, settings[k], "%s: %s", name, k)
		}
		assert.True(t, bytes.Contains(bin, []byte("v"+meta.Version)), "%s: versão injetada pelo -X", name)

		if goos != runtime.GOOS || goarch != runtime.GOARCH {
			continue
		}
		ran = true
		path := filepath.Join(t.TempDir(), exe)
		require.NoError(t, os.WriteFile(path, bin, 0o755))
		out, err := exec.Command(path, "--version").CombinedOutput()
		require.NoError(t, err, "%s", out)
		m := regexp.MustCompile(`^flow-gateway v(\S+) \(([0-9a-f]{7,40})\)\n$`).FindStringSubmatch(string(out))
		require.NotNil(t, m, "--version: %q", out)
		assert.Equal(t, meta.Version, m[1], "versão")
		assert.True(t, strings.HasPrefix(meta.Commit, m[2]), "commit %s, do build %s", m[2], meta.Commit)
	}
	assert.True(t, ran, "nenhum pacote para %s/%s: o binário não rodou", runtime.GOOS, runtime.GOARCH)
}

// unpack devolve o conteúdo de um .tar.gz ou .zip, por nome de arquivo.
func unpack(t *testing.T, name string, data []byte) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	if strings.HasSuffix(name, ".zip") {
		zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err, name)
		for _, f := range zr.File {
			rc, err := f.Open()
			require.NoError(t, err, name)
			files[f.Name], err = io.ReadAll(rc)
			require.NoError(t, errors.Join(err, rc.Close()), name)
		}
		return files
	}
	gz, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err, name)
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return files
		}
		require.NoError(t, err, name)
		files[h.Name], err = io.ReadAll(tr)
		require.NoError(t, err, name)
	}
}
