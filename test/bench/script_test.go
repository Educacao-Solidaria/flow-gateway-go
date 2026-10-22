package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScript roda o run.sh de verdade num repositório de mentira: a base tem um
// benchmark curto e o PR (a árvore de trabalho) o deixa cem vezes mais lento ou
// o quebra. Cobre o que o teste do portão não alcança: qual lado é a base, a
// ordem das colunas que chegam ao portão e o código de saída que chega ao job.
// Um run.sh que troque os lados, engula a saída ou tome a falha do PR por aviso
// deixaria o job verde. Precisa de Linux, bash, git e benchstat: o job bench o
// roda depois de instalar o benchstat; sem eles (a matriz do ci.yml), pula.
func TestScript(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("o run.sh é para Linux")
	}
	for _, tool := range []string{"bash", "git", "benchstat"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("sem %s no PATH", tool)
		}
	}
	dir := t.TempDir()
	write := func(name string, data []byte) {
		p := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, data, 0o644))
	}
	for _, f := range []string{"run.sh", "main.go"} {
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		write(filepath.Join("test", "bench", f), data)
	}
	bench := func(body string) []byte {
		return []byte("package x\n\nimport \"testing\"\n\nvar sink int\n\nfunc BenchmarkWork(b *testing.B) {\n" + body + "}\n")
	}
	loop := func(n int) string {
		return fmt.Sprintf("\tfor range b.N {\n\t\tfor i := range %d {\n\t\t\tsink += i\n\t\t}\n\t}\n", n)
	}
	write("go.mod", []byte("module example.com/x\n\ngo 1.23\n"))
	write(filepath.Join("x", "x_test.go"), bench(loop(50)))
	git := func(args ...string) string {
		out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).Output()
		require.NoError(t, err, "git %v", args)
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("add", "-A")
	base := git("write-tree") // a base é a árvore do índice: sem commit, sem identidade nem hooks

	for _, tc := range []struct {
		name, pr string
		code     int
		want     string // na saída padrão; vazio = saída vazia (parou antes do veredito)
	}{
		{"piora reprova", loop(5000), 1, "| example.com/x/x | Work"},
		{"falha do PR encerra", "\tb.Fatal(\"quebrado\")\n", 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			write(filepath.Join("x", "x_test.go"), bench(tc.pr))
			cmd := exec.Command("bash", "test/bench/run.sh")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "BASE="+base, "COUNT=4", "BENCHTIME=20ms",
				"BENCH=", "LIMIT=", "ACCEPT=", "GITHUB_STEP_SUMMARY=")
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, stderr.String())
			assert.Equal(t, tc.code, exit.ExitCode(), stderr.String())
			if tc.want == "" {
				assert.Empty(t, stdout.String())
				assert.Contains(t, stderr.String(), "quebrado")
			} else {
				assert.Contains(t, stdout.String(), tc.want)
			}
		})
	}
}
