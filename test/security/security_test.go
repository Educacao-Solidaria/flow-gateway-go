package security

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// root é a raiz do repositório.
const root = "../.."

// As chamadas do portão no security.yml; o `make audit` repete as duas primeiras. Afrouxar a
// auditoria (outra severidade, sem --ignore-unfixed ou --exit-code, um -exclude ou um
// --skip-dirs) muda uma destas linhas: a mudança tem de aparecer também aqui.
const (
	gosecRun   = `gosec -terse -severity high -nosec-require-rules -nosec-require-justification ./...`
	trivyGate  = `--severity HIGH,CRITICAL --ignore-unfixed --ignorefile .trivyignore --show-suppressed --exit-code 1`
	trivyFS    = `trivy fs --scanners vuln,secret,misconfig ` + trivyGate + ` .`
	trivyImage = `trivy image --scanners vuln,secret ` + trivyGate + ` "$GATEWAY_IMAGE"`
)

// read devolve o arquivo name da raiz com fim de linha LF (o checkout do Windows traz CRLF).
func read(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, name))
	require.NoError(t, err)
	return strings.ReplaceAll(string(data), "\r\n", "\n")
}

// TestWorkflow confere o security.yml e o `make audit` contra o contrato da Fase 5 (§6).
func TestWorkflow(t *testing.T) {
	type step struct {
		Name, Uses, Run, If string
		With                map[string]any
	}
	// KnownFields: chave que a struct não declara reprova. Um continue-on-error (o portão
	// vermelho passaria), um if ou permissions no job ficam de fora até entrar aqui.
	var wf struct {
		Name                  string
		On                    map[string]any `yaml:"on"`
		Permissions           map[string]string
		Concurrency, Defaults map[string]any
		Env                   map[string]string
		Jobs                  map[string]struct {
			RunsOn         string `yaml:"runs-on"`
			TimeoutMinutes int    `yaml:"timeout-minutes"`
			Env            map[string]string
			Steps          []step
		}
	}
	raw := read(t, ".github/workflows/security.yml")
	dec := yaml.NewDecoder(strings.NewReader(raw))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&wf))
	assert.Equal(t, map[string]any{"push": map[string]any{"branches": []any{"main"}}, "pull_request": nil}, wf.On,
		"todo PR e todo push na main")
	assert.Equal(t, map[string]string{"contents": "read"}, wf.Permissions)
	assert.NotContains(t, raw, "secrets.")
	assert.NotContains(t, raw, "github.token")
	assert.Regexp(t, `^v\d+\.\d+\.\d+$`, wf.Env["GOSEC_VERSION"], "gosec numa versão exata")
	assert.Regexp(t, `^\d+\.\d+\.\d+$`, wf.Env["TRIVY_VERSION"], "trivy numa versão exata")
	assert.Regexp(t, `^[0-9a-f]{64}$`, wf.Env["TRIVY_SHA256"])
	assert.Equal(t, "true", wf.Env["TRIVY_DISABLE_TELEMETRY"], "o trivy não manda uso para a Aqua")
	require.ElementsMatch(t, []string{"gosec", "trivy"}, slices.Collect(maps.Keys(wf.Jobs)))

	runs := map[string][]string{} // job -> scripts, na ordem
	for name, job := range wf.Jobs {
		assert.Positive(t, job.TimeoutMinutes, "%s: timeout-minutes", name)
		for _, s := range job.Steps {
			assert.NotContains(t, s.Run, "${{", "%s: contexto entra no script só por env:", name)
			// Um `if: false` (ou só no push) pularia o portão no PR sem ninguém ver.
			assert.Contains(t, []string{"", "${{ !cancelled() }}"}, s.If, "%s: passo condicional", name)
			switch {
			case strings.HasPrefix(s.Uses, "actions/checkout@"):
				assert.Equal(t, false, s.With["persist-credentials"], "%s: checkout sem deixar o token no .git", name)
			case strings.HasPrefix(s.Uses, "docker/build-push-action@"):
				assert.Equal(t, false, s.With["push"], "a imagem não sai do runner")
			case s.Run != "":
				runs[name] = append(runs[name], s.Run)
			}
		}
	}
	assert.Equal(t, []string{`go install "github.com/securego/gosec/v2/cmd/gosec@$GOSEC_VERSION"`, gosecRun}, runs["gosec"])
	require.Len(t, runs["trivy"], 3)
	assert.Contains(t, runs["trivy"][0], "https://github.com/aquasecurity/trivy/releases/download/v$TRIVY_VERSION/")
	assert.Contains(t, runs["trivy"][0], `echo "$TRIVY_SHA256  $RUNNER_TEMP/trivy.tgz" | sha256sum -c -`, "SHA-256 conferido")
	assert.Equal(t, []string{trivyFS, trivyImage}, runs["trivy"][1:])

	m := regexp.MustCompile(`(?m)^audit:.*\n((?:\t.*\n)+)`).FindStringSubmatch(read(t, "Makefile"))
	require.NotNil(t, m, "alvo audit no Makefile")
	assert.Equal(t, "\t"+gosecRun+"\n\t"+trivyFS+"\n", m[1], "make audit = as chamadas do CI")
}

// review é a data de revisão no motivo de uma anotação do gosec.
var review = regexp.MustCompile(`(?i)revisar em (\d{4}-\d{2}-\d{2})`)

func validDate(s string) bool {
	_, err := time.Parse(time.DateOnly, s)
	return err == nil
}

// trivyignoreProblems devolve o que está fora do formato do .trivyignore: cada linha que não é
// comentário nem vazia é `<id> exp:AAAA-MM-DD` (na data, o trivy deixa de aplicar a exceção),
// com o motivo num comentário logo acima, sem linha vazia no meio.
func trivyignoreProblems(text string) []string {
	entry := regexp.MustCompile(`^(\S+) exp:(\d{4}-\d{2}-\d{2})$`)
	var problems []string
	reason := false
	for i, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			reason = line != ""
			continue
		}
		switch m := entry.FindStringSubmatch(line); {
		case m == nil:
			problems = append(problems, fmt.Sprintf("linha %d: %q fora do formato '<id> exp:AAAA-MM-DD'", i+1, line))
		case !reason:
			problems = append(problems, fmt.Sprintf("linha %d: %s sem o motivo num comentário logo acima", i+1, m[1]))
		case !validDate(m[2]):
			problems = append(problems, fmt.Sprintf("linha %d: data %s inválida", i+1, m[2]))
		}
	}
	return problems
}

// nosecProblems devolve as anotações do gosec (#nosec ou //gosec:disable) do código Go src fora
// do formato `#nosec G### -- motivo ... revisar em AAAA-MM-DD`. A anotação vai da linha que
// começa com a marca até o fim do bloco de comentário (o motivo pode ocupar várias linhas).
func nosecProblems(src string) ([]string, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	var problems []string
	for _, g := range f.Comments {
		var note []string
		for _, c := range g.List {
			line := strings.TrimSpace(strings.TrimLeft(c.Text, "/*")) // `//` e `/* */`, como o gosec
			if len(note) > 0 || strings.HasPrefix(line, "#nosec") || strings.HasPrefix(line, "gosec:disable") {
				note = append(note, line)
			}
		}
		if len(note) == 0 {
			continue
		}
		rules, why, ok := strings.Cut(strings.Join(note, " "), "--")
		d := review.FindStringSubmatch(why)
		if !ok || !regexp.MustCompile(`\bG\d{3}\b`).MatchString(rules) || d == nil || !validDate(d[1]) {
			problems = append(problems, fmt.Sprintf("linha %d: %q sem 'G### -- motivo ... revisar em AAAA-MM-DD'",
				fset.Position(g.Pos()).Line, note[0]))
		}
	}
	return problems, nil
}

// TestExceptionRules confere os dois validadores com exceções boas e ruins.
func TestExceptionRules(t *testing.T) {
	assert.Empty(t, trivyignoreProblems("# cabeçalho\n\n# motivo\nCVE-1 exp:2027-01-22\nCVE-2 exp:2027-01-22\n\n# outro\nGHSA-x exp:2027-02-01\n"))
	for _, bad := range []string{
		"CVE-1 exp:2027-01-22\n",             // sem motivo
		"# motivo\n\nCVE-1 exp:2027-01-22\n", // motivo separado por linha vazia
		"# motivo\nCVE-1\n",                  // sem data: valeria para sempre
		"# motivo\nCVE-1 exp:2027-13-01\n",   // data inválida
		"# motivo\nCVE-1 exp:2027-01-22 \n",  // sobra no fim
	} {
		assert.Len(t, trivyignoreProblems(bad), 1, "%q", bad)
	}

	nosec := func(comment string) []string {
		problems, err := nosecProblems("package p\n\nfunc f() {\n" + comment + "\n\t_ = 0\n}\n")
		require.NoError(t, err, comment)
		return problems
	}
	for _, good := range []string{
		"// #nosec G404 -- id que não é segredo. Revisar em 2027-01-22.",
		"// #nosec G115 G404 -- motivo que ocupa\n// duas linhas. Revisar em 2027-01-22.",
		"//gosec:disable G404 -- motivo, revisar em 2027-01-22",
		"/* #nosec G404 -- motivo, revisar em 2027-01-22 */",
		"// prosa que cita um #nosec no meio não é anotação",
	} {
		assert.Empty(t, nosec(good), good)
	}
	for _, bad := range []string{
		"// #nosec",
		"// #nosec G404",
		"// #nosec -- sem regra, revisar em 2027-01-22",
		"// #nosec G404 -- sem data",
		"// #nosec G404 -- revisar em 2027-02-30",
		"//gosec:disable G404 -- sem data",
		"/* #nosec G404 -- sem data */",
		"// explicação antes\n// #nosec G404 -- sem data",
	} {
		assert.Len(t, nosec(bad), 1, bad)
	}
}

// TestExceptions confere cada exceção do repositório (contrato da Fase 5, §6): o .trivyignore e
// toda anotação do gosec no código Go, com motivo e data de revisão.
func TestExceptions(t *testing.T) {
	assert.Empty(t, trivyignoreProblems(read(t, ".trivyignore")))
	files := 0
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case d.IsDir() && (d.Name() == ".git" || d.Name() == "testdata"):
			return filepath.SkipDir
		case d.IsDir() || filepath.Ext(path) != ".go":
			return nil
		}
		files++
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		problems, err := nosecProblems(string(src))
		assert.NoError(t, err, path)
		assert.Empty(t, problems, path)
		return nil
	}))
	assert.Positive(t, files, "arquivos .go percorridos")
}
