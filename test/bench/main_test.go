package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testdata/benchstat.csv é a saída real do benchstat (a versão do bench.yml) para
// 10 rodadas sintéticas. Em example.com/a: Slower fica 20% mais lento, passa a
// alocar (0 → 8 B/op e 0 → 1 allocs/op) e dobra a vazão em B/s; Limit piora
// exatamente 10,00% nas três unidades (11/10 em float dá 10,000000000000009%);
// Noisy sobe 18% na mediana sem diferença significativa (~); OnlyBase e OnlyPR
// têm um lado só. Em example.com/b, Mixed melhora tempo e bytes e piora 50% nas
// alocações. As linhas geomean também pioram e não contam.
func fixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/benchstat.csv")
	require.NoError(t, err)
	return data
}

func TestCompare(t *testing.T) {
	data := fixture(t)
	for _, tc := range []struct {
		limit float64
		want  []string // pacote, benchmark e unidade de cada piora, na ordem do CSV
	}{
		{10, []string{"a Slower-4 sec/op", "a Slower-4 B/op", "a Slower-4 allocs/op", "b Mixed-4 allocs/op"}},
		{9.99, []string{"a Slower-4 sec/op", "a Limit-4 sec/op", "a Slower-4 B/op", "a Limit-4 B/op",
			"a Slower-4 allocs/op", "a Limit-4 allocs/op", "b Mixed-4 allocs/op"}},
		{1000, []string{"a Slower-4 B/op", "a Slower-4 allocs/op"}}, // base 0: piora infinita
	} {
		found, compared, err := compare(bytes.NewReader(data), tc.limit)
		require.NoError(t, err)
		assert.Equal(t, 12, compared, "Slower, Limit e Noisy em a e Mixed em b, nas 3 unidades")
		var got []string
		for _, w := range found {
			got = append(got, strings.TrimPrefix(w.pkg, "example.com/")+" "+w.name+" "+w.unit)
		}
		assert.Equal(t, tc.want, got, "limite %v", tc.limit)
	}
}

func TestRun(t *testing.T) {
	data := string(fixture(t))
	const header = ",base,,PR,,,\n,sec/op,CI,sec/op,CI,vs base,P\n"
	for _, tc := range []struct {
		name string
		args []string
		in   string
		code int
		want string
	}{
		{"piora reprova", nil, data, 1, "**4 piora(s) significativa(s) acima do limite.**"},
		{"linha da tabela", nil, data, 1, "| example.com/a | Slower-4 | B/op | 0 | 8 | +Inf% | p=0.000 n=10 |"},
		{"label aceita", []string{"-accept"}, data, 0, "**4 piora(s) aceita(s) pela label `bench-ok`**"},
		{"sem piora", nil, header + "Mixed-4,2e-07,0%,1.5e-07,0%,-25.00%,p=0.000 n=10\n", 0, "limite: 1 comparação(ões)"},
		{"vazio", nil, "", 2, "nada a comparar"},
		{"um lado só", nil, header + "OnlyBase-4,1e-08,0%\nOnlyPR-4,,,1e-08,0%\n", 2, "nada a comparar"},
		{"número inválido", nil, header + "X-4,abc,0%,1,0%,+1.00%,p=0.000 n=10\n", 2, `X-4 sec/op: strconv.ParseFloat: parsing "abc"`},
		{"csv cortado depois de uma linha boa", nil, header + "Mixed-4,2e-07,0%,1.5e-07,0%,-25.00%,p=0.000 n=10\n\"X-4,1\n", 2, "quoted-field"},
		{"limite negativo", []string{"-limit", "-1"}, data, 2, "use um número finito"},
		{"limite NaN", []string{"-limit", "NaN"}, data, 2, "use um número finito"},
		{"flag desconhecida", []string{"-x"}, data, 2, "flag provided but not defined"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			assert.Equal(t, tc.code, run(tc.args, strings.NewReader(tc.in), &out))
			assert.Contains(t, out.String(), tc.want)
		})
	}
}
