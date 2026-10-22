// Command bench é o portão de regressão do workflow bench.yml (o run.sh desta
// pasta o chama): lê da entrada o CSV do benchstat com duas colunas, base e PR
// nesta ordem, e escreve o veredito em Markdown.
//
// Regra: reprova (sai 1) quando, em sec/op, B/op ou allocs/op, o benchstat vê
// diferença significativa (p ≤ 0,05; o "~" marca a falta dela) e a mediana do PR
// fica mais de -limit por cento acima da mediana da base, com a piora arredondada
// a 2 casas, como o benchstat mostra. Base 0 que deixa de ser 0 (alocação nova
// num caminho que não alocava) é piora infinita. As unidades de vazão (B/s,
// req/s) são o inverso do tempo e ficam fora. Com -accept (a label bench-ok no
// PR) a piora é aceita: o veredito diz isso e sai 0. Entrada sem nenhuma linha
// com os dois lados sai 2, porque comparar nada não pode passar.
package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

// gated são as unidades que o portão julga.
var gated = map[string]bool{"sec/op": true, "B/op": true, "allocs/op": true}

// worse é uma linha do benchstat que piorou além do limite.
type worse struct {
	pkg, name, unit string
	base, pr, pct   float64
	p               string // a coluna P do benchstat ("p=0.000 n=10")
}

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout)) }

func run(args []string, in io.Reader, out io.Writer) int {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(out)
	limit := fs.Float64("limit", 10, "piora máxima aceita, em % sobre a mediana da base")
	accept := fs.Bool("accept", false, "aceita a piora (a label bench-ok está no PR)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if math.IsNaN(*limit) || math.IsInf(*limit, 0) || *limit < 0 {
		fmt.Fprintf(out, "-limit %v: use um número finito >= 0\n", *limit)
		return 2
	}
	found, compared, err := compare(in, *limit)
	if err == nil && compared == 0 {
		err = errors.New("nenhum benchmark com base e PR: nada a comparar")
	}
	if err != nil {
		fmt.Fprintf(out, "### Benchmarks: erro\n\n%v\n", err)
		return 2
	}

	fmt.Fprintf(out, "### Benchmarks do PR contra a base (limite: +%g%%)\n\n", *limit)
	switch {
	case len(found) == 0:
		fmt.Fprintf(out, "Nenhuma piora significativa acima do limite: %d comparação(ões) de sec/op, B/op e allocs/op.\n", compared)
		return 0
	case *accept:
		fmt.Fprintf(out, "**%d piora(s) aceita(s) pela label `bench-ok`**: a justificativa fica no PR.\n\n", len(found))
	default:
		fmt.Fprintf(out, "**%d piora(s) significativa(s) acima do limite.** Se for justificada, explique no PR, "+
			"ponha a label `bench-ok` e rode o job de novo.\n\n", len(found))
	}
	fmt.Fprintln(out, "| pacote | benchmark | unidade | base | PR | piora | P |\n|---|---|---|---|---|---|---|")
	for _, w := range found {
		fmt.Fprintf(out, "| %s | %s | %s | %.4g | %.4g | %+.2f%% | %s |\n", w.pkg, w.name, w.unit, w.base, w.pr, w.pct, w.p)
	}
	if *accept {
		return 0
	}
	return 1
}

// compare lê o CSV do benchstat e devolve as linhas que pioraram mais que
// limit, e quantas linhas das unidades julgadas tinham os dois lados.
func compare(r io.Reader, limit float64) ([]worse, int, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1 // cabeçalhos e linhas de um lado só têm larguras diferentes
	var (
		found     []worse
		compared  int
		pkg, unit string
	)
	for {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return found, compared, nil
		}
		if err != nil {
			return nil, 0, err
		}
		switch {
		case len(rec) == 1: // configuração antes das tabelas de cada pacote
			if v, ok := strings.CutPrefix(rec[0], "pkg: "); ok {
				pkg = v
			}
		case rec[0] == "": // cabeçalho: rótulos das colunas ou a unidade
			if len(rec) > 2 && rec[2] == "CI" {
				unit = rec[1]
			}
		case len(rec) == 7 && rec[0] != "geomean" && gated[unit]: // nome, base, CI, PR, CI, delta, P
			compared++
			if rec[5] == "~" {
				continue
			}
			base, err1 := strconv.ParseFloat(rec[1], 64)
			pr, err2 := strconv.ParseFloat(rec[3], 64)
			if err := errors.Join(err1, err2); err != nil {
				return nil, 0, fmt.Errorf("%s %s: %w", rec[0], unit, err)
			}
			// base 0 dá +Inf (piora) ou NaN (PR também 0, nunca maior que o limite)
			if pct := math.Round((pr/base-1)*1e4) / 100; pct > limit {
				found = append(found, worse{pkg, rec[0], unit, base, pr, pct, rec[6]})
			}
		}
	}
}
