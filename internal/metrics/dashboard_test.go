package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	promDrop  = regexp.MustCompile(`"(?:[^"\\]|\\.)*"|\{[^}]*\}|\[[^\]]*\]`)
	promGroup = regexp.MustCompile(`\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)`)
	promIdent = regexp.MustCompile(`\b[a-zA-Z_][a-zA-Z0-9_:]*(\s*\()?`)
	promWords = map[string]bool{"bool": true, "offset": true, "and": true, "or": true, "unless": true, "inf": true, "nan": true,
		"group_left": true, "group_right": true} // sem lista de labels, a promGroup não os tira
)

// metricNames devolve as métricas citadas numa expressão PromQL: sem strings,
// seletores de label e de intervalo, e cláusulas de agrupamento, todo
// identificador que sobra e não é chamada de função nem palavra-chave é métrica.
func metricNames(expr string) []string {
	expr = promGroup.ReplaceAllString(promDrop.ReplaceAllString(expr, ""), "")
	var out []string
	for _, m := range promIdent.FindAllStringSubmatch(expr, -1) {
		if m[1] == "" && !promWords[strings.ToLower(m[0])] {
			out = append(out, m[0])
		}
	}
	return out
}

func TestMetricNames(t *testing.T) {
	for expr, want := range map[string][]string{
		`histogram_quantile(0.95, sum by (le, model) (rate(a_bucket{model=~"x|y"}[$__rate_interval])))`: {"a_bucket"},
		`sum(rate(a_total{result="hit"}[5m])) / sum without (code) (rate(b_total[5m] offset 1h))`:       {"a_total", "b_total"},
		`topk(10, c_total) > bool 0 and on (route) group_left d_total unless e:rate5m`:                  {"c_total", "d_total", "e:rate5m"},
		`sum(flow_gatway_http_requests_total) * 1e3`:                                                    {"flow_gatway_http_requests_total"},
	} {
		assert.Equal(t, want, metricNames(expr), expr)
	}
	exported := exportedNames(t) // o controle negativo: nome errado não passa
	assert.False(t, exported["flow_gatway_http_requests_total"])
	assert.False(t, exported["flow_gateway_llm_ttft_seconds"], "histograma só existe como _bucket, _sum e _count")
	assert.True(t, exported["flow_gateway_llm_ttft_seconds_bucket"])
	assert.True(t, exported["flow_gateway_http_requests_total"])
}

// exportedNames são as séries que o Gateway escreve, tiradas do próprio texto.
func exportedNames(t *testing.T) map[string]bool {
	_, types := scrape(t, new(Gateway))
	exported := map[string]bool{}
	for _, typ := range types {
		name, kind, _ := strings.Cut(typ, " ")
		if kind != "histogram" {
			exported[name] = true
			continue
		}
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			exported[name+suffix] = true
		}
	}
	return exported
}

// TestDashboard confere o deploy/grafana/flow-gateway.json: uid fixo, toda
// fonte de dados pela variável ${DS_PROMETHEUS} e toda métrica citada nas
// queries na lista que o Gateway exporta (nome errado não passa).
func TestDashboard(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "grafana", "flow-gateway.json"))
	require.NoError(t, err)
	type ds struct{ Type, UID string }
	var d struct {
		UID        string
		Templating struct {
			List []struct{ Name, Type, Query string }
		}
		Panels []struct {
			ID         int
			Title      string
			Datasource ds
			Targets    []struct {
				Expr       string
				Datasource ds
			}
		}
	}
	require.NoError(t, json.Unmarshal(data, &d))
	assert.Equal(t, "flow-gateway", d.UID)
	require.Len(t, d.Templating.List, 1)
	assert.Equal(t, "DS_PROMETHEUS datasource prometheus", d.Templating.List[0].Name+" "+d.Templating.List[0].Type+" "+d.Templating.List[0].Query)

	exported, ids := exportedNames(t), map[int]bool{}
	for _, p := range d.Panels {
		assert.False(t, ids[p.ID], "id de painel repetido: %d", p.ID)
		ids[p.ID] = true
		want := ds{"prometheus", "${DS_PROMETHEUS}"}
		assert.Equal(t, want, p.Datasource, p.Title)
		require.NotEmpty(t, p.Targets, p.Title)
		for _, tg := range p.Targets {
			assert.Equal(t, want, tg.Datasource, p.Title)
			names := metricNames(tg.Expr)
			assert.NotEmpty(t, names, "%s: nenhuma métrica em %q", p.Title, tg.Expr)
			for _, n := range names {
				assert.True(t, exported[n], "%s: %q não é exportada pelo Gateway", p.Title, n)
			}
		}
	}
}
