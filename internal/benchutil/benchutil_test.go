package benchutil

import (
	"flag"
	"io"
	"net/http"
	"strings"
	"testing"
)

// quick encurta testing.Benchmark para não gastar 1 s por chamada no go test.
func quick(t *testing.T) {
	f := flag.Lookup("test.benchtime")
	old := f.Value.String()
	if err := f.Value.Set("200x"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Value.Set(old) })
}

var sink []byte

func TestHandlerReportsAllocsAndThroughput(t *testing.T) {
	quick(t)
	body := []byte(`{"model":"x"}`)
	var short int
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body) // aloca: o benchmark tem de ver
		if string(data) != string(body) {
			short++
		}
		sink = data
		w.Header().Set("X-Ok", "1")
	})
	res := testing.Benchmark(func(b *testing.B) { Handler(b, h, http.MethodPost, "/", body) })

	if res.N != 200 || short != 0 {
		t.Fatalf("N=%d, leituras incompletas=%d: o corpo não foi rebobinado entre iterações", res.N, short)
	}
	if res.AllocsPerOp() < 1 {
		t.Errorf("allocs/op = %d, esperava >= 1", res.AllocsPerOp())
	}
	if res.Extra["req/s"] <= 0 || res.Bytes != int64(len(body)) {
		t.Errorf("métricas de vazão ausentes: extra=%v bytes=%d", res.Extra, res.Bytes)
	}
}

func TestHandlerFailsOnNon2xx(t *testing.T) {
	quick(t)
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	res := testing.Benchmark(func(b *testing.B) { Handler(b, h, http.MethodGet, "/", nil) })
	if res.N != 0 { // testing.Benchmark devolve resultado zerado quando o benchmark falha
		t.Errorf("benchmark com resposta 418 deveria falhar, N=%d", res.N)
	}
}

func TestCompareRunsVariantsInOrder(t *testing.T) {
	quick(t)
	var order []string
	res := testing.Benchmark(func(b *testing.B) {
		Compare(b, 4,
			Variant{"concat", func() { order = append(order, "concat") }},
			Variant{"builder", func() { var sb strings.Builder; sb.WriteString("ab"); order = append(order, "builder") }},
		)
	})
	if res.N == 0 || len(order) == 0 || order[0] != "concat" || order[len(order)-1] != "builder" {
		t.Errorf("variantes fora de ordem ou não executadas: N=%d %v", res.N, order[:min(len(order), 3)])
	}
}
