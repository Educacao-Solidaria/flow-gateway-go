// Package benchutil é a base dos benchmarks do gateway: todo benchmark sai com
// allocs/op e B/op (alocações de heap) e com vazão (req/s ou MB/s), no mesmo
// formato em todos os pacotes, para o benchstat comparar execuções.
//
//	go test -run '^$' -bench . -count 10 ./... > novo.txt
//	benchstat velho.txt novo.txt
package benchutil

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// discardWriter é um http.ResponseWriter reaproveitado entre iterações, para
// que as alocações medidas sejam as do handler e não as do httptest.Recorder.
type discardWriter struct {
	header http.Header
	status int
}

func (w *discardWriter) Header() http.Header         { return w.header }
func (w *discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardWriter) WriteHeader(code int)        { w.status = code }

// Handler mede h atendendo method target com body (nil para sem corpo) e
// reporta allocs/op, B/op, req/s e, havendo corpo, MB/s. A mesma requisição é
// reaproveitada em toda iteração (o corpo é rebobinado), então h não deve
// guardá-la depois de responder. Falha o benchmark se h não responder 2xx.
func Handler(b *testing.B, h http.Handler, method, target string, body []byte) {
	b.Helper()
	rd := bytes.NewReader(body)
	req := httptest.NewRequest(method, target, rd)
	if body == nil {
		req.Body = http.NoBody
	} else {
		b.SetBytes(int64(len(body)))
	}
	w := &discardWriter{header: http.Header{}}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rd.Reset(body)
		clear(w.header)
		w.status = http.StatusOK
		h.ServeHTTP(w, req)
		if w.status < 200 || w.status > 299 {
			b.Fatalf("%s %s respondeu %d", method, target, w.status)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "req/s")
}

// Variant é uma implementação candidata em Compare.
type Variant struct {
	Name string
	Fn   func()
}

// Compare roda cada variante como sub-benchmark (na ordem dada) com
// ReportAllocs e, se bytesPerOp > 0, SetBytes — para comparar lado a lado
// alocações e vazão de implementações alternativas do mesmo trabalho.
func Compare(b *testing.B, bytesPerOp int64, variants ...Variant) {
	b.Helper()
	for _, v := range variants {
		b.Run(v.Name, func(b *testing.B) {
			if bytesPerOp > 0 {
				b.SetBytes(bytesPerOp)
			}
			b.ReportAllocs()
			for range b.N {
				v.Fn()
			}
		})
	}
}
