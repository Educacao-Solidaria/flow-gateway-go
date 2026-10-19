package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// BenchmarkObserve mede a contenção: todas as goroutines registram no mesmo
// Latency. Rode com -cpu=1,4,16: sem disputa, o ns/op ficaria parado; com
// ela, cresce com os núcleos. "buckets-variados" espalha as latências por
// buckets vizinhos, onde apareceria falso compartilhamento entre eles.
func BenchmarkObserve(b *testing.B) {
	for _, tc := range []struct {
		name string
		step time.Duration // quanto a latência anda a cada observação
	}{{"mesmo-bucket", 0}, {"buckets-variados", 7 * time.Millisecond}} {
		b.Run(tc.name, func(b *testing.B) {
			var l Latency
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				d := 3 * time.Millisecond
				for pb.Next() {
					l.Observe(d)
					d = (d + tc.step) % (10 * time.Second)
				}
			})
		})
	}
}

// BenchmarkMiddleware mede o que o middleware soma a cada requisição.
func BenchmarkMiddleware(b *testing.B) {
	var l Latency
	h := Middleware(&l)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		w := httptest.NewRecorder() // o handler não escreve nada
		for pb.Next() {
			h.ServeHTTP(w, req)
		}
	})
}
