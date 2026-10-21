package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestObserveBuckets(t *testing.T) {
	last := len(bounds) // o bucket de quem passa do maior limite
	for _, tc := range []struct {
		d    time.Duration
		want int
	}{
		{-time.Second, 0}, {0, 0}, {time.Millisecond, 0}, {time.Millisecond + 1, 1},
		{800 * time.Millisecond, 8}, {time.Second, 9}, {5 * time.Minute, last - 1}, {5*time.Minute + 1, last}, {time.Hour, last},
	} {
		var l Latency
		l.Observe(tc.d)
		s := l.Snapshot()
		assert.EqualValues(t, 1, s.Buckets[tc.want], "%s", tc.d)
		assert.EqualValues(t, 1, s.Count, "%s", tc.d)
	}
	b := Bounds()
	assert.Equal(t, 5*time.Minute, b[len(b)-1])
	b[0] = time.Hour
	assert.Equal(t, time.Millisecond, Bounds()[0], "Bounds devolve cópia")
}

func TestSnapshotSum(t *testing.T) {
	var l Latency
	for _, d := range []time.Duration{1500 * time.Microsecond, time.Second, 999 * time.Nanosecond, -time.Second} {
		l.Observe(d)
	}
	s := l.Snapshot()
	assert.EqualValues(t, 1_001_500, s.SumMicros, "µs inteiros; a fração e o negativo não somam")
	assert.EqualValues(t, 4, s.Count)
	assert.Zero(t, s.Active)
}

// TestMiddlewareGaugeUnderLoad põe 1.000 requisições dentro do handler ao mesmo
// tempo; um terço entra em panic e um terço só volta pelo cancelamento. No pico
// o gauge conta todas e, no fim, volta a zero com todas registradas.
func TestMiddlewareGaugeUnderLoad(t *testing.T) {
	const n = 1000
	var l Latency
	var inside sync.WaitGroup
	inside.Add(n)
	release := make(chan struct{})
	h := Middleware(&l)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		inside.Done()
		<-release
		switch r.URL.Path {
		case "/panic":
			panic("handler quebrou")
		case "/cancel":
			<-r.Context().Done()
		}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var panics atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		path := [...]string{"/", "/panic", "/cancel"}[i%3]
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					panics.Add(1)
				}
			}()
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx))
		}()
	}
	inside.Wait()
	assert.EqualValues(t, n, l.Snapshot().Active, "todas em andamento ao mesmo tempo")
	close(release)
	cancel() // as de /cancel só voltam por aqui
	wg.Wait()
	s := l.Snapshot()
	assert.Zero(t, s.Active, "o gauge volta a zero, com panic e cancelamento")
	assert.EqualValues(t, n, s.Count)
	assert.EqualValues(t, (n+1)/3, panics.Load(), "o panic segue subindo")
}

// TestSnapshotUnderConcurrentObserve lê snapshots enquanto 8 goroutines
// registram: nenhuma leitura volta para trás, e no fim os totais são exatos.
func TestSnapshotUnderConcurrentObserve(t *testing.T) {
	const writers, per = 8, 10_000
	var l Latency
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				l.Observe(time.Duration(i%4) * 10 * time.Millisecond) // 0, 10, 20 e 30 ms
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	var prev Snapshot
	for reading := true; reading; {
		select {
		case <-done:
			reading = false
		default:
		}
		s := l.Snapshot()
		assert.GreaterOrEqual(t, s.Count, prev.Count)
		assert.GreaterOrEqual(t, s.SumMicros, prev.SumMicros)
		for i := range s.Buckets {
			assert.GreaterOrEqual(t, s.Buckets[i], prev.Buckets[i], "bucket %d", i)
		}
		prev = s
	}
	assert.EqualValues(t, writers*per, prev.Count)
	assert.EqualValues(t, writers*per/4*(10_000+20_000+30_000), prev.SumMicros)
	assert.EqualValues(t, writers*per/4, prev.Buckets[2], "10ms cai no bucket de até 10ms")
}
