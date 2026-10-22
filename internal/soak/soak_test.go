package soak

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil/gatewaytest"
)

const workers = 16 // conexões da carga, cada uma com um pedido por vez

// sample é uma leitura do runtime com a carga parada, logo depois de um runtime.GC().
type sample struct {
	at         time.Duration // desde o início da carga
	heap       uint64        // /gc/heap/live:bytes: o que o GC marcou como vivo
	goroutines uint64        // /sched/goroutines:goroutines
	requests   int64         // pedidos feitos até aqui
}

func TestSoak(t *testing.T) {
	env := os.Getenv("SOAK_DURATION")
	if env == "" {
		t.Skip("sem SOAK_DURATION (ex.: SOAK_DURATION=3m): ver o doc do pacote")
	}
	d, err := time.ParseDuration(env)
	require.NoError(t, err, "SOAK_DURATION")
	every, warmup := min(max(d/500, time.Second), 30*time.Second), d/5
	require.GreaterOrEqual(t, d-warmup, 10*every, "SOAK_DURATION curta demais: a reta precisa de 10 amostras depois do aquecimento")
	if dl, ok := t.Deadline(); ok {
		require.True(t, time.Now().Add(d+time.Minute).Before(dl), "SOAK_DURATION %s não cabe no -timeout do go test", d)
	}

	t.Setenv("FLOW_SERVER_METRICS_USERNAME", "soak")
	t.Setenv("FLOW_SERVER_METRICS_PASSWORD", "soak")
	fs := pflag.NewFlagSet("soak", pflag.ContinueOnError)
	config.RegisterFlags(fs)
	require.NoError(t, fs.Parse([]string{"--server.addr=127.0.0.1:0"}))
	cfg, err := config.Load(fs) // os padrões do gateway (o pool de conexões com o upstream, por exemplo)
	require.NoError(t, err)
	log, err := logger.New(io.Discard, "info", "json") // o log de cada pedido é montado, como em produção, e descartado
	require.NoError(t, err)
	st, err := gatewaytest.Start(cfg, gatewaytest.Fakes{TTFT: 2 * time.Millisecond, Gap: time.Millisecond, Chunks: 10}, log)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, st.Close()) })
	hc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: workers}}
	url := st.URL + "/v1/chat/completions"
	require.NoError(t, call(context.Background(), hc, url, 1)) // abre a conexão do udscache, que fica até o Close
	base := quiet(hc, st)

	var mu sync.RWMutex // os pedidos levam a leitura; a amostra, a escrita: com ela, o cliente não tem pedido em voo
	var count, failed atomic.Int64
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { stop(); wg.Wait() }() // require falhando no meio não deixa a carga rodando
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				mu.RLock()
				err := call(ctx, hc, url, count.Add(1))
				mu.RUnlock()
				if err != nil && ctx.Err() == nil && failed.Add(1) == 1 {
					t.Logf("primeiro erro: %v", err)
				}
			}
		}()
	}

	start := time.Now()
	read := func() sample {
		mu.Lock()
		defer mu.Unlock()
		runtime.GC()
		m := []metrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/sched/goroutines:goroutines"}}
		metrics.Read(m)
		return sample{time.Since(start), m[0].Value.Uint64(), m[1].Value.Uint64(), count.Load()}
	}
	samples := append(make([]sample, 0, int(d/every)+2), read())
	logEvery := max(1, int(d/every)/12) // umas 12 linhas no log e no relatório
	t.Logf("%d conexões; amostra a cada %s, aquecimento %s\n| tempo | heap vivo | goroutines | pedidos |\n%s", workers, every, warmup, row(samples[0]))
	tick := time.NewTicker(every)
	defer tick.Stop()
	for time.Since(start) < d {
		<-tick.C
		require.NoError(t, scrape(hc, st.URL+"/metrics"))
		if samples = append(samples, read()); (len(samples)-1)%logEvery == 0 {
			t.Log(row(samples[len(samples)-1]))
		}
	}
	stop()
	wg.Wait()

	// até 10 s para voltar à base, fechando as conexões ociosas a cada volta: um pedido
	// cancelado que ainda terminava devolve a sua ao pool depois do primeiro fechamento
	end := runtime.NumGoroutine()
	for limit := time.Now().Add(10 * time.Second); end > base && time.Now().Before(limit); end = runtime.NumGoroutine() {
		hc.CloseIdleConnections()
		st.CloseIdleConnections()
		time.Sleep(10 * time.Millisecond)
	}
	if end > base {
		var b strings.Builder
		_ = pprof.Lookup("goroutine").WriteTo(&b, 1)
		t.Logf("goroutines que não saíram:\n%s", b.String())
	}
	runtime.GC()
	heapProfile(t)

	steady := samples
	for len(steady) > 0 && steady[0].at < warmup {
		steady = steady[1:]
	}
	rise, mean := growth(steady)
	tolerance := max(1<<20, mean/20)
	reqs := samples[len(samples)-1].requests
	report(t, d, samples, logEvery,
		fmt.Sprintf("- heap vivo depois do aquecimento (%s): média %.2f MiB; a reta sobe %+.3f MiB (tolerância %.2f MiB)", warmup, mib(mean), mib(rise), mib(tolerance)),
		fmt.Sprintf("- goroutines: base %d, depois da carga %d", base, end),
		fmt.Sprintf("- pedidos: %d (%.0f/s), com erro: %d; cachewriter: %+v", reqs, float64(reqs)/d.Seconds(), failed.Load(), st.Writer.Stats()))
	assert.LessOrEqual(t, rise, tolerance, "o heap vivo cresceu depois do aquecimento: vazamento")
	assert.LessOrEqual(t, end, base, "goroutines acima da base com a carga parada: vazamento")
	assert.LessOrEqual(t, failed.Load(), reqs/10000, "mais de 0,01% dos pedidos sem a resposta esperada")
	assert.LessOrEqual(t, st.Writer.Stats().Stored, uint64(reqs*55/100), "o cache recebeu stream de quem desistiu ou terminou em erro")
}

// heapProfile grava em SOAK_PROFILE_DIR (se houver) o perfil do heap da última
// coleta, para achar o que vazou: go tool pprof -sample_index=inuse_space heap.pb.gz.
func heapProfile(t *testing.T) {
	dir := os.Getenv("SOAK_PROFILE_DIR")
	if dir == "" {
		return
	}
	f, err := os.Create(filepath.Join(dir, "heap.pb.gz"))
	if assert.NoError(t, err) {
		assert.NoError(t, errors.Join(pprof.WriteHeapProfile(f), f.Close()))
	}
}

// call faz o pedido i da mistura e confere a resposta. A cada 10: 2 saem do
// cache, 5 vão ao upstream até o fim, 1 desiste depois do 1º pedaço (o cliente
// que cai no meio do stream) e 2 terminam no evento de erro — 503 em toda
// tentativa (a resiliência repete) e o upstream que cai depois do 1º pedaço. O
// prompt é único: um mapa por prompt que nunca encolhe aparece no heap.
func call(ctx context.Context, hc *http.Client, url string, i int64) error {
	prompt, want := "pergunta "+strconv.FormatInt(i, 10), `"source":"upstream"` // o source só vem no evento done
	switch i % 10 {
	case 0, 5:
		prompt, want = gatewaytest.HitPrefix+" "+prompt, `"source":"cache"`
	case 7:
		prompt, want = gatewaytest.FailPrefix+" "+prompt, "event: error\n"
	case 9:
		prompt, want = gatewaytest.CutPrefix+" "+prompt, "event: error\n"
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url,
		strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"`+prompt+`"}]}`))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s", res.Status)
	}
	if i%10 == 3 { // o cabeçalho chega com o 1º pedaço; o Close sozinho drenaria o resto por até 50 ms (e o stream acaba antes): o cancelamento derruba a conexão
		_, err := io.ReadFull(res.Body, make([]byte, 16))
		cancel()
		return err
	}
	body, err := io.ReadAll(res.Body)
	if err == nil && !bytes.Contains(body, []byte(want)) {
		err = fmt.Errorf("resposta sem %q: %q", want, body)
	}
	return err
}

// scrape lê o /metrics como o Prometheus.
func scrape(hc *http.Client, url string) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth("soak", "soak")
	res, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, err = io.Copy(io.Discard, res.Body)
	if err == nil && res.StatusCode != http.StatusOK {
		err = fmt.Errorf("/metrics: %s", res.Status)
	}
	return err
}

// quiet fecha as conexões ociosas da carga e do gateway e devolve as
// goroutines quando a contagem para de mudar por 100 ms (as de uma conexão
// fechada saem sozinhas, sem aviso).
func quiet(hc *http.Client, st *gatewaytest.Stack) int {
	n := runtime.NumGoroutine()
	for range 50 {
		hc.CloseIdleConnections()
		st.CloseIdleConnections()
		time.Sleep(100 * time.Millisecond)
		if m := runtime.NumGoroutine(); m != n {
			n = m
			continue
		}
		break
	}
	return n
}

// growth ajusta uma reta de mínimos quadrados ao heap vivo de s e devolve
// quanto ela sobe da primeira à última amostra (bytes; negativo = desceu) e a
// média do heap. Sem dois instantes distintos, não há reta: sobe 0.
func growth(s []sample) (rise, mean float64) {
	if len(s) == 0 {
		return 0, 0
	}
	var mt float64
	for _, x := range s {
		mt += x.at.Seconds() / float64(len(s))
		mean += float64(x.heap) / float64(len(s))
	}
	var cov, vt float64
	for _, x := range s {
		dt := x.at.Seconds() - mt
		cov += dt * (float64(x.heap) - mean)
		vt += dt * dt
	}
	if vt == 0 {
		return 0, mean
	}
	return cov / vt * (s[len(s)-1].at - s[0].at).Seconds(), mean
}

func mib[T uint64 | float64](b T) float64 { return float64(b) / (1 << 20) }

func row(s sample) string {
	return fmt.Sprintf("| %s | %.2f MiB | %d | %d |", s.at.Round(time.Second), mib(s.heap), s.goroutines, s.requests)
}

// report põe no log e no GITHUB_STEP_SUMMARY (se houver) uma amostra a cada
// logEvery, a última e as linhas do veredito.
func report(t *testing.T, d time.Duration, samples []sample, logEvery int, verdict ...string) {
	lines := []string{fmt.Sprintf("### soak: %s, %d conexões", d, workers), "", "| tempo | heap vivo | goroutines | pedidos |", "|---|---|---|---|"}
	for i, s := range samples {
		if i%logEvery == 0 || i == len(samples)-1 {
			lines = append(lines, row(s))
		}
	}
	text := strings.Join(append(append(lines, ""), verdict...), "\n") + "\n"
	t.Log("\n" + text)
	if p := os.Getenv("GITHUB_STEP_SUMMARY"); p != "" {
		f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if assert.NoError(t, err) {
			_, err = f.WriteString(text)
			assert.NoError(t, errors.Join(err, f.Close()))
		}
	}
}

func TestGrowth(t *testing.T) {
	series := func(heaps ...uint64) []sample {
		s := make([]sample, len(heaps))
		for i, h := range heaps {
			s[i] = sample{at: time.Duration(i) * time.Second, heap: h}
		}
		return s
	}
	for _, c := range []struct {
		name       string
		s          []sample
		rise, mean float64
	}{
		{"vazia", nil, 0, 0},
		{"uma amostra", series(5), 0, 5},
		{"mesmo instante", []sample{{heap: 1}, {heap: 9}}, 0, 5},
		{"plana", series(7, 7, 7, 7), 0, 7},
		{"serrote sem tendência", series(10, 0, 10, 0, 10), 0, 6},
		{"rampa", series(0, 10, 20, 30), 30, 15},
		{"rampa com ruído", series(3, 7, 23, 27), 26.4, 15},
		{"descendo", series(30, 20, 10, 0), -30, 15},
	} {
		rise, mean := growth(c.s)
		assert.InDelta(t, c.rise, rise, 1e-9, c.name)
		assert.InDelta(t, c.mean, mean, 1e-9, c.name)
	}
}
