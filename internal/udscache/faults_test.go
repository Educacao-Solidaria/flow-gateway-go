package udscache

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ipc"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
)

const hitPayload = `{"hit":true,"score":0.97,"cached_response":"resposta guardada","latency_micros":12}`

// O semantic-cache que morre no meio da resposta, em qualquer byte do frame
// (cabeçalho, payload ou crc): só aquele pedido falha (a rota segue como miss,
// nunca com um hit pela metade), não há a quem mandar ERROR, e a chamada
// seguinte disca de novo e recebe a resposta inteira.
func TestResponseCutAtAnyByteFailsOnlyThatRequest(t *testing.T) {
	var n atomic.Int32
	path, accepts := serve(t, func(nc net.Conn, f ipc.Frame) {
		raw, _ := ipc.Append(nil, ipc.Frame{Kind: ipc.KindLookupResp, RequestID: f.RequestID, Payload: []byte(hitPayload)})
		if k := n.Add(1); k%2 == 1 { // pedidos 1, 3, 5...: só os bytes 0, 1, 2... do frame, e a conexão cai
			_, _ = nc.Write(raw[:k/2])
			_ = nc.Close()
			return
		}
		_, _ = nc.Write(raw)
	})
	before := runtime.NumGoroutine()
	c := New(path, -1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // pedido pendurado falha aqui, com mensagem
	defer cancel()
	size := ipc.HeaderSize + len(hitPayload) + ipc.TrailerSize
	for k := range size {
		res, err := c.Lookup(ctx, "p", []float32{0.5}, 0.9)
		require.ErrorContains(t, err, "caiu", "corte no byte %d", k)
		require.Nil(t, res)
		res, err = c.Lookup(ctx, "p", []float32{0.5}, 0.9)
		require.NoError(t, err, "depois do corte no byte %d", k)
		require.Equal(t, "resposta guardada", res.CachedResponse)
	}
	assert.Zero(t, c.DiscardedFrames(), "corte não é frame corrompido")
	assert.EqualValues(t, size+1, accepts.Load(), "uma conexão nova por corte, nenhuma a mais")
	require.NoError(t, c.Close())
	testutil.NoGoroutineLeak(t, before)
}

// O corte com pedidos em pipeline na mesma conexão: o que já tinha resposta
// inteira volta, e os outros falham juntos, sem nenhum ficar esperando.
func TestCutWithPipelinedRequests(t *testing.T) {
	cut := make(chan struct{})
	var held []ipc.Frame // uma conexão só: o handler roda na goroutine dela
	path, _ := serve(t, func(nc net.Conn, f ipc.Frame) {
		if held = append(held, f); len(held) < 3 {
			return
		}
		reply(nc, ipc.KindStatsResp, held[1].RequestID, `{"total_entries":7}`)
		<-cut // o 2º já foi entregue: agora metade do frame do 1º, e a conexão cai
		raw, _ := ipc.Append(nil, ipc.Frame{Kind: ipc.KindStatsResp, RequestID: held[0].RequestID, Payload: []byte(`{}`)})
		_, _ = nc.Write(raw[:len(raw)/2])
		_ = nc.Close()
	})
	before := runtime.NumGoroutine()
	c := New(path, -1, nil)
	type result struct {
		res *cache.StatsResult
		err error
	}
	results := make(chan result, 3)
	for range 3 {
		go func() {
			res, err := c.Stats(context.Background())
			results <- result{res, err}
		}()
	}
	next := func() result {
		select {
		case r := <-results:
			return r
		case <-time.After(10 * time.Second):
			t.Fatal("pedido pendurado depois do corte")
			return result{}
		}
	}
	r := next()
	require.NoError(t, r.err)
	assert.EqualValues(t, 7, r.res.TotalEntries)
	close(cut)
	for range 2 {
		require.ErrorContains(t, next().err, "caiu")
	}
	require.NoError(t, c.Close())
	testutil.NoGoroutineLeak(t, before)
}

// TestRealServerKilledAndRestarted roda contra o semantic-cache-server de
// verdade (semantic-cache-rs) quando SEMANTIC_CACHE_BIN aponta para o binário:
// o processo morre com SIGKILL no meio da sessão, o cliente degrada (erro na
// hora: a rota segue como miss) e volta quando ele sobe de novo. Sem a
// variável, pula: o CI do Go não depende do Rust.
func TestRealServerKilledAndRestarted(t *testing.T) {
	bin := os.Getenv("SEMANTIC_CACHE_BIN")
	if bin == "" || runtime.GOOS == "windows" {
		t.Skip("sem SEMANTIC_CACHE_BIN (o binário do semantic-cache-server); no Windows ele fala named pipe")
	}
	dir, err := os.MkdirTemp("", "sc")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "s.sock")
	start := func() *exec.Cmd {
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(), "SEMCACHE_IPC_SOCKET_PATH="+path)
		require.NoError(t, cmd.Start())
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			if nc, err := net.Dial("unix", path); err == nil {
				_ = nc.Close()
				return cmd
			}
			require.True(t, time.Now().Before(deadline), "o semantic-cache-server não subiu em %s", path)
		}
	}
	srv := start()
	c := New(path, -1, nil)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.Stats(ctx)
	require.NoError(t, err)

	require.NoError(t, srv.Process.Kill()) // SIGKILL: sem drenar e sem apagar o socket
	_ = srv.Wait()
	res, err := c.Lookup(ctx, "p", []float32{0.5}, 0.9)
	require.Error(t, err, "a conexão morreu com o processo")
	assert.Nil(t, res)
	_, err = c.Lookup(ctx, "p", []float32{0.5}, 0.9)
	require.ErrorIs(t, err, ErrUnavailable, "ninguém escuta: falha na hora")

	start()                 // na partida, o servidor remove o socket morto
	c.retryAt = time.Time{} // sem esperar o backoff
	_, err = c.Stats(ctx)
	require.NoError(t, err)
}
