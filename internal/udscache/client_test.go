package udscache

import (
	"bufio"
	"context"
	"encoding/json"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ipc"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
)

// serve sobe um semantic-cache de mentira que fala IPC v1 num caminho curto (teto de 104
// bytes no macOS; o t.TempDir() leva o nome do teste). handle roda na goroutine da conexão.
func serve(t *testing.T, handle func(nc net.Conn, f ipc.Frame)) (string, *atomic.Int32) {
	dir, err := os.MkdirTemp("", "sc")
	require.NoError(t, err)
	path := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close(); _ = os.RemoveAll(dir) })
	var accepts atomic.Int32
	go func() {
		for nc, err := ln.Accept(); err == nil; nc, err = ln.Accept() {
			accepts.Add(1)
			go func() {
				defer nc.Close()
				r := bufio.NewReader(nc)
				for f, err := ipc.ReadFrame(r); err == nil; f, err = ipc.ReadFrame(r) {
					handle(nc, f)
				}
			}()
		}
	}()
	return path, &accepts
}

func reply(nc net.Conn, kind ipc.Kind, id uint32, payload string) {
	raw, _ := ipc.Append(nil, ipc.Frame{Kind: kind, RequestID: id, Payload: []byte(payload)})
	_, _ = nc.Write(raw)
}

func TestContractRoundTrip(t *testing.T) {
	next := make(chan ipc.Frame, 1) // a resposta do próximo pedido
	var reqs []string               // uma conexão só: o handler roda numa goroutine
	path, accepts := serve(t, func(nc net.Conn, f ipc.Frame) {
		reqs = append(reqs, string(f.Payload))
		r := <-next
		reply(nc, r.Kind, f.RequestID, string(r.Payload))
	})
	c := New(path, -1, nil)
	defer c.Close()
	ctx := context.Background()
	_, err := c.Lookup(ctx, "p", []float32{float32(math.NaN())}, 0.9)
	require.ErrorContains(t, err, "NaN")
	assert.Zero(t, accepts.Load(), "pedido que não codifica nem disca")

	// Chaves fora de ordem e score inteiro: o receptor não depende de nenhum dos dois.
	next <- ipc.Frame{Kind: ipc.KindLookupResp, Payload: []byte(`{"latency_micros":42,"cached_response":"oi","score":1,"hit":true}`)}
	hit, err := c.Lookup(ctx, "olá", []float32{0.5, -0.25}, 0.75)
	require.NoError(t, err)
	assert.Equal(t, cache.LookupResult{Hit: true, Score: 1, CachedResponse: "oi", LatencyMicros: 42}, *hit)
	next <- ipc.Frame{Kind: ipc.KindStoreResp, Payload: []byte(`{}`)}
	require.NoError(t, c.Store(ctx, "p", "r", []float32{1}, 60))
	next <- ipc.Frame{Kind: ipc.KindStatsResp, Payload: []byte(`{"total_entries":3,"hit_count":2,"miss_count":1,"hit_ratio":0.5}`)}
	st, err := c.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, cache.StatsResult{TotalEntries: 3, HitCount: 2, MissCount: 1, HitRatio: 0.5}, *st)

	// ERROR do servidor, kind errado e payload acima do teto falham só o pedido.
	next <- ipc.Frame{Kind: ipc.KindError, Payload: []byte(`{"code":"bad_request","message":"dimensão 2 ≠ 1536"}`)}
	var se *ServerError
	require.ErrorAs(t, c.Store(ctx, "p", "r", []float32{1, 2}, 0), &se)
	assert.Equal(t, ServerError{Code: ipc.CodeBadRequest, Message: "dimensão 2 ≠ 1536"}, *se)
	next <- ipc.Frame{Kind: ipc.KindStoreResp}
	_, err = c.Lookup(ctx, "p", nil, 0.9)
	require.ErrorContains(t, err, "kind 0x04")
	require.ErrorContains(t, c.Store(ctx, "p", strings.Repeat("x", ipc.MaxPayload), nil, 0), "acima do teto")
	next <- ipc.Frame{Kind: ipc.KindStatsResp} // corpo vazio = valor zero
	_, err = c.Stats(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, accepts.Load(), "uma conexão para tudo")

	// O 1º pedido é, byte a byte, o payload do LOOKUP_REQ de referência do contrato.
	assert.Equal(t, []string{
		`{"prompt":"olá","embedding":[0.5,-0.25],"threshold":0.75}`,
		`{"prompt":"p","response":"r","embedding":[1],"ttl_seconds":60}`,
		`{}`,
	}, reqs[:3])
}

func TestPipelineCancelAndClose(t *testing.T) {
	arrived := make(chan ipc.Frame, 1)
	var slow net.Conn
	path, accepts := serve(t, func(nc net.Conn, f ipc.Frame) {
		var req lookupReq
		_ = json.Unmarshal(f.Payload, &req)
		if req.Prompt == "lento" {
			slow = nc
			arrived <- f // segura sem responder
			return
		}
		reply(nc, ipc.KindLookupResp, f.RequestID, `{"hit":true,"cached_response":"`+req.Prompt+`"}`)
	})
	c := New(path, -1, nil)
	defer c.Close()
	lento := func(ctx context.Context) chan error {
		errc := make(chan error, 1)
		go func() { _, err := c.Lookup(ctx, "lento", nil, 0.9); errc <- err }()
		return errc
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := lento(ctx)
	f := <-arrived

	// Com o "lento" sem resposta, outro pedido vai na mesma conexão e volta
	// antes dele: pipeline fora de ordem, correlacionado pelo request_id.
	res, err := c.Lookup(context.Background(), "rápido", nil, 0.9)
	require.NoError(t, err)
	assert.Equal(t, "rápido", res.CachedResponse)
	cancel()
	require.ErrorIs(t, <-errc, context.Canceled)
	c.cur.mu.Lock()
	assert.Empty(t, c.cur.pending, "quem desistiu sai da tabela")
	c.cur.mu.Unlock()

	// A resposta atrasada não tem mais dono: é descartada, e a conexão segue.
	reply(slow, ipc.KindLookupResp, f.RequestID, `{"hit":true}`)
	res, err = c.Lookup(context.Background(), "de novo", nil, 0.9)
	require.NoError(t, err)
	assert.Equal(t, "de novo", res.CachedResponse)
	assert.EqualValues(t, 1, accepts.Load())

	// Close falha o pedido pendente, espera o leitor sair e vale para sempre.
	errc = lento(context.Background())
	<-arrived
	require.NoError(t, c.Close())
	require.ErrorIs(t, <-errc, ErrClosed)
	_, err = c.Stats(context.Background())
	require.ErrorIs(t, err, ErrClosed)
}

func TestReconnectAndBackoff(t *testing.T) {
	path, accepts := serve(t, func(nc net.Conn, f ipc.Frame) {
		switch {
		case strings.Contains(string(f.Payload), `"cai"`):
			_ = nc.Close()
		default:
			reply(nc, ipc.KindStatsResp, f.RequestID, `{}`)
		}
	})
	c := New(path+".nada", -1, nil) // ninguém escutando
	defer c.Close()
	c.minBackoff = time.Hour
	ctx := context.Background()
	_, err := c.Stats(ctx)
	require.ErrorIs(t, err, ErrUnavailable)
	c.path = path // o servidor certo existe, mas o backoff ainda vale
	_, err = c.Stats(ctx)
	require.ErrorIs(t, err, ErrUnavailable, "dentro do backoff, falha sem discar")
	assert.Zero(t, accepts.Load())
	c.retryAt = time.Time{} // o backoff passou
	_, err = c.Stats(ctx)
	require.NoError(t, err)

	// Cada queda falha quem esperava, e a chamada seguinte disca de novo, sem backoff.
	_, err = c.Lookup(ctx, "cai", nil, 0.9)
	require.ErrorContains(t, err, "caiu")
	_, err = c.Stats(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 2, accepts.Load())
}

func TestStalledServerHitsWriteTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("o AF_UNIX do Windows aceita a escrita inteira sem o par ler (visto no CI): a escrita não trava")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // nunca pendura o teste
	defer cancel()
	stall := make(chan struct{})
	defer close(stall)
	path, _ := serve(t, func(nc net.Conn, f ipc.Frame) { // responde o 1º pedido e para de ler
		reply(nc, ipc.KindStatsResp, f.RequestID, `{}`)
		<-stall
	})
	c := New(path, -1, nil)
	c.writeTimeout = 50 * time.Millisecond
	defer c.Close()
	_, err := c.Stats(ctx)
	require.NoError(t, err)
	// 15 MiB não cabem no buffer do socket: a escrita trava até o prazo.
	err = c.Store(ctx, "p", strings.Repeat("x", 15<<20), nil, 0)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	assert.True(t, c.cur.dead(), "escrita parcial descarta a conexão")
}

func TestRequestIDSkipsZeroAndPending(t *testing.T) {
	cn := &conn{pending: map[uint32]chan ipc.Frame{math.MaxUint32: nil}, lastID: math.MaxUint32 - 1}
	id, err := cn.register(make(chan ipc.Frame, 1))
	require.NoError(t, err)
	assert.EqualValues(t, 1, id, "pula o MaxUint32 ainda pendente e o 0")
}
