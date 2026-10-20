package ipc_test

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ipc"
)

// dialEcho sobe no transporte dado um servidor que responde cada frame com o
// kind seguinte (LOOKUP_REQ → LOOKUP_RESP, PING → PONG), o mesmo request_id e o
// payload resp, como o semantic-cache, e devolve a conexão do cliente. O framing
// e o código são os mesmos nos dois transportes: "unix" num caminho curto (o
// sun_path do macOS tem 104 bytes) ou "tcp" no 127.0.0.1.
func dialEcho(tb testing.TB, network string, resp []byte) net.Conn {
	addr := "127.0.0.1:0"
	if network == "unix" {
		dir, err := os.MkdirTemp("", "sc")
		require.NoError(tb, err)
		tb.Cleanup(func() { _ = os.RemoveAll(dir) })
		addr = filepath.Join(dir, "b.sock")
	}
	ln, err := net.Listen(network, addr)
	if err != nil && network == "unix" {
		tb.Skipf("AF_UNIX indisponível neste sistema (Windows anterior ao 10): %v", err)
	}
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = ln.Close() })
	go func() {
		sc, err := ln.Accept()
		if err != nil {
			return
		}
		defer sc.Close()
		r := bufio.NewReader(sc)
		var out []byte
		for f, err := ipc.ReadFrame(r); err == nil; f, err = ipc.ReadFrame(r) {
			out, _ = ipc.Append(out[:0], ipc.Frame{Kind: f.Kind + 1, RequestID: f.RequestID, Payload: resp})
			if _, err := sc.Write(out); err != nil { // um frame por escrita, como o cliente
				return
			}
		}
	}()
	nc, err := net.Dial(network, ln.Addr().String())
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = nc.Close() })
	return nc
}

// exchange manda n frames kind com payload req, com até depth em voo (1 = ida e
// volta; mais = o pipeline do udscache), e lê as n respostas, conferindo kind,
// request_id (o servidor responde em ordem) e tamanho do payload.
func exchange(nc net.Conn, r *bufio.Reader, kind ipc.Kind, req []byte, respLen, n, depth int) error {
	var out []byte
	for sent, got := 0, 0; got < n; {
		if sent < n && sent-got < depth {
			sent++
			out, _ = ipc.Append(out[:0], ipc.Frame{Kind: kind, RequestID: uint32(sent), Payload: req})
			if _, err := nc.Write(out); err != nil {
				return err
			}
			continue
		}
		f, err := ipc.ReadFrame(r)
		if err != nil {
			return err
		}
		if got++; f.Kind != kind+1 || f.RequestID != uint32(got) || len(f.Payload) != respLen {
			return fmt.Errorf("resposta %d: kind %#02x, request_id %d, %d bytes", got, f.Kind, f.RequestID, len(f.Payload))
		}
	}
	return nil
}

// O harness do benchmark nos dois transportes, no CI (que não roda benchmark):
// 200 lookups com 32 em voo voltam todos, em ordem e íntegros (o crc confere
// cada um), e uma resposta diferente da esperada é acusada.
func TestTransportHarness(t *testing.T) {
	resp := []byte(`{"hit":false,"score":0.1,"latency_micros":3}`)
	for _, network := range []string{"unix", "tcp"} {
		t.Run(network, func(t *testing.T) {
			nc := dialEcho(t, network, resp)
			r := bufio.NewReader(nc)
			require.NoError(t, exchange(nc, r, ipc.KindLookupReq, []byte(`{"prompt":"p"}`), len(resp), 200, 32))
			require.ErrorContains(t, exchange(nc, r, ipc.KindLookupReq, nil, len(resp)+1, 1, 1), fmt.Sprintf("%d bytes", len(resp)))
		})
	}
}

// BenchmarkTransport compara o IPC v1 em Unix domain socket e em TCP de
// loopback com o mesmo framing, o mesmo servidor e o mesmo cliente: só o
// transporte muda. Casos (pedido → resposta):
//   - ping: 0 B → 0 B, só cabeçalho e crc (o custo fixo de uma ida e volta);
//   - lookup: LOOKUP_REQ com embedding de 1536 dimensões em JSON (18 KB) → hit de 83 B;
//   - lookup-pipeline-32: o mesmo, com 32 em voo na mesma conexão, como o udscache;
//   - hit-1MiB: o mesmo pedido → resposta guardada de 1 MiB.
//
// MB/s conta os bytes dos dois sentidos. Para a tabela lado a lado:
//
//	go test -run '^$' -bench Transport -count 10 ./internal/ipc/ > uds-tcp.txt
//	benchstat -col /net uds-tcp.txt
func BenchmarkTransport(b *testing.B) {
	emb := bytes.Repeat([]byte("0.123456789,"), 1536)
	hit := []byte(`{"hit":true,"score":0.97,"cached_response":"resposta guardada","latency_micros":12}`)
	for _, c := range []struct {
		name      string
		kind      ipc.Kind
		req, resp []byte
		depth     int
	}{
		{"ping", ipc.KindPing, nil, nil, 1},
		{"lookup", ipc.KindLookupReq, emb, hit, 1},
		{"lookup-pipeline-32", ipc.KindLookupReq, emb, hit, 32},
		{"hit-1MiB", ipc.KindLookupReq, emb, bytes.Repeat([]byte("x"), 1<<20), 1},
	} {
		for _, network := range []string{"unix", "tcp"} {
			b.Run(c.name+"/net="+network, func(b *testing.B) {
				nc := dialEcho(b, network, c.resp)
				r := bufio.NewReader(nc)
				b.SetBytes(int64(2*(ipc.HeaderSize+ipc.TrailerSize) + len(c.req) + len(c.resp)))
				b.ReportAllocs()
				b.ResetTimer()
				if err := exchange(nc, r, c.kind, c.req, len(c.resp), b.N, c.depth); err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}
