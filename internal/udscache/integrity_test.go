package udscache

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ipc"
)

func TestCorruptFrameGetsBadFrameAndDropsTheConnection(t *testing.T) {
	var stats atomic.Int32
	rejected := make(chan ipc.Frame, 1)
	path, accepts := serve(t, func(nc net.Conn, f ipc.Frame) {
		switch {
		case f.Kind == ipc.KindError:
			rejected <- f
		case stats.Add(1) == 1: // a 1ª resposta sai com 1 bit trocado no crc
			raw, _ := ipc.Append(nil, ipc.Frame{Kind: ipc.KindStatsResp, RequestID: f.RequestID, Payload: []byte(`{}`)})
			raw[len(raw)-1] ^= 1
			_, _ = nc.Write(raw)
		default:
			reply(nc, ipc.KindStatsResp, f.RequestID, `{}`)
		}
	})
	c := New(path, -1, nil)
	defer c.Close()
	_, err := c.Stats(context.Background())
	require.ErrorContains(t, err, "bad_frame: crc")
	select {
	case e := <-rejected: // o contrato: ERROR bad_frame com o request_id do cabeçalho, sem dado do payload
		assert.EqualValues(t, 1, e.RequestID)
		assert.JSONEq(t, `{"code":"bad_frame","message":"crc"}`, string(e.Payload))
	case <-time.After(10 * time.Second):
		t.Fatal("o servidor não recebeu o ERROR bad_frame")
	}
	assert.EqualValues(t, 1, c.DiscardedFrames())
	_, err = c.Stats(context.Background()) // a conexão caiu: a seguinte é outra
	require.NoError(t, err)
	assert.EqualValues(t, 2, accepts.Load())
	assert.EqualValues(t, 1, c.DiscardedFrames())
}

// noPeer simula plataforma sem SO_PEERCRED, para só o Lstat poder recusar.
func noPeer(t *testing.T) {
	orig := peerCred
	peerCred = func(net.Conn) (int, bool, error) { return 0, false, nil }
	t.Cleanup(func() { peerCred = orig })
}

func TestUntrustedSocketIsMissWithLog(t *testing.T) {
	noPeer(t)
	path, accepts := serve(t, func(nc net.Conn, f ipc.Frame) { reply(nc, ipc.KindStatsResp, f.RequestID, `{}`) })
	plain, link := filepath.Join(filepath.Dir(path), "comum"), filepath.Join(filepath.Dir(path), "link")
	require.NoError(t, os.WriteFile(plain, nil, 0o600))
	type target struct {
		path string
		uid  int
	}
	cases := map[string]target{"arquivo comum": {plain, -1}}
	if os.Symlink(path, link) == nil { // o Windows sem privilégio não cria symlink
		cases["symlink para o socket certo"] = target{link, -1}
	}
	if runtime.GOOS != "windows" { // sem uid no Windows
		cases["dono de outro uid"] = target{path, os.Getuid() + 1}
	}
	for name, tc := range cases {
		var logs bytes.Buffer
		c := New(tc.path, tc.uid, slog.New(slog.NewTextHandler(&logs, nil)))
		_, err := c.Stats(context.Background())
		assert.ErrorIs(t, err, ErrUnavailable, name)
		assert.ErrorIs(t, err, ErrUntrustedSocket, name)
		assert.Contains(t, logs.String(), "socket não confiável", name)
		require.NoError(t, c.Close())
	}
	assert.Zero(t, accepts.Load(), "nenhum caso chegou a conectar")
	c := New(path, -1, nil) // o socket certo, do próprio uid
	defer c.Close()
	_, err := c.Stats(context.Background())
	require.NoError(t, err)
}

func TestPeerCredentialsOnLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SO_PEERCRED só no Linux")
	}
	path, _ := serve(t, func(net.Conn, ipc.Frame) {})
	nc, err := net.Dial("unix", path)
	require.NoError(t, err)
	defer nc.Close()
	require.NoError(t, checkPeer(nc, os.Getuid()))
	require.ErrorIs(t, checkPeer(nc, os.Getuid()+1), ErrUntrustedSocket)
}

func TestDialRefusesAndClosesPeerOfAnotherUID(t *testing.T) {
	dir, err := os.MkdirTemp("", "sc")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	defer ln.Close()
	closed := make(chan error, 1)
	go func() {
		nc, err := ln.Accept()
		if err == nil {
			_ = nc.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, err = nc.Read(make([]byte, 1))
			_ = nc.Close()
		}
		closed <- err
	}()
	defer func(orig func(net.Conn) (int, bool, error)) { peerCred = orig }(peerCred)
	var asked atomic.Bool
	peerCred = func(net.Conn) (int, bool, error) { // o caminho foi trocado depois do Lstat
		asked.Store(true)
		return os.Getuid() + 1, true, nil
	}
	c := New(path, -1, nil)
	defer c.Close()
	_, err = c.Stats(context.Background())
	require.ErrorIs(t, err, ErrUntrustedSocket)
	assert.True(t, asked.Load(), "conectou e conferiu o par")
	err = <-closed // EOF, ou reset no Windows; só o prazo do teste indicaria conexão esquecida
	assert.Error(t, err)
	assert.NotErrorIs(t, err, os.ErrDeadlineExceeded, "a conexão recusada é fechada, não esquecida")
}
