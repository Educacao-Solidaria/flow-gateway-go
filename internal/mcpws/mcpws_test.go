package mcpws

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/middleware"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/jsonrpc"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// disp é o Dispatcher de teste: devolve os params como resultado; "lento"
// espera release, "avisa" notifica antes de responder, "trava" espera o ctx e
// "pane" entra em panic.
type disp struct {
	release chan struct{}
	ended   chan error
}

func (d *disp) Dispatch(ctx context.Context, p Peer, req *jsonrpc.Request) *jsonrpc.Response {
	switch req.Method {
	case "lento":
		<-d.release
	case "avisa":
		for _, params := range []any{map[string]int{"progress": 1}, nil} {
			if err := p.Notify(ctx, "notifications/progress", params); err != nil {
				return jsonrpc.NewErrorResponse(req.ID, jsonrpc.CodeInternalError, err.Error(), nil)
			}
		}
	case "trava":
		<-ctx.Done()
		d.ended <- ctx.Err()
		return nil
	case "pane":
		panic("tool quebrou")
	}
	return jsonrpc.NewSuccessResponse(req.ID, req.Params)
}

// start sobe o Server atrás do middleware de compressão do gateway (o
// upgrade tem de atravessar o wrapper dele) e devolve a URL ws://.
func start(t *testing.T, o Options) (*Server, *disp, *httptest.Server, string) {
	t.Helper()
	d := &disp{release: make(chan struct{}), ended: make(chan error, 1)}
	s := New(d, o, quiet)
	srv := httptest.NewServer(middleware.Compress()(s))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { _ = s.Shutdown(context.Background()) })
	return s, d, srv, "ws" + strings.TrimPrefix(srv.URL, "http")
}

func ctx10s(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

var mcp = &websocket.DialOptions{Subprotocols: []string{"mcp"}}

func dial(t *testing.T, url string, o *websocket.DialOptions) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.Dial(ctx10s(t), url, o) //nolint:bodyclose // no sucesso, o corpo é a conexão
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

func send(t *testing.T, c *websocket.Conn, msg string) {
	t.Helper()
	require.NoError(t, c.Write(ctx10s(t), websocket.MessageText, []byte(msg)))
}

func recv(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	typ, b, err := c.Read(ctx10s(t))
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, typ)
	return string(b)
}

func refused(t *testing.T, url string, o *websocket.DialOptions) int {
	t.Helper()
	_, resp, err := websocket.Dial(ctx10s(t), url, o)
	require.Error(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestHandshakeNeedsSubprotocolAndAllowedOrigin(t *testing.T) {
	_, _, srv, url := start(t, Options{Origins: []string{"studio.example"}})
	assert.Equal(t, http.StatusBadRequest, refused(t, url, nil), "sem o subprotocolo mcp")
	evil := &websocket.DialOptions{Subprotocols: []string{"mcp"}, HTTPHeader: http.Header{"Origin": {"https://evil.example"}}}
	assert.Equal(t, http.StatusForbidden, refused(t, url, evil), "Origin fora da lista")
	for _, origin := range []string{"https://studio.example", srv.URL, ""} { // da lista, a mesma origem, cliente sem navegador
		c := dial(t, url, &websocket.DialOptions{Subprotocols: []string{"chat", "mcp"}, HTTPHeader: http.Header{"Origin": {origin}}})
		assert.Equal(t, "mcp", c.Subprotocol(), origin)
	}
}

func TestRequestsNotificationsAndInvalidMessages(t *testing.T) {
	_, _, _, url := start(t, Options{})
	c := dial(t, url, mcp)
	send(t, c, `{"jsonrpc":"2.0","id":1,"method":"eco","params":{"a":1}}`)
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":{"a":1}}`, recv(t, c))
	send(t, c, `{"jsonrpc":"2.0","method":"eco","params":{"a":2}}`) // notificação: nada volta
	send(t, c, `{"jsonrpc":"2.0","id":"x","method":"avisa","params":{}}`)
	assert.JSONEq(t, `{"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":1}}`, recv(t, c))
	assert.JSONEq(t, `{"jsonrpc":"2.0","method":"notifications/progress"}`, recv(t, c), "sem params, sem o campo (null é inválido)")
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":"x","result":{}}`, recv(t, c))

	send(t, c, `{"jsonrpc":"2.0","id":2,`)
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"JSON inválido"}}`, recv(t, c))
	for _, msg := range []string{`[{"jsonrpc":"2.0","id":3,"method":"eco"}]`, `{"jsonrpc":"1.0","id":4,"method":"eco"}`, `{"jsonrpc":"2.0","id":5}`} {
		send(t, c, msg) // a ordem dos campos é a do jsonrpc.Response; a mensagem é do encoding/json
		assert.Contains(t, recv(t, c), `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,`, msg)
	}
}

func TestNotificationsInOrderPanicsAndBusyBecomeErrors(t *testing.T) {
	_, d, _, url := start(t, Options{MaxInFlight: 1})
	c := dial(t, url, mcp)
	var released atomic.Bool
	send(t, c, `{"jsonrpc":"2.0","method":"lento"}`) // notificação: segura a leitura até o release
	send(t, c, `{"jsonrpc":"2.0","id":1,"method":"eco","params":1}`)
	time.AfterFunc(20*time.Millisecond, func() { released.Store(true); close(d.release) })
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":1}`, recv(t, c))
	assert.True(t, released.Load(), "o pedido depois da notificação esperou ela acabar")

	send(t, c, `{"jsonrpc":"2.0","id":2,"method":"pane"}`)
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":2,"error":{"code":-32603,"message":"erro interno"}}`, recv(t, c))
	send(t, c, `{"jsonrpc":"2.0","method":"pane"}`)         // em notificação: nada volta, e a conexão segue
	send(t, c, `{"jsonrpc":"2.0","id":3,"method":"trava"}`) // ocupa a única vaga
	send(t, c, `{"jsonrpc":"2.0","id":4,"method":"eco","params":4}`)
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":4,"error":{"code":-32603,"message":"mcpws: requisições demais em curso nesta conexão"}}`, recv(t, c))
}

func TestDispatchRunsConcurrentlyAndStopsWhenTheClientLeaves(t *testing.T) {
	_, d, _, url := start(t, Options{})
	c := dial(t, url, mcp)
	send(t, c, `{"jsonrpc":"2.0","id":1,"method":"lento","params":1}`)
	send(t, c, `{"jsonrpc":"2.0","id":2,"method":"eco","params":2}`)
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":2,"result":2}`, recv(t, c), "o lento não segura o outro")
	close(d.release)
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":1}`, recv(t, c))

	send(t, c, `{"jsonrpc":"2.0","id":3,"method":"trava"}`)
	require.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
	assert.ErrorIs(t, ended(t, d), context.Canceled, "o Dispatch em curso é cancelado quando a conexão cai")
}

// ended espera o "trava" sair pelo ctx.
func ended(t *testing.T, d *disp) error {
	t.Helper()
	select {
	case err := <-d.ended:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("o Dispatch não foi cancelado")
		return nil
	}
}

func TestLimitsCloseWith1009And1003(t *testing.T) {
	_, _, _, url := start(t, Options{MaxMessage: 64})
	c := dial(t, url, mcp)
	head := `{"jsonrpc":"2.0","id":1,"method":"eco","params":"`
	exact := head + strings.Repeat("x", 64-len(head)-2) + `"}`
	send(t, c, exact) // no teto: passa
	assert.Contains(t, recv(t, c), `"result":"xx`)
	send(t, c, exact+" ")
	_, _, err := c.Read(ctx10s(t))
	assert.Equal(t, websocket.StatusMessageTooBig, websocket.CloseStatus(err))

	c = dial(t, url, mcp)
	require.NoError(t, c.Write(ctx10s(t), websocket.MessageBinary, []byte(`{}`)))
	_, _, err = c.Read(ctx10s(t))
	assert.Equal(t, websocket.StatusUnsupportedData, websocket.CloseStatus(err))
}

func TestPingKeepsReaderAliveAndDropsSilentPeer(t *testing.T) {
	_, d, _, url := start(t, Options{PingInterval: 5 * time.Millisecond, PongTimeout: 2 * time.Second})
	c := dial(t, url, mcp)
	send(t, c, `{"jsonrpc":"2.0","id":1,"method":"lento","params":1}`)
	time.AfterFunc(60*time.Millisecond, func() { close(d.release) }) // ~10 pings respondidos pelo Read
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":1}`, recv(t, c))

	_, d, srv, _ := start(t, Options{PingInterval: 5 * time.Millisecond, PongTimeout: 50 * time.Millisecond})
	nc, err := net.Dial("tcp", srv.Listener.Addr().String()) // cliente cru: lê, mas nunca responde ao ping
	require.NoError(t, err)
	defer nc.Close()
	_, err = fmt.Fprint(nc, "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: chat, mcp\r\n\r\n") // como o navegador: com espaço
	require.NoError(t, err)
	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	note := `{"jsonrpc":"2.0","method":"trava"}`                                         // notificação que só sai pelo ctx, segurando a leitura
	_, err = nc.Write(append([]byte{0x81, 0x80 | byte(len(note)), 0, 0, 0, 0}, note...)) // máscara zero
	require.NoError(t, err)
	require.NoError(t, nc.SetDeadline(time.Now().Add(10*time.Second)))
	_, err = io.Copy(io.Discard, br)
	assert.NoError(t, err, "o servidor derrubou a conexão (EOF) antes do prazo do teste")
	assert.ErrorIs(t, ended(t, d), context.Canceled, "a queda pelo pong cancela o Dispatch em curso")
}

func TestShutdownClosesWith1001AndRefusesNewConnections(t *testing.T) {
	s, _, _, url := start(t, Options{})
	c := dial(t, url, mcp)
	readErr := make(chan error, 1)
	go func() { _, _, err := c.Read(context.Background()); readErr <- err }()
	require.NoError(t, s.Shutdown(ctx10s(t)))
	assert.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(<-readErr))
	assert.Equal(t, http.StatusServiceUnavailable, refused(t, url, mcp))
}
