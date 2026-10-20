// Package udscache implementa o cache.Client (pkg/client/cache) sobre o IPC v1
// (docs/ipc-v1.md), falando com o semantic-cache-rs por Unix domain socket.
// Uma conexão serve todas as goroutines: os pedidos vão em pipeline e cada
// resposta volta a quem a pediu pelo request_id, em qualquer ordem. Conexão
// que cai é refeita na chamada seguinte; discagem que falha faz as chamadas
// falharem na hora (ErrUnavailable) até passar o backoff, e o gateway trata o
// cache como miss em vez de esperar.
package udscache

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ipc"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
)

var (
	// ErrUnavailable: a discagem falhou agora, ou há pouco e o backoff não passou.
	ErrUnavailable = errors.New("udscache: semantic-cache indisponível")
	ErrClosed      = errors.New("udscache: cliente fechado")
)

// ServerError é um frame ERROR; pelo contrato, Message não carrega dado do usuário.
type ServerError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *ServerError) Error() string { return "udscache: " + e.Code + ": " + e.Message }

// Client implementa cache.Client, seguro para uso concorrente. O ctx vale para
// a discagem e para a espera pela resposta; a escrita não para no meio do frame
// (o stream perderia a fronteira): tem prazo próprio, que só estoura com o
// servidor parado, e aí a conexão é descartada.
type Client struct {
	path                      string
	dialTimeout, writeTimeout time.Duration
	minBackoff, maxBackoff    time.Duration // a espera dobra a cada discagem que falha
	readers                   sync.WaitGroup

	lock    chan struct{} // guarda os campos abaixo; canal para a espera respeitar o ctx
	cur     *conn
	retryAt time.Time
	backoff time.Duration
	lastErr error
	closed  bool
}

var _ cache.Client = (*Client)(nil)

// New cria o cliente do socket em path (config cache.socket_path). A conexão
// só é aberta na primeira chamada.
func New(path string) *Client {
	return &Client{path: path, dialTimeout: time.Second, writeTimeout: time.Second,
		minBackoff: 100 * time.Millisecond, maxBackoff: 5 * time.Second, lock: make(chan struct{}, 1)}
}

type lookupReq struct {
	Prompt    string    `json:"prompt"`
	Embedding []float32 `json:"embedding"`
	Threshold float32   `json:"threshold"`
}

type storeReq struct {
	Prompt     string    `json:"prompt"`
	Response   string    `json:"response"`
	Embedding  []float32 `json:"embedding"`
	TTLSeconds int       `json:"ttl_seconds"`
}

// Lookup implementa cache.Client (LOOKUP_REQ → LOOKUP_RESP).
func (c *Client) Lookup(ctx context.Context, prompt string, embedding []float32, threshold float32) (*cache.LookupResult, error) {
	return call[cache.LookupResult](ctx, c, ipc.KindLookupReq, lookupReq{prompt, embedding, threshold}, ipc.KindLookupResp)
}

// Store implementa cache.Client (STORE_REQ → STORE_RESP); ttlSeconds 0 = sem TTL.
func (c *Client) Store(ctx context.Context, prompt string, response string, embedding []float32, ttlSeconds int) error {
	_, err := call[struct{}](ctx, c, ipc.KindStoreReq, storeReq{prompt, response, embedding, ttlSeconds}, ipc.KindStoreResp)
	return err
}

// Stats implementa cache.Client (STATS_REQ → STATS_RESP).
func (c *Client) Stats(ctx context.Context) (*cache.StatsResult, error) {
	return call[cache.StatsResult](ctx, c, ipc.KindStatsReq, struct{}{}, ipc.KindStatsResp)
}

// Close falha os pendentes com ErrClosed, espera o leitor sair e vale para sempre.
func (c *Client) Close() error {
	c.lock <- struct{}{}
	if c.closed = true; c.cur != nil {
		c.cur.close(ErrClosed)
	}
	<-c.lock
	c.readers.Wait()
	return nil
}

// call manda o pedido e decodifica a resposta do kind want (corpo vazio = valor zero).
func call[T any](ctx context.Context, c *Client, kind ipc.Kind, req any, want ipc.Kind) (*T, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("udscache: codificando o pedido: %w", err) // ex.: NaN no embedding
	}
	cn, err := c.conn(ctx)
	if err != nil {
		return nil, err
	}
	f, err := cn.roundTrip(ctx, kind, payload, c.writeTimeout)
	var out T
	var se ServerError
	switch {
	case err != nil:
		return nil, err
	case f.Kind == ipc.KindError && json.Unmarshal(f.Payload, &se) == nil:
		return nil, &se
	case f.Kind != want:
		return nil, fmt.Errorf("udscache: resposta de kind %#02x a um pedido %#02x", f.Kind, kind)
	case len(f.Payload) > 0 && json.Unmarshal(f.Payload, &out) != nil:
		return nil, errors.New("udscache: resposta ilegível")
	}
	return &out, nil
}

// conn devolve a conexão aberta ou disca outra, uma discagem por vez.
func (c *Client) conn(ctx context.Context) (*conn, error) {
	select {
	case c.lock <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-c.lock }()
	switch {
	case c.closed:
		return nil, ErrClosed
	case c.cur != nil && !c.cur.dead():
		return c.cur, nil
	case time.Now().Before(c.retryAt):
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, c.lastErr)
	}
	dctx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()
	var d net.Dialer
	nc, err := d.DialContext(dctx, "unix", c.path)
	if err != nil {
		if ctx.Err() == nil { // desistência de quem discava não é falha do cache
			c.backoff = min(max(2*c.backoff, c.minBackoff), c.maxBackoff)
			c.retryAt, c.lastErr = time.Now().Add(c.backoff), err
		}
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	c.backoff = 0
	c.cur = &conn{nc: nc, done: make(chan struct{}), pending: map[uint32]chan ipc.Frame{}}
	c.readers.Add(1)
	go c.readLoop(c.cur)
	return c.cur, nil
}

// readLoop entrega cada resposta a quem espera pelo request_id dela até a
// conexão acabar; frame inválido também a encerra (o contrato não ressincroniza).
func (c *Client) readLoop(cn *conn) {
	defer c.readers.Done()
	r := bufio.NewReader(cn.nc)
	for {
		f, err := ipc.ReadFrame(r)
		if err != nil {
			cn.close(fmt.Errorf("udscache: conexão com o semantic-cache caiu: %w", err))
			return
		}
		cn.mu.Lock()
		ch, ok := cn.pending[f.RequestID]
		delete(cn.pending, f.RequestID)
		cn.mu.Unlock()
		if ok {
			ch <- f // buffer 1 e entrega única: nunca bloqueia
		} // sem dono (quem já desistiu, ou o ERROR de conexão com id 0 que precede o fechamento): descartada
	}
}

// conn é uma conexão aberta e os pedidos que esperam resposta nela.
type conn struct {
	nc   net.Conn      // Write é serializado pelo próprio net.Conn: um frame por chamada
	done chan struct{} // fechado quando a conexão acaba; err diz por quê
	err  error

	mu      sync.Mutex
	pending map[uint32]chan ipc.Frame // nil depois de close
	lastID  uint32
}

func (cn *conn) roundTrip(ctx context.Context, kind ipc.Kind, payload []byte, writeTimeout time.Duration) (ipc.Frame, error) {
	ch := make(chan ipc.Frame, 1)
	id, err := cn.register(ch)
	if err != nil {
		return ipc.Frame{}, err
	}
	defer func() { cn.mu.Lock(); delete(cn.pending, id); cn.mu.Unlock() }()
	raw, err := ipc.Append(nil, ipc.Frame{Kind: kind, RequestID: id, Payload: payload})
	if err != nil { // payload acima de 16 MiB: o pedido não sai, a conexão segue
		return ipc.Frame{}, fmt.Errorf("udscache: %w", err)
	}
	_ = cn.nc.SetWriteDeadline(time.Now().Add(writeTimeout))
	if _, err = cn.nc.Write(raw); err != nil { // escrita parcial deixa o stream sem fronteira de frame
		cn.close(fmt.Errorf("udscache: escrevendo o pedido: %w", err))
		return ipc.Frame{}, cn.err
	}
	select {
	case f := <-ch:
		return f, nil
	case <-ctx.Done():
		return ipc.Frame{}, ctx.Err()
	case <-cn.done:
		select {
		case f := <-ch: // a resposta chegou antes de a conexão cair
			return f, nil
		default:
			return ipc.Frame{}, cn.err
		}
	}
}

// register reserva um request_id que não esteja pendente nem seja 0 (erro de conexão).
func (cn *conn) register(ch chan ipc.Frame) (uint32, error) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	for cn.pending != nil {
		cn.lastID++
		if _, busy := cn.pending[cn.lastID]; cn.lastID != 0 && !busy {
			cn.pending[cn.lastID] = ch
			return cn.lastID, nil
		}
	}
	return 0, cn.err
}

func (cn *conn) dead() bool {
	select {
	case <-cn.done:
		return true
	default:
		return false
	}
}

// close encerra a conexão uma vez só: guarda a causa, fecha o socket e acorda quem espera.
func (cn *conn) close(err error) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	if cn.pending != nil {
		cn.err, cn.pending = err, nil
		_ = cn.nc.Close()
		close(cn.done)
	}
}
