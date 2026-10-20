// Package mcpws é o transporte WebSocket do servidor MCP para o studio
// (contrato §3 da Fase 3): monte o Server em /mcp/ws. Só aceita o
// subprotocolo "mcp"; um frame de texto é uma mensagem JSON-RPC 2.0 (o
// envelope do pkg/jsonrpc); frame binário fecha com 1003 e mensagem acima de
// MaxMessage (1 MiB) com 1009; o servidor pinga a cada 30 s e derruba a
// conexão sem pong em 60 s; e o Origin é conferido contra uma lista (sem
// lista, só a mesma origem), contra cross-site WebSocket hijacking. O
// protocolo é o da github.com/coder/websocket (sem dependências, go 1.23).
// O Server não autentica: a rota o monta atrás do auth e do rate limit.
package mcpws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/jsonrpc"
)

// Dispatcher é o dispatcher JSON-RPC do servidor MCP (do Luan): recebe a
// requisição validada e devolve a resposta (a de notificação é descartada).
// Requisição roda em goroutine própria, até MaxInFlight por conexão; além
// disso, é recusada na hora com erro interno (parar de ler deixaria o pong sem
// leitura). Notificação roda em ordem, antes da próxima leitura, e tem de ser
// rápida. O ctx cai com a conexão; panic vira erro interno; peer notifica o studio.
type Dispatcher interface {
	Dispatch(ctx context.Context, peer Peer, req *jsonrpc.Request) *jsonrpc.Response
}

// Peer é o studio do outro lado de uma conexão.
type Peer interface {
	Notify(ctx context.Context, method string, params any) error
}

// Options configura o Server; zero vale o padrão do contrato.
type Options struct {
	Origins      []string      // padrões de host (path.Match) aceitos além da mesma origem
	MaxMessage   int64         // padrão 1 MiB
	PingInterval time.Duration // padrão 30 s
	PongTimeout  time.Duration // padrão 60 s
	MaxInFlight  int           // requisições em curso por conexão; padrão 16
}

// writeTimeout é o prazo de cada escrita; estourado, a conexão cai.
const writeTimeout = 10 * time.Second

// Server atende as conexões WebSocket do MCP.
type Server struct {
	d      Dispatcher
	opts   Options
	log    *slog.Logger
	ctx    context.Context // cancelado no Shutdown
	cancel context.CancelFunc

	mu     sync.Mutex // closed e o Add de conns, para nada entrar depois do Shutdown
	closed bool
	conns  sync.WaitGroup
}

// New cria o Server; log nil usa slog.Default().
func New(d Dispatcher, o Options, log *slog.Logger) *Server {
	o.MaxMessage, o.MaxInFlight = orDefault(o.MaxMessage, 1<<20), orDefault(o.MaxInFlight, 16)
	o.PingInterval, o.PongTimeout = orDefault(o.PingInterval, 30*time.Second), orDefault(o.PongTimeout, time.Minute)
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{d: d, opts: o, log: log, ctx: ctx, cancel: cancel}
}

func orDefault[T int | int64 | time.Duration](v, padrao T) T {
	if v <= 0 {
		return padrao
	}
	return v
}

// ServeHTTP faz o upgrade e atende a conexão até ela acabar.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !offers(r.Header, "mcp") {
		http.Error(w, `mcpws: o subprotocolo "mcp" é obrigatório`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		http.Error(w, "mcpws: servidor encerrando", http.StatusServiceUnavailable)
		return
	}
	s.conns.Add(1)
	s.mu.Unlock()
	defer s.conns.Done()
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"mcp"}, OriginPatterns: s.opts.Origins})
	if err != nil { // o Accept já respondeu (403 para Origin de fora)
		s.log.Info("mcpws: handshake recusado", "err", err)
		return
	}
	s.serve(c)
}

// offers diz se o cliente pediu o subprotocolo proto (lista separada por vírgula).
func offers(h http.Header, proto string) bool {
	return slices.ContainsFunc(strings.Split(strings.Join(h.Values("Sec-WebSocket-Protocol"), ","), ","),
		func(p string) bool { return strings.EqualFold(strings.TrimSpace(p), proto) })
}

// serve lê as mensagens até a conexão acabar e só volta depois que o ping e
// todo Dispatch dela voltaram.
func (s *Server) serve(c *websocket.Conn) {
	defer func() { _ = c.CloseNow() }() // já fechada pelo caminho normal: no-op
	c.SetReadLimit(s.opts.MaxMessage)
	ctx, cancel := context.WithCancel(s.ctx)
	var work sync.WaitGroup
	defer work.Wait()
	defer cancel()
	work.Add(1)
	go func() { defer work.Done(); s.keepalive(ctx, c); cancel() }() // conexão morta cancela os Dispatch
	p := &peer{c: c}
	slots := make(chan struct{}, s.opts.MaxInFlight)
	for {
		// Sem ctx: cancelar o Read derruba a conexão sem o handshake de
		// fechamento. Ela acaba pelo Close do keepalive ou pelo cliente.
		typ, msg, err := c.Read(context.Background())
		if err != nil {
			return // fechou (1000/1001 do cliente, 1009 do teto) ou caiu
		}
		if typ != websocket.MessageText {
			_ = c.Close(websocket.StatusUnsupportedData, "mcpws: só frame de texto")
			return
		}
		req, err := jsonrpc.ParseRequest(msg)
		switch {
		case err != nil:
			s.reply(ctx, p, invalid(msg, err))
		case req.IsNotification():
			s.dispatch(ctx, p, req) // em ordem: o initialized chega antes do que vem depois dele
		default:
			select {
			case slots <- struct{}{}:
				work.Add(1)
				go func() {
					defer work.Done()
					resp := s.dispatch(ctx, p, req)
					<-slots // antes de responder: quem recebe a resposta já acha a vaga livre
					s.reply(ctx, p, resp)
				}()
			default:
				s.reply(ctx, p, jsonrpc.NewErrorResponse(req.ID, jsonrpc.CodeInternalError, "mcpws: requisições demais em curso nesta conexão", nil))
			}
		}
	}
}

// keepalive pinga a cada PingInterval e derruba a conexão sem pong em
// PongTimeout; no Shutdown, fecha com 1001 (Going Away).
func (s *Server) keepalive(ctx context.Context, c *websocket.Conn) {
	t := time.NewTicker(s.opts.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			if s.ctx.Err() != nil {
				_ = c.Close(websocket.StatusGoingAway, "mcpws: servidor encerrando")
			}
			return
		case <-t.C:
		}
		pctx, cancel := context.WithTimeout(ctx, s.opts.PongTimeout)
		err := c.Ping(pctx)
		cancel()
		if err != nil && ctx.Err() == nil { // sem pong no prazo, ou a conexão caiu agora
			s.log.Info("mcpws: ping sem resposta; conexão derrubada", "err", err)
			_ = c.CloseNow()
			return
		}
	}
}

// invalid é a resposta a msg que não é requisição JSON-RPC (err do ParseRequest).
func invalid(msg []byte, err error) *jsonrpc.Response {
	if !json.Valid(msg) {
		return jsonrpc.NewErrorResponse(nil, jsonrpc.CodeParseError, "JSON inválido", nil)
	}
	// array (lote, fora do MCP atual), sem method, versão errada
	return jsonrpc.NewErrorResponse(nil, jsonrpc.CodeInvalidRequest, err.Error(), nil)
}

// reply manda resp ao studio; nil não manda nada.
func (s *Server) reply(ctx context.Context, p *peer, resp *jsonrpc.Response) {
	if resp != nil {
		if err := p.send(ctx, resp); err != nil {
			s.log.Debug("mcpws: resposta não entregue", "err", err)
		}
	}
}

// dispatch chama o Dispatcher; panic (de uma tool, por exemplo) vira erro
// interno logado com a stack, em vez de derrubar o processo.
func (s *Server) dispatch(ctx context.Context, p Peer, req *jsonrpc.Request) (resp *jsonrpc.Response) {
	defer func() {
		if rec := recover(); rec != nil {
			s.log.Error("mcpws: panic no Dispatch", "method", req.Method, "panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
			resp = jsonrpc.NewErrorResponse(req.ID, jsonrpc.CodeInternalError, "erro interno", nil)
		}
	}()
	return s.d.Dispatch(ctx, p, req)
}

// peer escreve na conexão; a coder/websocket serializa as escritas.
type peer struct{ c *websocket.Conn }

func (p *peer) Notify(ctx context.Context, method string, params any) error {
	n := jsonrpc.Request{JSONRPC: jsonrpc.Version, Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return err
		}
		n.Params = raw
	}
	return p.send(ctx, n)
}

func (p *peer) send(ctx context.Context, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return p.c.Write(ctx, websocket.MessageText, b)
}

// Shutdown recusa conexões novas, fecha as abertas com 1001 e espera elas
// acabarem até ctx. O http.Server.Shutdown não cobre isto: a conexão
// WebSocket sai dele no Hijack.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	done := make(chan struct{})
	go func() { s.conns.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
