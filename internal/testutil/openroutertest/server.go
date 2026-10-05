// Package openroutertest sobe um servidor HTTP local que imita a API de chat
// completions da OpenRouter: sucesso (JSON ou SSE), 429 com Retry-After e
// erros 5xx, roteirizados por requisição. Aponte openrouter.base_url para
// Server.URL.
package openroutertest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
)

// Response roteiriza uma resposta. O valor zero é um 200 com Content.
type Response struct {
	// Status HTTP; 0 ou qualquer 2xx é sucesso (sai 200). Fora de 2xx o corpo
	// é o envelope de erro da OpenRouter: {"error":{"code":Status,"message":Message}}.
	Status  int
	Message string
	// RetryAfter, se > 0, vai no cabeçalho Retry-After (segundos).
	RetryAfter int
	// Content é o texto da resposta; vazio usa DefaultContent. Em requisição
	// com "stream": true, cada item de Chunks vira um evento SSE (sem Chunks,
	// Content vai num evento só).
	Content string
	Chunks  []string
}

// DefaultContent é o texto de um sucesso sem Content.
const DefaultContent = "olá do mock da OpenRouter"

// RateLimited é um 429 com Retry-After, como a OpenRouter devolve.
func RateLimited(retryAfter int) Response {
	return Response{Status: http.StatusTooManyRequests, RetryAfter: retryAfter, Message: "Rate limit exceeded"}
}

// ServerError é um erro de upstream (500, 502, 503...).
func ServerError(status int) Response {
	return Response{Status: status, Message: http.StatusText(status)}
}

// Call é uma requisição recebida pelo mock.
type Call struct {
	Header  http.Header
	Request domain.ChatCompletionRequest
}

// Server é o mock. Respostas enfileiradas com Enqueue saem em ordem, uma por
// requisição; com a fila vazia, todo pedido recebe sucesso padrão.
type Server struct {
	*httptest.Server

	mu    sync.Mutex
	queue []Response
	calls []Call
}

// New sobe o mock e o encerra no fim do teste.
func New(t testing.TB) *Server {
	s := &Server{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /chat/completions", s.chat)
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

// Enqueue acrescenta respostas ao roteiro.
func (s *Server) Enqueue(rs ...Response) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(s.queue, rs...)
}

// Calls devolve as requisições recebidas até agora, em ordem.
func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	var req domain.ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, Response{Status: http.StatusBadRequest, Message: "invalid JSON: " + err.Error()})
		return
	}

	s.mu.Lock()
	s.calls = append(s.calls, Call{Header: r.Header.Clone(), Request: req})
	var resp Response
	if len(s.queue) > 0 {
		resp, s.queue = s.queue[0], s.queue[1:]
	}
	n := len(s.calls)
	s.mu.Unlock()

	if resp.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(resp.RetryAfter))
	}
	if resp.Status != 0 && (resp.Status < 200 || resp.Status > 299) {
		writeError(w, resp)
		return
	}
	if resp.Content == "" {
		resp.Content = DefaultContent
	}
	id := fmt.Sprintf("gen-mock-%d", n)
	if req.Stream {
		stream(w, id, req.Model, resp)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(domain.ChatCompletionResponse{
		ID: id, Object: "chat.completion", Model: req.Model,
		Choices: []domain.Choice{{Message: &domain.ChatMessage{Role: domain.RoleAssistant, Content: resp.Content}, FinishReason: "stop"}},
		Usage:   &domain.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
	})
}

// stream escreve no formato SSE da OpenRouter: um comentário de keep-alive,
// um chat.completion.chunk por pedaço, o último com finish_reason, e [DONE].
func stream(w http.ResponseWriter, id, model string, resp Response) {
	chunks := resp.Chunks
	if len(chunks) == 0 {
		chunks = []string{resp.Content}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, _ := w.(http.Flusher)
	_, _ = fmt.Fprint(w, ": OPENROUTER PROCESSING\n\n")
	for i, c := range chunks {
		choice := domain.Choice{Delta: &domain.ChatMessage{Role: domain.RoleAssistant, Content: c}}
		if i == len(chunks)-1 {
			choice.FinishReason = "stop"
		}
		data, _ := json.Marshal(domain.ChatCompletionResponse{ID: id, Object: "chat.completion.chunk", Model: model, Choices: []domain.Choice{choice}})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		if flusher != nil {
			flusher.Flush()
		}
	}
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
}

func writeError(w http.ResponseWriter, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.Status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": resp.Status, "message": resp.Message}})
}
