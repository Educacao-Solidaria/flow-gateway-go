package openroutertest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/domain"
)

// result é a resposta já lida e fechada.
type result struct {
	Status int
	Header http.Header
	Body   []byte
}

func post(t *testing.T, s *Server, req domain.ChatCompletionRequest) result {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)
	hr, err := http.NewRequest(http.MethodPost, s.URL+"/chat/completions", bytes.NewReader(body))
	require.NoError(t, err)
	hr.Header.Set("Authorization", "Bearer sk-teste")
	resp, err := s.Client().Do(hr)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return result{resp.StatusCode, resp.Header, data}
}

var chatReq = domain.ChatCompletionRequest{
	Model:    "openai/gpt-4o-mini",
	Messages: []domain.ChatMessage{{Role: domain.RoleUser, Content: "oi"}},
}

func TestSuccessByDefault(t *testing.T) {
	s := New(t)
	resp := post(t, s, chatReq)
	require.Equal(t, http.StatusOK, resp.Status)

	var out domain.ChatCompletionResponse
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	assert.Equal(t, "gen-mock-1", out.ID)
	assert.Equal(t, chatReq.Model, out.Model)
	require.Len(t, out.Choices, 1)
	assert.Equal(t, DefaultContent, out.Choices[0].Message.Content)

	calls := s.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, "Bearer sk-teste", calls[0].Header.Get("Authorization"))
	assert.Equal(t, "oi", calls[0].Request.Messages[0].Content)
}

func TestScriptedErrorsThenSuccess(t *testing.T) {
	s := New(t)
	s.Enqueue(RateLimited(7), ServerError(http.StatusBadGateway), Response{Content: "recuperou"})

	resp := post(t, s, chatReq)
	assert.Equal(t, http.StatusTooManyRequests, resp.Status)
	assert.Equal(t, "7", resp.Header.Get("Retry-After"))
	var env struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &env))
	assert.Equal(t, http.StatusTooManyRequests, env.Error.Code)
	assert.NotEmpty(t, env.Error.Message)

	resp = post(t, s, chatReq)
	assert.Equal(t, http.StatusBadGateway, resp.Status)
	assert.Empty(t, resp.Header.Get("Retry-After"))

	resp = post(t, s, chatReq)
	var out domain.ChatCompletionResponse
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	assert.Equal(t, "recuperou", out.Choices[0].Message.Content)
}

func TestStreaming(t *testing.T) {
	s := New(t)
	s.Enqueue(Response{Chunks: []string{"Ol", "á, ", "mundo"}})
	req := chatReq
	req.Stream = true
	resp := post(t, s, req)
	require.Equal(t, http.StatusOK, resp.Status)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	var text strings.Builder
	var finish []string
	var last string
	sc := bufio.NewScanner(bytes.NewReader(resp.Body))
	for sc.Scan() {
		line := sc.Text()
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue // linha em branco ou comentário ": OPENROUTER PROCESSING"
		}
		last = data
		if data == "[DONE]" {
			continue
		}
		var chunk domain.ChatCompletionResponse
		require.NoError(t, json.Unmarshal([]byte(data), &chunk))
		assert.Equal(t, "chat.completion.chunk", chunk.Object)
		text.WriteString(chunk.Choices[0].Delta.Content)
		finish = append(finish, chunk.Choices[0].FinishReason)
	}
	require.NoError(t, sc.Err())
	assert.Equal(t, "Olá, mundo", text.String())
	assert.Equal(t, []string{"", "", "stop"}, finish)
	assert.Equal(t, "[DONE]", last)
}

func TestInvalidJSONIsNotRecorded(t *testing.T) {
	s := New(t)
	s.Enqueue(RateLimited(1))
	resp, err := s.Client().Post(s.URL+"/chat/completions", "application/json", strings.NewReader("{"))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Empty(t, s.Calls())
	assert.Equal(t, http.StatusTooManyRequests, post(t, s, chatReq).Status, "o roteiro não deve ser consumido por requisição inválida")
}

func TestConcurrentCalls(t *testing.T) {
	s := New(t)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, _ := json.Marshal(chatReq)
			resp, err := s.Client().Post(s.URL+"/chat/completions", "application/json", bytes.NewReader(body))
			if err == nil {
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	assert.Len(t, s.Calls(), 20)
}
