// Package httpclient monta o cliente HTTP do upstream (OpenRouter): um pool de
// conexões keep-alive reaproveitadas entre chamadas, com prazo em cada fase.
package httpclient

import (
	"net"
	"net/http"
	"time"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
)

// New devolve o *http.Client do upstream; crie um por processo, porque o pool
// mora no Transport dele.
//
// Não há Client.Timeout de propósito: ele conta até o fim do corpo e cortaria
// no meio um stream SSE longo. Cada fase tem o próprio prazo (conectar, TLS,
// esperar os cabeçalhos), e a duração total de uma chamada fica com o ctx da
// requisição (http.NewRequestWithContext; a janela do resilience.Fallback).
func New(cfg config.OpenRouterConfig) *http.Client {
	dialer := &net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: cfg.TCPKeepAlive}
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true, // com DialContext próprio, o HTTP/2 só vem se pedido
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		ExpectContinueTimeout: time.Second,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		MaxIdleConns:          cfg.MaxIdleConnsPerHost, // um upstream só: o total acompanha o por host
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
	}}
}
