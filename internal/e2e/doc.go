// Package e2e é a suíte ponta a ponta do streaming do gateway: os componentes
// compostos como a rota /v1/chat/completions (PR 66 do Luan) vai compor —
// Lookup no semantic-cache (udscache), contexto do RAG (raghttp), upstream com
// retry e fallback (resilience) lido pelo sse-reader (sse.Decode), o
// cachewriter no meio do stream e o sse-writer até o cliente — contra fakes da
// OpenRouter (openroutertest), do semantic-cache (o cache em memória do
// pkg/client/cache atrás do IPC v1) e do brain-router. Só tem testes.
package e2e
