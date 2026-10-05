package rag

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type memoryDoc struct {
	id      string
	title   string
	content string
	uri     string
}

// InMemoryClient é um cliente simulado para testes que indexa textos na memória.
type InMemoryClient struct {
	mu   sync.RWMutex
	docs []memoryDoc
}

// NewInMemoryClient instancia o mock de RAG thread-safe.
func NewInMemoryClient() *InMemoryClient {
	return &InMemoryClient{
		docs: make([]memoryDoc, 0),
	}
}

// HybridSearch executa uma busca simulada procurando palavras-chave nos documentos cadastrados.
func (c *InMemoryClient) HybridSearch(ctx context.Context, query string, topK int, alpha float32) (*SearchResult, error) {
	start := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()

	terms := strings.Fields(strings.ToLower(query))
	hits := make([]ChunkHit, 0)

	rank := 1
	for _, doc := range c.docs {
		lowerContent := strings.ToLower(doc.content)
		matches := 0
		for _, t := range terms {
			if strings.Contains(lowerContent, t) {
				matches++
			}
		}

		if matches > 0 || len(terms) == 0 {
			score := float32(matches) / float32(len(terms)+1)
			hits = append(hits, ChunkHit{
				ID:        fmt.Sprintf("chk-%s-1", doc.id),
				Content:   doc.content,
				SourceURI: doc.uri,
				Score:     score,
				Rank:      rank,
			})
			rank++
			if topK > 0 && len(hits) >= topK {
				break
			}
		}
	}

	return &SearchResult{
		Query:           query,
		Hits:            hits,
		TotalFound:      len(hits),
		ExecutionTimeMs: float64(time.Since(start).Microseconds()) / 1000.0,
	}, nil
}

// IndexDocument cadastra o texto na memória para subsequente busca.
func (c *InMemoryClient) IndexDocument(ctx context.Context, title string, content string, uri string) (*IndexResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	docID := fmt.Sprintf("doc-%d", len(c.docs)+1)
	c.docs = append(c.docs, memoryDoc{
		id:      docID,
		title:   title,
		content: content,
		uri:     uri,
	})

	return &IndexResult{
		DocumentID:    docID,
		ChunksCreated: 1,
		Success:       true,
	}, nil
}

// Close encerra recursos do cliente.
func (c *InMemoryClient) Close() error {
	return nil
}
