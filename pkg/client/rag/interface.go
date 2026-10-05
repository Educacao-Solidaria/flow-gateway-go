package rag

import "context"

// ChunkHit representa um trecho relevante recuperado pelo serviço de RAG.
type ChunkHit struct {
	ID        string  `json:"id"`
	Content   string  `json:"content"`
	SourceURI string  `json:"source_uri"`
	Score     float32 `json:"score"`
	Rank      int     `json:"rank"`
}

// SearchResult consolida os chunks encontrados na busca híbrida.
type SearchResult struct {
	Query            string     `json:"query"`
	Hits             []ChunkHit `json:"hits"`
	TotalFound       int        `json:"total_found"`
	ExecutionTimeMs float64    `json:"execution_time_ms"`
}

// IndexResult detalha a confirmação de indexação de um novo documento.
type IndexResult struct {
	DocumentID    string `json:"document_id"`
	ChunksCreated int    `json:"chunks_created"`
	Success       bool   `json:"success"`
}

// Client define a interface de comunicação do Gateway com o brain-router-py.
type Client interface {
	HybridSearch(ctx context.Context, query string, topK int, alpha float32) (*SearchResult, error)
	IndexDocument(ctx context.Context, title string, content string, uri string) (*IndexResult, error)
	Close() error
}
