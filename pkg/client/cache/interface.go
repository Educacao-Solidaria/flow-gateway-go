package cache

import "context"

// LookupResult detalha o resultado da consulta ao cache semântico.
type LookupResult struct {
	Hit            bool    `json:"hit"`
	Score          float32 `json:"score"`
	CachedResponse string  `json:"cached_response,omitempty"`
	LatencyMicros  int64   `json:"latency_micros"`
}

// StatsResult expõe métricas e ocupação do motor de cache.
type StatsResult struct {
	TotalEntries uint64  `json:"total_entries"`
	HitCount     uint64  `json:"hit_count"`
	MissCount    uint64  `json:"miss_count"`
	HitRatio     float32 `json:"hit_ratio"`
}

// Client define o contrato canônico consumido pelo Gateway para interagir com o semantic-cache-rs.
type Client interface {
	Lookup(ctx context.Context, prompt string, embedding []float32, threshold float32) (*LookupResult, error)
	Store(ctx context.Context, prompt string, response string, embedding []float32, ttlSeconds int) error
	Stats(ctx context.Context) (*StatsResult, error)
	Close() error
}
