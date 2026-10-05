package cache

import (
	"context"
	"math"
	"sync"
	"time"
)

type memoryEntry struct {
	prompt    string
	response  string
	embedding []float32
	createdAt time.Time
	ttl       time.Duration
}

// InMemoryClient é um cliente de desenvolvimento/testes que executa cálculo semântico local.
type InMemoryClient struct {
	mu        sync.RWMutex
	entries   []memoryEntry
	hitCount  uint64
	missCount uint64
}

// NewInMemoryClient instancia um cliente mock thread-safe.
func NewInMemoryClient() *InMemoryClient {
	return &InMemoryClient{
		entries: make([]memoryEntry, 0),
	}
}

func cosineSim(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := 0; i < len(a); i++ {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(normA) * math.Sqrt(normB)))
}

// Lookup busca similaridade semântica entre os vetores armazenados.
func (c *InMemoryClient) Lookup(ctx context.Context, prompt string, embedding []float32, threshold float32) (*LookupResult, error) {
	start := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	var bestScore float32 = -1.0
	var bestResponse string

	now := time.Now()
	for _, entry := range c.entries {
		if entry.ttl > 0 && now.Sub(entry.createdAt) > entry.ttl {
			continue // expirado
		}
		if entry.prompt == prompt {
			c.hitCount++
			return &LookupResult{
				Hit:            true,
				Score:          1.0,
				CachedResponse: entry.response,
				LatencyMicros:  time.Since(start).Microseconds(),
			}, nil
		}
		if len(embedding) > 0 && len(entry.embedding) > 0 {
			score := cosineSim(embedding, entry.embedding)
			if score > bestScore {
				bestScore = score
				bestResponse = entry.response
			}
		}
	}

	if bestScore >= threshold {
		c.hitCount++
		return &LookupResult{
			Hit:            true,
			Score:          bestScore,
			CachedResponse: bestResponse,
			LatencyMicros:  time.Since(start).Microseconds(),
		}, nil
	}

	c.missCount++
	return &LookupResult{
		Hit:           false,
		Score:         bestScore,
		LatencyMicros: time.Since(start).Microseconds(),
	}, nil
}

// Store armazena uma nova entrada no cache mock.
func (c *InMemoryClient) Store(ctx context.Context, prompt string, response string, embedding []float32, ttlSeconds int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ttl time.Duration
	if ttlSeconds > 0 {
		ttl = time.Duration(ttlSeconds) * time.Second
	}

	c.entries = append(c.entries, memoryEntry{
		prompt:    prompt,
		response:  response,
		embedding: embedding,
		createdAt: time.Now(),
		ttl:       ttl,
	})
	return nil
}

// Stats retorna contadores acumulados de hits e misses.
func (c *InMemoryClient) Stats(ctx context.Context) (*StatsResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	totalReqs := c.hitCount + c.missCount
	var ratio float32
	if totalReqs > 0 {
		ratio = float32(c.hitCount) / float32(totalReqs)
	}

	return &StatsResult{
		TotalEntries: uint64(len(c.entries)),
		HitCount:     c.hitCount,
		MissCount:    c.missCount,
		HitRatio:     ratio,
	}, nil
}

// Close encerra recursos do cliente.
func (c *InMemoryClient) Close() error {
	return nil
}
