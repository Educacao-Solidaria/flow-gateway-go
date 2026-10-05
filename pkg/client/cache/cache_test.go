package cache_test

import (
	"context"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/cache"
)

func TestInMemoryClient_ExactHit(t *testing.T) {
	client := cache.NewInMemoryClient()
	ctx := context.Background()

	prompt := "Qual e a capital do Brasil?"
	resp := "Brasilia"

	err := client.Store(ctx, prompt, resp, []float32{1.0, 0.0}, 0)
	if err != nil {
		t.Fatalf("erro ao armazenar no cache: %v", err)
	}

	result, err := client.Lookup(ctx, prompt, []float32{1.0, 0.0}, 0.90)
	if err != nil {
		t.Fatalf("erro no lookup: %v", err)
	}

	if !result.Hit {
		t.Fatal("esperava cache hit exato")
	}
	if result.CachedResponse != resp {
		t.Fatalf("resposta divergente: %s", result.CachedResponse)
	}

	stats, _ := client.Stats(ctx)
	if stats.HitCount != 1 {
		t.Fatalf("hit count deve ser 1, obteve %d", stats.HitCount)
	}
}

func TestInMemoryClient_SemanticHit(t *testing.T) {
	client := cache.NewInMemoryClient()
	ctx := context.Background()

	_ = client.Store(ctx, "ola amigo", "resposta amigavel", []float32{1.0, 0.0}, 0)

	// Vetor muito próximo (cosseno ~0.999)
	result, err := client.Lookup(ctx, "ola companheiro", []float32{0.999, 0.01}, 0.95)
	if err != nil {
		t.Fatalf("erro no lookup: %v", err)
	}

	if !result.Hit {
		t.Fatal("esperava hit semantico com similaridade alta")
	}
}
