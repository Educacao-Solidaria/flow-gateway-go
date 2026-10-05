package rag_test

import (
	"context"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/client/rag"
)

func TestInMemoryClient_IndexAndSearch(t *testing.T) {
	client := rag.NewInMemoryClient()
	ctx := context.Background()

	// 1. Ingestão
	res, err := client.IndexDocument(ctx, "Guia Go", "Go e uma linguagem eficiente com goroutines.", "docs/go.md")
	if err != nil {
		t.Fatalf("erro ao indexar: %v", err)
	}
	if !res.Success || res.DocumentID == "" {
		t.Fatalf("retorno inesperado na indexacao: %+v", res)
	}

	// 2. Busca com match
	searchRes, err := client.HybridSearch(ctx, "linguagem goroutines", 5, 0.5)
	if err != nil {
		t.Fatalf("erro na busca: %v", err)
	}

	if searchRes.TotalFound != 1 {
		t.Fatalf("esperava 1 hit, obteve %d", searchRes.TotalFound)
	}
	if searchRes.Hits[0].SourceURI != "docs/go.md" {
		t.Fatalf("uri de origem incorreta: %s", searchRes.Hits[0].SourceURI)
	}

	// 3. Busca sem match
	noMatch, err := client.HybridSearch(ctx, "ruby rails", 5, 0.5)
	if err != nil {
		t.Fatalf("erro na busca: %v", err)
	}
	if noMatch.TotalFound != 0 {
		t.Fatalf("esperava 0 hits, obteve %d", noMatch.TotalFound)
	}
}
