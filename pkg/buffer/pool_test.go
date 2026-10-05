package buffer_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/buffer"
)

func TestPool_BasicGetAndPut(t *testing.T) {
	p := buffer.NewPool(buffer.Config{
		InitialCapacity: 128,
		MaxCapacity:     1024,
	})

	buf := p.Get()
	if buf == nil {
		t.Fatal("buffer nulo retornado")
	}

	buf.WriteString("hello flow gateway")
	if buf.String() != "hello flow gateway" {
		t.Fatalf("conteudo inesperado: %s", buf.String())
	}

	p.Put(buf)

	buf2 := p.Get()
	if buf2.Len() != 0 {
		t.Fatalf("esperava buffer resetado com len 0, obteve %d", buf2.Len())
	}
	p.Put(buf2)

	gets, puts, drops := p.Stats()
	if gets != 2 || puts != 2 || drops != 0 {
		t.Fatalf("stats incorretas: gets=%d puts=%d drops=%d", gets, puts, drops)
	}
}

func TestPool_MaxCapacityDrop(t *testing.T) {
	p := buffer.NewPool(buffer.Config{
		InitialCapacity: 64,
		MaxCapacity:     256,
	})

	buf := p.Get()
	// Escreve mais que MaxCapacity para expandir o buffer além de 256 bytes
	largeData := bytes.Repeat([]byte("a"), 512)
	buf.Write(largeData)

	p.Put(buf)

	_, _, drops := p.Stats()
	if drops != 1 {
		t.Fatalf("esperava 1 drop de buffer grande, obteve %d", drops)
	}
}

func TestPool_ConcurrentAccess(t *testing.T) {
	p := buffer.NewPool(buffer.Config{
		InitialCapacity: 256,
		MaxCapacity:     4096,
	})

	var wg sync.WaitGroup
	workers := 20
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				b := p.Get()
				b.WriteString("dado concorrente")
				p.Put(b)
			}
		}()
	}

	wg.Wait()
	gets, puts, _ := p.Stats()
	expected := int64(workers * iterations)
	if gets != expected || puts != expected {
		t.Fatalf("esperava %d operacoes, obteve gets=%d puts=%d", expected, gets, puts)
	}
}

func TestGlobalPool_Helpers(t *testing.T) {
	buf := buffer.GetBytes()
	if buf == nil {
		t.Fatal("buffer nulo")
	}
	buf.WriteString("teste global")
	buffer.PutBytes(buf)
	buffer.PutBytes(nil) // Nao deve entrar em panico
}
