package ringbuffer

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRing[T any](t *testing.T, capacity int) *Ring[T] {
	t.Helper()
	r, err := New[T](capacity)
	require.NoError(t, err)
	return r
}

// seqs extrai os números de sequência, para comparar em uma linha.
func seqs[T any](events []Event[T]) []uint64 {
	out := make([]uint64, len(events))
	for i, e := range events {
		out[i] = e.Seq
	}
	return out
}

func TestNewRejectsNonPositiveCapacity(t *testing.T) {
	for _, c := range []int{0, -1, math.MinInt} {
		_, err := New[string](c)
		assert.Error(t, err, "capacidade %d", c)
	}
}

func TestEmptyRing(t *testing.T) {
	r := newRing[string](t, 2)
	assert.Empty(t, r.Snapshot())
	events, complete := r.Since(0)
	assert.Empty(t, events)
	assert.True(t, complete, "nada aconteceu, nada se perdeu")
	events, complete = r.Since(1)
	assert.Nil(t, events)
	assert.False(t, complete, "id do futuro não é deste stream")
}

func TestPushAndSinceBeforeWrap(t *testing.T) {
	r := newRing[string](t, 4)
	for i, v := range []string{"a", "b", "c"} {
		assert.EqualValues(t, i+1, r.Push(v))
	}
	assert.Equal(t, []Event[string]{{1, "a"}, {2, "b"}, {3, "c"}}, r.Snapshot())

	events, complete := r.Since(2)
	assert.Equal(t, []Event[string]{{3, "c"}}, events)
	assert.True(t, complete)
	events, complete = r.Since(3)
	assert.Empty(t, events, "cliente em dia não recebe nada")
	assert.True(t, complete)
	events, complete = r.Since(4)
	assert.Nil(t, events)
	assert.False(t, complete)
}

func TestOverwriteOldest(t *testing.T) {
	r := newRing[string](t, 3)
	for _, v := range []string{"e1", "e2", "e3", "e4", "e5"} {
		r.Push(v)
	}
	assert.Equal(t, []Event[string]{{3, "e3"}, {4, "e4"}, {5, "e5"}}, r.Snapshot())

	events, complete := r.Since(2)
	assert.Equal(t, []uint64{3, 4, 5}, seqs(events))
	assert.True(t, complete, "viu até o 2 e o 3 em diante está todo aqui")
	events, complete = r.Since(1)
	assert.Equal(t, []uint64{3, 4, 5}, seqs(events), "vêm os que restam")
	assert.False(t, complete, "o 2 foi sobrescrito")
	_, complete = r.Since(0)
	assert.False(t, complete)
}

func TestCapacityOne(t *testing.T) {
	r := newRing[string](t, 1)
	r.Push("x")
	assert.EqualValues(t, 2, r.Push("y"))
	assert.Equal(t, []Event[string]{{2, "y"}}, r.Snapshot())
	events, complete := r.Since(1)
	assert.Equal(t, []Event[string]{{2, "y"}}, events)
	assert.True(t, complete)
	_, complete = r.Since(0)
	assert.False(t, complete)
}

func TestReadsAreCopies(t *testing.T) {
	r := newRing[string](t, 2)
	r.Push("a")
	r.Push("b")
	snap := r.Snapshot()
	snap[0].Value = "alterado"
	r.Push("c") // reaproveita a posição de "a" por dentro
	assert.Equal(t, []Event[string]{{1, "alterado"}, {2, "b"}}, snap, "a cópia não acompanha o buffer")
	assert.Equal(t, []Event[string]{{2, "b"}, {3, "c"}}, r.Snapshot(), "nem o buffer acompanha a cópia")
}

// Escritores e leitores ao mesmo tempo (rode com -race -count=N): toda leitura
// vê uma sequência contígua, e no fim restam os últimos cap eventos.
func TestConcurrentPushAndRead(t *testing.T) {
	const capacity, writers, perWriter = 64, 8, 500
	r := newRing[uint64](t, capacity)
	var pushed sync.Map // seq -> valor gravado
	var writersWG, readersWG sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := 0; i < perWriter; i++ {
				v := uint64(w*perWriter + i)
				pushed.Store(r.Push(v), v)
			}
		}()
	}
	for g := 0; g < 4; g++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				events := r.Snapshot()
				assert.LessOrEqual(t, len(events), capacity)
				for i := 1; i < len(events); i++ {
					if events[i].Seq != events[i-1].Seq+1 {
						t.Errorf("leitura não contígua: %v", seqs(events))
						return
					}
				}
			}
		}()
	}
	writersWG.Wait()
	close(stop)
	readersWG.Wait()

	events := r.Snapshot()
	require.Len(t, events, capacity)
	assert.EqualValues(t, writers*perWriter, events[len(events)-1].Seq)
	for _, e := range events {
		v, ok := pushed.Load(e.Seq)
		require.True(t, ok)
		assert.Equal(t, v, e.Value, "seq %d com o valor de outro Push", e.Seq)
	}
}
