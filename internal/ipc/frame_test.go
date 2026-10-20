package ipc_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"runtime"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ipc"
)

// Frames de referência do contrato (docs/ipc-v1.md), os mesmos bytes que o
// semantic-cache-rs confere do lado dele.
var refs = []struct {
	name      string
	kind      ipc.Kind
	requestID uint32
	payload   string
	hex       string
}{
	{"ping", ipc.KindPing, 1, "", "53434950010e00000000000100000000c47a57de"},
	{"lookup_req", ipc.KindLookupReq, 7, `{"prompt":"olá","embedding":[0.5,-0.25],"threshold":0.75}`,
		"5343495001010000000000070000003a7b2270726f6d7074223a226f6cc3a1222c22656d62656464696e67223a5b302e352c2d302e32355d2c227468726573686f6c64223a302e37357d9cad1a2b"},
	{"error", ipc.KindError, 7, `{"code":"bad_request","message":"dimensão 2 ≠ 1536"}`,
		"53434950017f000000000007000000377b22636f6465223a226261645f72657175657374222c226d657373616765223a2264696d656e73c3a36f203220e289a02031353336227d0777f547"},
}

// flippedLookup é o LOOKUP_REQ de referência com 1 bit trocado no offset 20.
const flippedLookup = "5343495001010000000000070000003a7b2270726e6d7074223a226f6cc3a1222c22656d62656464696e67223a5b302e352c2d302e32355d2c227468726573686f6c64223a302e37357d9cad1a2b"

func mustHex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// reseal recalcula o crc depois de o teste alterar o frame, para que só a
// regra sob teste possa recusá-lo.
func reseal(raw []byte) []byte {
	n := len(raw) - ipc.TrailerSize
	binary.BigEndian.PutUint32(raw[n:], crc32.ChecksumIEEE(raw[:n]))
	return raw
}

func TestReferenceFrames(t *testing.T) {
	for _, tc := range refs {
		t.Run(tc.name, func(t *testing.T) {
			want := mustHex(t, tc.hex)
			f := ipc.Frame{Kind: tc.kind, RequestID: tc.requestID, Payload: []byte(tc.payload)}

			got, err := ipc.Append(nil, f)
			require.NoError(t, err)
			assert.Equal(t, tc.hex, hex.EncodeToString(got))

			// O crc cobre só o frame, não o que já estava em dst.
			got, err = ipc.Append([]byte("prefixo"), f)
			require.NoError(t, err)
			assert.Equal(t, "prefixo"+string(want), string(got))

			// Lido de uma vez ou byte a byte, dá o mesmo frame e não passa do fim dele.
			for _, r := range []io.Reader{bytes.NewReader(want), iotest.OneByteReader(bytes.NewReader(want))} {
				dec, err := ipc.ReadFrame(r)
				require.NoError(t, err)
				assert.Equal(t, tc.kind, dec.Kind)
				assert.Zero(t, dec.Flags)
				assert.Equal(t, tc.requestID, dec.RequestID)
				assert.Equal(t, tc.payload, string(dec.Payload))
				_, err = ipc.ReadFrame(r)
				assert.ErrorIs(t, err, io.EOF)
			}
		})
	}
}

func TestFlippedBitIsRejectedByCRC(t *testing.T) {
	_, err := ipc.ReadFrame(bytes.NewReader(mustHex(t, flippedLookup)))
	var fe *ipc.FrameError
	require.ErrorAs(t, err, &fe)
	assert.Equal(t, ipc.FrameError{Code: ipc.CodeBadFrame, RequestID: 7, Reason: "crc"}, *fe)
	// A mensagem não carrega nada do payload (pode ser prompt do usuário).
	assert.EqualError(t, err, "ipc: bad_frame: crc")
}

func TestHeaderDefects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset int
		value  byte
		reason string
	}{
		{"magic", 0, 'X', "magic"},
		{"versão", 4, 2, "versão 2"},
		{"flags", 7, 0x01, "flags 0x0001"},
		{"flags alto", 6, 0x80, "flags 0x8000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustHex(t, refs[1].hex)
			raw[tc.offset] = tc.value
			_, err := ipc.ReadFrame(bytes.NewReader(reseal(raw)))
			var fe *ipc.FrameError
			require.ErrorAs(t, err, &fe)
			assert.Equal(t, ipc.FrameError{Code: ipc.CodeBadFrame, RequestID: 7, Reason: tc.reason}, *fe)
		})
	}
}

func TestPayloadLimit(t *testing.T) {
	// Só o cabeçalho, declarando 1 byte além do teto: a recusa vem antes de
	// tentar ler (seria io.ErrUnexpectedEOF) e antes de alocar o payload.
	hdr := mustHex(t, refs[0].hex)[:ipc.HeaderSize]
	binary.BigEndian.PutUint32(hdr[12:], ipc.MaxPayload+1)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := ipc.ReadFrame(bytes.NewReader(hdr))
	runtime.ReadMemStats(&after)
	var fe *ipc.FrameError
	require.ErrorAs(t, err, &fe)
	assert.Equal(t, ipc.FrameError{Code: ipc.CodeTooLarge, RequestID: 1, Reason: "payload_len 16777217"}, *fe)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "alocou o payload recusado")

	// Exatamente no teto passa nos dois sentidos.
	big := ipc.Frame{Kind: ipc.KindStoreReq, RequestID: 2, Payload: bytes.Repeat([]byte{'a'}, ipc.MaxPayload)}
	raw, err := ipc.Append(nil, big)
	require.NoError(t, err)
	dec, err := ipc.ReadFrame(bytes.NewReader(raw))
	require.NoError(t, err)
	assert.Len(t, dec.Payload, ipc.MaxPayload)

	// Acima do teto, ou com flags, o encoder recusa e devolve dst intacto.
	for _, f := range []ipc.Frame{
		{Kind: ipc.KindStoreReq, Payload: make([]byte, ipc.MaxPayload+1)},
		{Kind: ipc.KindPing, Flags: 1},
	} {
		dst, err := ipc.Append([]byte("x"), f)
		require.Error(t, err)
		assert.Equal(t, "x", string(dst))
	}
}

func TestTruncatedStream(t *testing.T) {
	raw := mustHex(t, refs[1].hex)
	for cut := 0; cut < len(raw); cut++ {
		want := io.ErrUnexpectedEOF
		if cut == 0 {
			want = io.EOF // fim limpo, entre frames
		}
		_, err := ipc.ReadFrame(bytes.NewReader(raw[:cut]))
		require.ErrorIs(t, err, want, "corte no byte %d", cut)
	}
}

func TestPipelineAndUnknownKind(t *testing.T) {
	frames := []ipc.Frame{
		{Kind: 0x42, RequestID: 1, Payload: []byte(`{}`)}, // quem despacha responde unsupported
		{Kind: ipc.KindPing, RequestID: 2},
		{Kind: ipc.KindStoreResp, RequestID: math.MaxUint32, Payload: []byte(`{}`)},
	}
	var stream []byte
	for _, f := range frames {
		var err error
		stream, err = ipc.Append(stream, f)
		require.NoError(t, err)
	}
	r := iotest.OneByteReader(bytes.NewReader(stream))
	for _, want := range frames {
		got, err := ipc.ReadFrame(r)
		require.NoError(t, err)
		assert.Equal(t, want.Kind, got.Kind)
		assert.Equal(t, want.RequestID, got.RequestID)
		assert.Equal(t, string(want.Payload), string(got.Payload))
	}
	_, err := ipc.ReadFrame(r)
	assert.ErrorIs(t, err, io.EOF)
}

// FuzzReadFrame: entrada arbitrária nunca causa panic; o que decodifica
// recodifica nos mesmos bytes; o resto é FrameError ou EOF.
func FuzzReadFrame(f *testing.F) {
	for _, tc := range refs {
		f.Add(mustHex(f, tc.hex))
	}
	f.Add(mustHex(f, flippedLookup))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		got, err := ipc.ReadFrame(r)
		var fe *ipc.FrameError
		switch {
		case err == nil:
			enc, err := ipc.Append(nil, got)
			if err != nil || !bytes.Equal(enc, data[:len(data)-r.Len()]) {
				t.Fatalf("recodificação diverge (err=%v)", err)
			}
		case errors.As(err, &fe):
			if fe.Code != ipc.CodeBadFrame && fe.Code != ipc.CodeTooLarge {
				t.Fatalf("código inesperado %q", fe.Code)
			}
		case !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF):
			t.Fatalf("erro inesperado: %v", err)
		}
	})
}

// FuzzRoundTrip: todo frame válido volta igual ao passar pelo encoder e pelo decoder.
func FuzzRoundTrip(f *testing.F) {
	f.Add(byte(ipc.KindLookupReq), uint32(7), []byte(refs[1].payload))
	f.Add(byte(ipc.KindPing), uint32(0), []byte{})
	f.Fuzz(func(t *testing.T, kind byte, id uint32, payload []byte) {
		raw, err := ipc.Append(nil, ipc.Frame{Kind: ipc.Kind(kind), RequestID: id, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		got, err := ipc.ReadFrame(bytes.NewReader(raw))
		if err != nil || got.Kind != ipc.Kind(kind) || got.RequestID != id || !bytes.Equal(got.Payload, payload) {
			t.Fatalf("ida e volta diverge (err=%v)", err)
		}
	})
}

// BenchmarkFrame usa um payload do tamanho de um LOOKUP_REQ com embedding de
// 1536 dimensões em JSON (~18 KB).
func BenchmarkFrame(b *testing.B) {
	f := ipc.Frame{Kind: ipc.KindLookupReq, RequestID: 1, Payload: bytes.Repeat([]byte("0.123456789,"), 1536)}
	raw, err := ipc.Append(nil, f)
	require.NoError(b, err)
	b.Run("Append", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))
		buf := make([]byte, 0, len(raw))
		for i := 0; i < b.N; i++ {
			buf, _ = ipc.Append(buf[:0], f)
		}
	})
	b.Run("ReadFrame", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))
		r := bytes.NewReader(raw)
		for i := 0; i < b.N; i++ {
			r.Reset(raw)
			if _, err := ipc.ReadFrame(r); err != nil {
				b.Fatal(err)
			}
		}
	})
}
