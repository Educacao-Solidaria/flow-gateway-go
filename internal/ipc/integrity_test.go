package ipc_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/ipc"
)

// TestEveryByteChangeIsRejected troca cada byte de cada frame de referência
// pelos outros 255 valores: nenhuma troca passa. Fora o payload_len, todas
// caem como *FrameError (o crc32 pega todo erro de até 32 bits seguidos). O
// payload_len trocado ainda cai: acima do teto (too_large), curto (crc) ou
// mais longo que o que veio (io.ErrUnexpectedEOF, à espera do resto).
func TestEveryByteChangeIsRejected(t *testing.T) {
	for _, tc := range refs {
		raw := mustHex(t, tc.hex)
		for off := range raw {
			for x := 1; x < 256; x++ {
				bad := bytes.Clone(raw)
				bad[off] ^= byte(x)
				_, err := ipc.ReadFrame(bytes.NewReader(bad))
				var fe *ipc.FrameError
				if off >= 12 && off < ipc.HeaderSize && !errors.As(err, &fe) {
					require.ErrorIs(t, err, io.ErrUnexpectedEOF, "%s[%d]^%#02x", tc.name, off, x)
					continue
				}
				require.ErrorAs(t, err, &fe, "%s[%d]^%#02x", tc.name, off, x)
			}
		}
	}
}
