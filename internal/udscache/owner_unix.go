//go:build unix

package udscache

import (
	"os"
	"syscall"
)

// ownerUID é o uid do dono do arquivo.
func ownerUID(fi os.FileInfo) (int, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int(st.Uid), true
}
