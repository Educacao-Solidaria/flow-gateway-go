//go:build linux

package udscache

import (
	"errors"
	"net"
	"syscall"
)

// peerUID é o uid do processo do outro lado (SO_PEERCRED). Fecha a janela que o
// Lstat deixa: entre ele e o connect, o caminho pode ter sido trocado.
func peerUID(nc net.Conn) (int, bool, error) {
	sc, ok := nc.(syscall.Conn)
	if !ok {
		return 0, false, nil
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return 0, false, err
	}
	var cred *syscall.Ucred
	cerr := rc.Control(func(fd uintptr) {
		cred, err = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	if cerr != nil || err != nil {
		return 0, false, errors.Join(cerr, err)
	}
	return int(cred.Uid), true, nil
}
