//go:build !linux

package udscache

import "net"

// peerUID: fora do Linux, o pacote syscall não tem SO_PEERCRED e só o Lstat
// confere. ponytail: no macOS, o LOCAL_PEERCRED do golang.org/x/sys fecharia a
// mesma janela entre o Lstat e o connect.
func peerUID(net.Conn) (int, bool, error) { return 0, false, nil }
