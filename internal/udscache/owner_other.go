//go:build !unix

package udscache

import "os"

// ownerUID: sem uid (Windows), o dono não é conferido; o tipo do arquivo, sim.
func ownerUID(os.FileInfo) (int, bool) { return 0, false }
