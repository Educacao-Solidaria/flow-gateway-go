// Package version expõe os metadados de build do gateway.
//
// Os valores padrão servem ao desenvolvimento local; em release eles são
// injetados via -ldflags "-X .../internal/version.Version=v1.2.3".
package version

import "fmt"

var (
	// Version é a versão semântica do binário.
	Version = "dev"
	// Commit é o hash curto do commit de origem.
	Commit = "none"
)

// String devolve a identificação legível do binário.
func String() string {
	return fmt.Sprintf("flow-gateway %s (%s)", Version, Commit)
}
