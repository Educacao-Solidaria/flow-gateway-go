// Package release testa a release do gateway: o .goreleaser.yaml, o workflow release.yml e
// os pacotes que o GoReleaser gera.
//
// TestConfig lê os dois arquivos e confere o contrato da Fase 5 (§6) sem o goreleaser: roda
// em todo `go test ./...`. TestSnapshot confere o dist/ de um `goreleaser release
// --snapshot` (sem RELEASE_DIST, pula); o job snapshot do release.yml e o `make
// release-snapshot` rodam:
//
//	RELEASE_DIST=dist go test -count=1 -timeout 2m -run '^TestSnapshot$' ./test/release/
package release
