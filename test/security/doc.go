// Package security testa a auditoria de segurança do gateway (contrato da Fase 5, §6): o
// workflow security.yml (gosec e trivy), o alvo `make audit` e as exceções.
//
// TestWorkflow lê o workflow e o Makefile e confere o portão sem rodar as ferramentas: versões
// exatas (o trivy com SHA-256), as chamadas do gosec e do trivy, permissões, prazos, passos
// condicionais e o checkout sem credencial. TestExceptions confere cada exceção do
// repositório: no .trivyignore, o motivo num comentário logo acima e a data no `exp:`; no
// código Go, cada anotação do gosec com a regra, o motivo e "revisar em AAAA-MM-DD". Os
// testes rodam em todo `go test ./...`; o gosec e o trivy de verdade rodam no CI e no
// `make audit`.
package security
