// Package docker testa a imagem do gateway (Dockerfile e .dockerignore na raiz) e o
// compose do ecossistema (deploy/compose).
//
// TestDockerfile, TestDockerignore, TestDockerWorkflow e TestCompose leem os arquivos e
// conferem o contrato da Fase 5 sem Docker: rodam em todo `go test ./...`, assim como o
// TestBinarySmoke (o smoke no binário deste checkout). Os TestImage* precisam do Docker e
// da imagem já construída em GATEWAY_IMAGE (sem ela, pulam); o TestImageSmoke passa o
// mesmo smoke no container. O workflow docker.yml constrói a imagem e roda, reprovando o
// teste que pula:
//
//	GATEWAY_IMAGE=educacao-solidaria/flow-gateway:dev go test -count=1 -run '^TestImage' ./test/docker/
//
// Os TestStack* sondam o ecossistema já de pé (`make compose`; sem COMPOSE_STACK,
// pulam); o workflow compose.yml sobe os 4 repositórios lado a lado e roda:
//
//	COMPOSE_STACK=1 go test -count=1 -run '^TestStack' ./test/docker/
package docker
