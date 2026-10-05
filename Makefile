BINARY    := flow-gateway
MODULE    := github.com/Educacao-Solidaria/flow-gateway-go
MAIN      := ./cmd/flow-gateway
DIST      := dist
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS   := -s -w -X $(MODULE)/internal/version.Version=$(VERSION) -X $(MODULE)/internal/version.Commit=$(COMMIT)
GOBUILD   := CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)"
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64

os   = $(word 1,$(subst /, ,$1))
arch = $(word 2,$(subst /, ,$1))

.DEFAULT_GOAL := help
.PHONY: help build run test race cover lint fmt fmt-check tidy check cross build-linux build-darwin build-windows clean $(PLATFORMS)

help: ## lista os targets disponíveis
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## compila para a plataforma local em bin/
	$(GOBUILD) -o bin/$(BINARY) $(MAIN)

run: build ## compila e executa o gateway
	./bin/$(BINARY)

test: ## roda os testes unitários
	go test -count=1 ./...

race: ## roda os testes com o race detector
	go test -race -count=1 ./...

cover: ## gera coverage.out e mostra o total
	go test -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

lint: ## roda o golangci-lint
	golangci-lint run

fmt: ## formata o código
	gofmt -w .

fmt-check: ## falha se houver arquivo fora do gofmt
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }

tidy: ## sincroniza go.mod/go.sum
	go mod tidy

check: fmt-check lint race ## o mesmo portão do CI

cross: $(PLATFORMS) ## compila para todas as plataformas em dist/

build-linux: linux/amd64 linux/arm64 ## binários linux
build-darwin: darwin/amd64 darwin/arm64 ## binários darwin
build-windows: windows/amd64 windows/arm64 ## binários windows

$(PLATFORMS):
	GOOS=$(call os,$@) GOARCH=$(call arch,$@) $(GOBUILD) \
		-o $(DIST)/$(BINARY)-$(call os,$@)-$(call arch,$@)$(if $(filter windows,$(call os,$@)),.exe) $(MAIN)

clean: ## remove artefatos de build
	rm -rf bin $(DIST) coverage.out
