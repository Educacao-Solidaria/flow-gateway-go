# Imagem do flow-gateway (contrato da Fase 5, §2): binário estático numa distroless
# sem shell, rodando como 65532:65532 e ouvindo em :8080 (padrão de FLOW_SERVER_ADDR).
#   make docker   (ou: docker build --build-arg COMMIT="$(git rev-parse --short HEAD)" -t educacao-solidaria/flow-gateway:dev .)
# Sem HEALTHCHECK: a imagem não tem shell nem curl; quem orquestra sonda /livez de fora
# (test/docker, rodado pelo workflow docker.yml).
#
# ponytail: o estágio de build roda na plataforma do alvo, então um build cruzado
# (arm64 num host amd64) emula o compilador. Para publicar multi-arch, use
# `FROM --platform=$BUILDPLATFORM` e GOOS/GOARCH vindos de TARGETOS/TARGETARCH.

# golang:1.27.3-alpine3.24 (índice multi-arquitetura)
FROM golang:1.27.1-alpine3.24@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
ARG VERSION=dev
ARG COMMIT=none
WORKDIR /src
# Código por bind mount (o .dockerignore só deixa passar o que o build lê) e caches
# do Go no BuildKit: nada disso vira camada.
RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/Educacao-Solidaria/flow-gateway-go/internal/version.Version=${VERSION} -X github.com/Educacao-Solidaria/flow-gateway-go/internal/version.Commit=${COMMIT}" \
      -o /out/flow-gateway ./cmd/flow-gateway

# gcr.io/distroless/static-debian12:nonroot (índice multi-arquitetura; traz CAs e tzdata)
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
# O binário fica do root (0755): o processo não reescreve o próprio executável.
COPY --from=build /out/flow-gateway /usr/local/bin/flow-gateway
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/flow-gateway"]
