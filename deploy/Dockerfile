# Build estatico: el binario no depende de libc, asi corre en una imagen sin distro.
FROM golang:1.24-alpine AS build

WORKDIR /src
# Las dependencias se cachean aparte del codigo: un cambio en .go no revalida modulos.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/etl ./cmd/etl

# distroless/static trae los certificados raiz que hacen falta para TLS con
# api.riotgames.com, y nada mas: sin shell ni gestor de paquetes.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/etl /usr/local/bin/etl

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/etl"]
# Sobrescribir en ECS con `command`; por defecto solo informa de la base.
CMD ["stats"]
