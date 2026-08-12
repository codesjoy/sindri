# syntax=docker/dockerfile:1

FROM golang:1.26.4 AS build

WORKDIR /src

COPY go.mod go.sum go.work go.work.sum ./
COPY gen/go/go.mod gen/go/go.sum ./gen/go/
COPY pkg/sequence/go.mod pkg/sequence/go.sum ./pkg/sequence/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/goose github.com/pressly/goose/v3/cmd/goose

FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=build /out/goose /app/goose
COPY migrations/sequence /app/migrations

ENTRYPOINT ["/app/goose"]
CMD ["up"]
