# syntax=docker/dockerfile:1

# cgo binding to libopus (internal/audio/opuscodec) needs libopus + pkg-config
# at build time, and libopus's shared runtime library at run time.
FROM golang:1.26.1-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        libopus-dev libopusfile-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /out/agent ./cmd/agent

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libopus0 libopusfile0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/agent /usr/local/bin/agent

ENTRYPOINT ["/usr/local/bin/agent"]
