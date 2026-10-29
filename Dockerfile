# syntax=docker/dockerfile:1

# cgo binding to libopus (internal/audio/opuscodec) needs libopus + pkg-config
# at build time, and libopus's shared runtime library at run time.
FROM golang:alpine AS build

RUN apk add --no-cache gcc musl-dev pkgconfig opus-dev opusfile-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o /out/agent ./cmd/agent

FROM alpine:3.21

RUN apk add --no-cache opus opusfile ca-certificates

COPY --from=build /out/agent /usr/local/bin/agent

ENTRYPOINT ["/usr/local/bin/agent"]

