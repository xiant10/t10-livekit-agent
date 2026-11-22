# syntax=docker/dockerfile:1

# cgo binding to libopus (internal/audio/opuscodec) needs libopus + pkg-config
# at build time, and libopus's shared runtime library at run time.
FROM golang:1.27.4-alpine AS build

RUN apk add --no-cache gcc musl-dev pkgconfig opus-dev opusfile-dev

WORKDIR /src
COPY go.mod go.sum ./
COPY third_party/t10-modkit ./third_party/t10-modkit
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=1 go build -ldflags="-s -w -X main.version=${VERSION}" -o /out/t10-livekit-agent ./cmd/t10-livekit-agent

FROM alpine:3.21

RUN apk add --no-cache opus opusfile ca-certificates

COPY --from=build /out/t10-livekit-agent /usr/local/bin/t10-livekit-agent

USER 65534:65534
# The docs and health routes, when LIVEKIT_AGENT_LISTEN_ADDR is set.
EXPOSE 18210

ENTRYPOINT ["/usr/local/bin/t10-livekit-agent"]
# Logs go to stderr: the default file can't be written by this user.
CMD ["-log-file="]

