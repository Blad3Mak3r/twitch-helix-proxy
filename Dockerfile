# Build stage
FROM golang:1.25.1-alpine AS builder

# Install build dependencies
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /build

# Leverage layer cache: copy dependency files before source
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source and build a fully static binary.
# CGO_ENABLED=0: static linking (no libc dependency).
# -trimpath: remove local file paths from the binary (reproducible + smaller).
# -ldflags '-w -s': strip DWARF debug info and symbol table.
# TARGETOS/TARGETARCH are injected by docker buildx for multi-platform builds.
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -trimpath \
    -ldflags='-w -s -extldflags "-static"' \
    -o twitch-proxy \
    .

# Final stage: minimal scratch image
FROM scratch

# CA certificates for outbound HTTPS to Twitch APIs
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Timezone data for accurate log timestamps
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=builder /build/twitch-proxy /twitch-proxy

EXPOSE 3000

# Run as nobody (UID 65534) — no shell available in scratch
USER 65534:65534

ENTRYPOINT ["/twitch-proxy"]
