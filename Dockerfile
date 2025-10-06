# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata file

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum* ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build static binary
# CGO_ENABLED=0: Disable CGO for static binary
# -ldflags: Linker flags to reduce binary size and remove debug info
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a \
    -installsuffix cgo \
    -o twitch-proxy \
    .

# Verify the binary is statically linked
RUN file twitch-proxy | grep -q "statically linked" || (echo "Binary is not static!" && exit 1)

# Final stage
FROM scratch

# Copy CA certificates for HTTPS requests
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy timezone data (optional, for accurate time logs)
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy the static binary
COPY --from=builder /build/twitch-proxy /twitch-proxy

# Expose port
EXPOSE 3000

# Run as non-root user (security best practice)
USER 65534:65534

# Set entrypoint
ENTRYPOINT ["/twitch-proxy"]