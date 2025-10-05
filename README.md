# Twitch API Proxy

A high-performance, production-ready HTTP proxy for the Twitch API with intelligent rate limiting and automatic OAuth token management.

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat&logo=docker)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## ✨ Features

- **🔐 Automatic OAuth Management**: Handles Twitch OAuth token acquisition and renewal automatically
- **⚡ Intelligent Rate Limiting**: Synchronizes with Twitch's rate limit headers to prevent 429 errors
- **🔄 Concurrent Request Handling**: Non-blocking, concurrent request processing with version detection
- **🛡️ Stale Response Detection**: Ignores out-of-order responses using bucket versioning
- **🔁 Automatic Retry Logic**: Handles 401 and 429 errors with exponential backoff
- **📊 Status Endpoint**: Real-time monitoring of rate limits and token expiry
- **🐳 Docker Ready**: Optimized multi-stage build with scratch base image (~10 MB)
- **🚀 Zero Dependencies**: Uses only Go standard library

## 🎯 Why Use This Proxy?

When working with the Twitch API, you need to:
- Manage OAuth tokens and renewal
- Respect rate limits (800 requests/minute)
- Handle 429 responses gracefully
- Deal with concurrent request ordering issues

This proxy handles all of that automatically, letting you focus on building your application.

## 🚀 Quick Start

### Using Docker

```bash
# Run with Docker
docker run -d \
  -p 3000:3000 \
  -e TWITCH_CLIENT_ID=your_client_id \
  -e TWITCH_CLIENT_SECRET=your_client_secret \
  --name twitch-proxy \
  twitch-proxy:latest
```

### Using Docker Compose

```bash
# Create .env file
echo "TWITCH_CLIENT_ID=your_client_id" > .env
echo "TWITCH_CLIENT_SECRET=your_client_secret" >> .env

# Start the proxy
docker-compose up -d
```

### Build from Source

```bash
# Clone the repository
git clone https://github.com/blad3mak3r/twitch-helix-proxy.git
cd twitch-helix-proxy

# Build
go build -o twitch-helix-proxy .

# Run
export TWITCH_CLIENT_ID=your_client_id
export TWITCH_CLIENT_SECRET=your_client_secret
./twitch-helix-proxy
```

## 📡 Usage

Once running, the proxy listens on port `3000`. Simply replace `https://api.twitch.tv` with `http://localhost:3000` in your API calls:

```bash
# Original Twitch API call
curl -H "Authorization: Bearer token" \
     -H "Client-Id: client_id" \
     https://api.twitch.tv/helix/users?login=twitchdev

# Through the proxy (authentication handled automatically)
curl http://localhost:3000/helix/users?login=twitchdev
```

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `/helix/*` | Proxy to Twitch API |
| `/health` | Health check endpoint |
| `/status` | Rate limit and token status |

### Status Endpoint

```bash
curl http://localhost:3000/status
```

Response:
```json
{
  "tokens_remaining": 742,
  "reset_in_seconds": 23.4,
  "token_expires_in_seconds": 3298.7
}
```

## 🔧 Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TWITCH_CLIENT_ID` | Yes | Your Twitch application Client ID |
| `TWITCH_CLIENT_SECRET` | Yes | Your Twitch application Client Secret |

### Rate Limiting Configuration

The proxy is configured with conservative defaults:
- **12 requests/second** (720/minute, below Twitch's 800/minute limit)
- **50 token buffer** (pauses requests when fewer than 50 tokens remain)
- **10 minute token renewal buffer** (renews OAuth tokens 10 minutes before expiry)

These values can be adjusted in the code if needed for your specific use case.

## 🏗️ Architecture

### Rate Limiting Algorithm

The proxy uses a sophisticated rate limiting strategy:

1. **Token Bucket Synchronization**: Tracks Twitch's rate limit bucket in real-time
2. **Version Detection**: Uses `Ratelimit-Reset` timestamp to identify bucket windows
3. **Stale Response Filtering**: Ignores out-of-order responses by tracking the lowest `Ratelimit-Remaining` seen
4. **Preventive Pausing**: Stops sending requests when approaching the limit

```
Request A → tokens=800
Request B → tokens=799
Request C → tokens=798

Twitch processes: C, B, A (out of order)

C responds: remaining=797 → ✅ Update (lowest seen)
B responds: remaining=798 → ❌ Ignore (stale, 798 > 797)
A responds: remaining=799 → ❌ Ignore (stale, 799 > 797)
```

### OAuth Token Management

- Automatically obtains tokens using Client Credentials flow
- Renews tokens 10 minutes before expiry (or 20% of lifetime for short-lived tokens)
- Handles 401 responses by immediately renewing and retrying
- Thread-safe token access with RWMutex

## 📊 Performance

- **Latency Overhead**: ~1-2ms (logging and header processing)
- **Memory Usage**: ~10-20 MB (Go runtime + minimal state)
- **Concurrent Requests**: Handles thousands of concurrent requests
- **Docker Image Size**: ~8-10 MB (static binary on scratch)

## 🐳 Docker Details

The Docker image uses a multi-stage build:

```dockerfile
# Stage 1: Build static binary
FROM golang:1.21-alpine AS builder
# ... build process ...

# Stage 2: Minimal runtime
FROM scratch
# Only binary + CA certs
```

**Benefits:**
- ✅ Static binary (no dependencies)
- ✅ Minimal attack surface (scratch base)
- ✅ Small image size (~10 MB)
- ✅ Non-root user (UID 65534)
- ✅ No shell (security)

## 📈 Monitoring

The proxy provides detailed logs:

```
🚀 Twitch proxy running on http://localhost:3000
🔑 Requesting new access token...
✅ Token obtained (expires in 5400 seconds, renewal in 80.0 minutes)
⏰ Next token renewal in 80.0 minutes
🔄 GET https://api.twitch.tv/helix/users?login=twitchdev
📊 [125ms] Rate limit: 797/800 tokens (reset: 1728123460)
```

**Log Indicators:**
- 🔑 Authentication events
- 📊 Rate limit status
- ⚠️ Warnings (low tokens)
- ❌ Errors (429, 401, etc.)
- 🔄 Bucket resets
- ⏪ Stale responses ignored

## 🛠️ Development

### Prerequisites

- Go 1.21 or higher
- Docker (optional, for containerization)

### Build

```bash
# Build binary
make build

# Run locally
make run

# Run tests
make test

# Build Docker image
make docker-build

# Check binary size
make size
```

### Project Structure

```
.
├── main.go              # Main application code
├── go.mod               # Go module definition
├── Dockerfile           # Multi-stage Docker build
├── docker-compose.yml   # Docker Compose configuration
├── Makefile             # Build automation
├── .dockerignore        # Docker build exclusions
└── README.md            # This file
```

## 🔒 Security

- Uses HTTPS for all Twitch API communications
- OAuth tokens stored in memory only (never persisted)
- Non-root container user (UID 65534)
- Read-only root filesystem support
- No shell in final Docker image
- Static binary (no shared library vulnerabilities)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built for the [Twitch API](https://dev.twitch.tv/docs/api/)
- Follows [Twitch rate limiting guidelines](https://dev.twitch.tv/docs/api/guide#rate-limits)

## 📧 Support

If you encounter any issues or have questions:
- Open an [Issue](https://github.com/blad3mak3r/twitch-helix-proxy/issues)
- Check existing [Discussions](https://github.com/blad3mak3r/twitch-helix-proxy/discussions)

---

Made with ❤️ for the Twitch developer community