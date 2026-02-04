# BlobForge

**BlobForge** is a distributed job queue system for processing large files (PDFs, documents, etc.) across multiple workers.

## Features

- 🔐 **OIDC Authentication** - Secure login with group-based access control
- 🔑 **API Tokens** - For worker authentication and CLI access
- 📊 **Live Dashboard** - Real-time updates via Server-Sent Events (SSE)
- ⚡ **Flexible Priority** - 5-level priority system (1=critical to 5=background)
- 💾 **Litestream Backup** - Continuous SQLite replication to S3
- 🔄 **Worker Management** - Drain, remove, and monitor workers from UI
- 🎯 **Job Management** - Retry, cancel, and reprioritize from dashboard
- 🐳 **GPU Support** - marker-pdf workers with CUDA acceleration

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         BlobForge Server                        │
│                           (Go + SQLite)                         │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   REST API  │  │  Dashboard  │  │      Litestream         │  │
│  │   /api/*    │  │ (HTMX+SSE)  │  │  (SQLite → S3 backup)   │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                          │                                      │
│  ┌─────────────┐  ┌──────┴──────┐  ┌─────────────────────────┐  │
│  │ OIDC/Auth   │  │   SQLite    │  │    SSE Hub (live)       │  │
│  │ middleware  │  │   (WAL)     │  │    updates              │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
        ▲                                           │
        │ HTTP API                                  │ Presigned URLs
        │                                           ▼
┌───────┴───────┐                          ┌───────────────────┐
│    Workers    │ ◄────── Download/Upload ─┤   S3 (R2/MinIO)   │
│  (PDF, etc.)  │                          │   sources/        │
└───────────────┘                          │   outputs/        │
                                           └───────────────────┘
```

## Components

| Component | Description | Documentation |
|-----------|-------------|---------------|
| **Server** | Go server with SQLite, REST API, web dashboard | [server/README.md](server/README.md) |
| **PDF Worker** | Python worker for PDF→Markdown conversion | [workers/pdf/README.md](workers/pdf/README.md) |
| **CLI** | Python CLI for job submission | [cli/README.md](cli/README.md) |

## Quick Start

### 1. Start the Server

```bash
cd server

export BLOBFORGE_S3_ENDPOINT=https://xxx.r2.cloudflarestorage.com
export BLOBFORGE_S3_BUCKET=blobforge
export BLOBFORGE_S3_ACCESS_KEY=xxx
export BLOBFORGE_S3_SECRET_KEY=xxx

go build -o blobforge .
./blobforge
```

Dashboard at http://localhost:8080/

### 2. Create an API Token

1. Open the dashboard
2. Go to **Admin** → **API Tokens**
3. Click **Create Token**
4. Copy the token

### 3. Start a Worker

```bash
cd workers/pdf

pip install httpx marker-pdf

export BLOBFORGE_SERVER_URL=http://localhost:8080
export BLOBFORGE_API_TOKEN=bf_your_token_here

python worker.py
```

### 4. Submit a Job

```bash
cd server
go build -o blobforge ./cmd/blobforge

export BLOBFORGE_SERVER_URL=http://localhost:8080
export BLOBFORGE_API_TOKEN=bf_your_token_here

./blobforge submit /path/to/document.pdf
```

## Docker Compose

```bash
docker-compose up
```

See [docker-compose.yml](docker-compose.yml) for configuration.

## Priority System

| Priority | Name | Use Case |
|----------|------|----------|
| 1 | Critical | User-requested, immediate processing |
| 2 | High | Important batch processing |
| 3 | Normal | Default priority |
| 4 | Low | Background processing |
| 5 | Background | Lowest, processed when idle |

## Development

```bash
# Server (with hot reload)
cd server && go run github.com/air-verse/air@latest

# Tests
cd server && go test ./...

# PDF Worker
cd workers/pdf && python worker.py
```

## License

MIT
