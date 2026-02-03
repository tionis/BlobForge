# BlobForge

**BlobForge** is a distributed job queue system with a web dashboard, designed for processing large files (PDFs, documents, etc.) across multiple workers. Features OIDC authentication, live SSE updates, and SQLite with Litestream replication.

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

## Quick Start

### 1. Start the Server

```bash
cd server

# S3 configuration (required)
export BLOBFORGE_S3_ENDPOINT=https://xxx.r2.cloudflarestorage.com
export BLOBFORGE_S3_BUCKET=blobforge
export BLOBFORGE_S3_ACCESS_KEY=xxx
export BLOBFORGE_S3_SECRET_KEY=xxx

# Optional: OIDC authentication
export BLOBFORGE_OIDC_ISSUER=https://accounts.google.com
export BLOBFORGE_OIDC_CLIENT_ID=your-client-id
export BLOBFORGE_OIDC_CLIENT_SECRET=your-client-secret
export BLOBFORGE_OIDC_REDIRECT_URL=http://localhost:8080/auth/callback
export BLOBFORGE_OIDC_ALLOWED_GROUPS=users,admins  # optional: restrict access
export BLOBFORGE_OIDC_ADMIN_GROUPS=admins          # optional: admin access

# Build and run
go build -o blobforge .
./blobforge
```

Dashboard at http://localhost:8080/

### 2. Start a PDF Worker (CPU)

```bash
cd workers/pdf

# Install dependencies
pip install -r requirements.txt

# Configure
export BLOBFORGE_SERVER_URL=http://localhost:8080
export BLOBFORGE_API_TOKEN=your-api-token  # from admin UI

# Run
python worker.py
```

### 3. Start a PDF Worker (GPU - CUDA)

```bash
cd workers/pdf

# Install with GPU support
pip install marker-pdf[gpu]

# Enable GPU processing
export BLOBFORGE_USE_GPU=true
export CUDA_VISIBLE_DEVICES=0  # specific GPU

python worker.py
```

### 4. Submit Jobs (Go CLI - Recommended)

```bash
cd server

# Build the CLI
go build -o blobforge ./cmd/blobforge

# Configure
export BLOBFORGE_SERVER_URL=http://localhost:8080
export BLOBFORGE_API_TOKEN=your-api-token

# Submit a PDF
./blobforge submit /path/to/document.pdf

# Submit with priority (1=critical, 5=background)
./blobforge submit /path/to/urgent.pdf --priority 1

# Ingest all PDFs in a directory (batch processing)
./blobforge ingest /path/to/pdfs/ --type pdf

# Check status
./blobforge stats
./blobforge jobs list
./blobforge workers list

# Manage jobs
./blobforge jobs retry 123
./blobforge jobs cancel 456

# Manage workers
./blobforge workers drain worker-1
./blobforge workers remove worker-1

# Manage API tokens (admin)
./blobforge tokens list
./blobforge tokens create --description "Worker token"
```

### 5. Submit Jobs (Python CLI - Alternative)

```bash
cd cli

# Install
pip install httpx

# Configure
export BLOBFORGE_SERVER_URL=http://localhost:8080
export BLOBFORGE_API_TOKEN=your-api-token

# Submit a PDF
python blobforge.py submit /path/to/document.pdf

# Check status
python blobforge.py stats
```

## Configuration

### Server Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOBFORGE_PORT` | `8080` | HTTP server port |
| `BLOBFORGE_DB_PATH` | `./data/blobforge.db` | SQLite database path |
| `BLOBFORGE_S3_ENDPOINT` | - | S3/R2/MinIO endpoint URL |
| `BLOBFORGE_S3_BUCKET` | `blobforge` | S3 bucket name |
| `BLOBFORGE_S3_REGION` | `auto` | S3 region |
| `BLOBFORGE_S3_ACCESS_KEY` | - | S3 access key |
| `BLOBFORGE_S3_SECRET_KEY` | - | S3 secret key |
| `BLOBFORGE_S3_PREFIX` | - | Optional path prefix in bucket |
| `BLOBFORGE_WORKER_TIMEOUT` | `180` | Seconds before worker considered stale |
| `BLOBFORGE_MAX_ATTEMPTS` | `3` | Max job retry attempts |

### OIDC Authentication Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOBFORGE_OIDC_ISSUER` | - | OIDC provider URL (e.g., `https://accounts.google.com`) |
| `BLOBFORGE_OIDC_CLIENT_ID` | - | OAuth2 client ID |
| `BLOBFORGE_OIDC_CLIENT_SECRET` | - | OAuth2 client secret |
| `BLOBFORGE_OIDC_REDIRECT_URL` | - | OAuth2 callback URL |
| `BLOBFORGE_OIDC_ALLOWED_GROUPS` | - | Comma-separated groups allowed to access (empty = all) |
| `BLOBFORGE_OIDC_ADMIN_GROUPS` | - | Comma-separated groups with admin access |

### Worker Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOBFORGE_SERVER_URL` | `http://localhost:8080` | Server URL |
| `BLOBFORGE_API_TOKEN` | - | API token for authentication |
| `BLOBFORGE_POLL_INTERVAL` | `5` | Seconds between job polls |
| `BLOBFORGE_HEARTBEAT_INTERVAL` | `30` | Seconds between heartbeats |
| `BLOBFORGE_USE_GPU` | `false` | Enable GPU acceleration (marker-pdf) |
| `CUDA_VISIBLE_DEVICES` | - | Which GPU to use |

## Authentication

BlobForge supports two authentication methods:

### OIDC (Browser)

Configure OIDC environment variables and users will be redirected to the identity provider. Supports group-based access control.

### API Tokens (Workers/CLI)

Generate tokens from the Admin UI (requires admin access):

1. Login via OIDC
2. Go to Admin → API Tokens
3. Create a new token with description
4. Copy the token (only shown once!)
5. Use in workers/CLI via `BLOBFORGE_API_TOKEN` or `Authorization: Bearer <token>`

## Litestream (SQLite Replication)

BlobForge can continuously backup SQLite to S3 using Litestream.

### With Docker

The Dockerfile includes Litestream. Configure via environment:

```bash
export LITESTREAM_ACCESS_KEY_ID=xxx
export LITESTREAM_SECRET_ACCESS_KEY=xxx
```

### Standalone Litestream

1. Install Litestream: https://litestream.io/install/
2. Configure `litestream.yml`:

```yaml
dbs:
  - path: /data/blobforge.db
    replicas:
      - type: s3
        bucket: ${BLOBFORGE_S3_BUCKET}
        path: db
        endpoint: ${BLOBFORGE_S3_ENDPOINT}
```

3. Run with Litestream:

```bash
litestream replicate -config litestream.yml
```

## Priority System

Jobs have a priority from 1-5:

| Priority | Name | Use Case |
|----------|------|----------|
| 1 | Critical | User-requested, needs immediate processing |
| 2 | High | Important batch processing |
| 3 | Normal | Default priority |
| 4 | Low | Background processing |
| 5 | Background | Lowest priority, processed when idle |

Workers claim jobs in priority order (lowest number first), then by creation time.

## API Endpoints

### Authentication

- `GET /auth/login` - Initiate OIDC login
- `GET /auth/callback` - OIDC callback
- `POST /auth/logout` - Logout

### Workers

- `POST /api/workers/register` - Register a worker
- `POST /api/workers/{id}/heartbeat` - Send heartbeat
- `POST /api/workers/{id}/unregister` - Unregister worker
- `POST /api/workers/{id}/drain` - Set worker to draining (admin)
- `DELETE /api/workers/{id}` - Remove worker (admin)
- `GET /api/workers` - List all workers

### Jobs

- `POST /api/jobs` - Create a new job
- `GET /api/jobs` - List jobs (with filters)
- `GET /api/jobs/{id}` - Get job details
- `POST /api/jobs/claim` - Claim next available job
- `POST /api/jobs/{id}/complete` - Mark job complete
- `POST /api/jobs/{id}/fail` - Mark job failed
- `POST /api/jobs/{id}/retry` - Retry a failed job
- `POST /api/jobs/{id}/cancel` - Cancel a pending job
- `PATCH /api/jobs/{id}/priority` - Update job priority

### Files

- `GET /api/files/{job_id}/{type}/url` - Get presigned download URL
- `POST /api/files/upload-url` - Get presigned upload URL

### Admin (requires admin access)

- `GET /api/admin/tokens` - List API tokens
- `POST /api/admin/tokens` - Create API token
- `DELETE /api/admin/tokens/{id}` - Delete API token
- `GET /api/admin/users` - List users

### Stats & Health

- `GET /api/stats` - Get queue statistics
- `GET /api/health` - Health check
- `GET /api/events` - SSE stream for live updates

## Live Dashboard Updates

The dashboard uses Server-Sent Events (SSE) for real-time updates:

- Job created/updated/completed/failed
- Worker online/offline
- Stats updates

No page refresh needed - changes appear instantly.

## GPU Acceleration

PDF workers using marker-pdf support GPU acceleration for faster processing:

```bash
# Install with GPU support
pip install marker-pdf[gpu]

# Configure NVIDIA runtime
export BLOBFORGE_USE_GPU=true
export CUDA_VISIBLE_DEVICES=0
```

For Docker, use the NVIDIA runtime:

```bash
docker run --gpus all -e BLOBFORGE_USE_GPU=true blobforge-pdf-worker
```

## Docker Deployment

```bash
# Build images
docker build -t blobforge-server -f server/Containerfile server/
docker build -t blobforge-pdf-worker -f workers/pdf/Containerfile workers/pdf/

# Run with docker-compose
docker-compose up
```

## Development

```bash
# Server (with hot reload)
cd server
go run github.com/air-verse/air@latest

# Run tests
cd server
go test ./...

# PDF Worker
cd workers/pdf
python worker.py
```

## License

MIT
