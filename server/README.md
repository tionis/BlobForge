# BlobForge Server

The BlobForge server is the central coordinator for the distributed job queue system. It provides:

- REST API for workers and clients
- Web dashboard with live updates (SSE)
- SQLite database with Litestream replication
- OIDC authentication (optional)
- Presigned URL generation for S3 file transfers

## Quick Start

```bash
# S3 configuration (required)
export BLOBFORGE_S3_ENDPOINT=https://xxx.r2.cloudflarestorage.com
export BLOBFORGE_S3_BUCKET=blobforge
export BLOBFORGE_S3_ACCESS_KEY=xxx
export BLOBFORGE_S3_SECRET_KEY=xxx

# Build and run
go build -o blobforge .
./blobforge
```

Dashboard available at http://localhost:8080/

## Configuration

### Required Environment Variables

| Variable | Description |
|----------|-------------|
| `BLOBFORGE_S3_ENDPOINT` | S3/R2/MinIO endpoint URL |
| `BLOBFORGE_S3_BUCKET` | S3 bucket name |
| `BLOBFORGE_S3_ACCESS_KEY` | S3 access key |
| `BLOBFORGE_S3_SECRET_KEY` | S3 secret key |

### Optional Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOBFORGE_PORT` | `8080` | HTTP server port |
| `BLOBFORGE_DB_PATH` | `./data/blobforge.db` | SQLite database path |
| `BLOBFORGE_S3_REGION` | `auto` | S3 region |
| `BLOBFORGE_S3_PREFIX` | - | Optional path prefix in bucket |
| `BLOBFORGE_WORKER_TIMEOUT` | `180` | Seconds before worker considered stale |
| `BLOBFORGE_MAX_ATTEMPTS` | `3` | Max job retry attempts |

### OIDC Authentication

To enable OIDC authentication:

| Variable | Description |
|----------|-------------|
| `BLOBFORGE_OIDC_ISSUER` | OIDC provider URL (e.g., `https://accounts.google.com`) |
| `BLOBFORGE_OIDC_CLIENT_ID` | OAuth2 client ID |
| `BLOBFORGE_OIDC_CLIENT_SECRET` | OAuth2 client secret |
| `BLOBFORGE_OIDC_REDIRECT_URL` | OAuth2 callback URL (e.g., `http://localhost:8080/auth/callback`) |
| `BLOBFORGE_OIDC_ALLOWED_GROUPS` | Comma-separated groups allowed access (empty = all) |
| `BLOBFORGE_OIDC_ADMIN_GROUPS` | Comma-separated groups with admin access |

### Litestream Replication

The server container includes [Litestream](https://litestream.io/) for continuous SQLite backup to S3.

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOBFORGE_LITESTREAM_DISABLED` | `false` | Set to `true` to disable Litestream |
| `BLOBFORGE_LITESTREAM_SKIP_RESTORE` | `false` | Set to `true` to skip restore on startup |
| `BLOBFORGE_LITESTREAM_AGE_SECRET_KEY` | - | Age secret key for encryption (optional) |
| `BLOBFORGE_LITESTREAM_AGE_PUBLIC_KEY` | - | Age public key for encryption (optional) |

Backups are stored in the same bucket under `litestream/` prefix.

## Docker

```bash
# Build
docker build -t blobforge-server -f Containerfile .

# Run
docker run -d \
  -p 8080:8080 \
  -e BLOBFORGE_S3_ENDPOINT=https://xxx.r2.cloudflarestorage.com \
  -e BLOBFORGE_S3_BUCKET=blobforge \
  -e BLOBFORGE_S3_ACCESS_KEY=xxx \
  -e BLOBFORGE_S3_SECRET_KEY=xxx \
  -v blobforge-data:/app/data \
  blobforge-server
```

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

### Admin

- `GET /api/admin/tokens` - List API tokens
- `POST /api/admin/tokens` - Create API token
- `DELETE /api/admin/tokens/{id}` - Delete API token
- `GET /api/admin/users` - List users

### Stats & Health

- `GET /api/stats` - Get queue statistics
- `GET /api/health` - Health check
- `GET /events` - SSE stream for live updates

## Development

```bash
# Run with hot reload
go run github.com/air-verse/air@latest

# Run tests
go test ./...

# Build
go build -o blobforge .
```

## CLI Tool

The server includes a CLI tool for administration:

```bash
# Build the CLI
go build -o blobforge ./cmd/blobforge

# Configure
export BLOBFORGE_SERVER_URL=http://localhost:8080
export BLOBFORGE_API_TOKEN=your-api-token

# Commands
./blobforge submit /path/to/document.pdf
./blobforge ingest /path/to/pdfs/ --type pdf
./blobforge stats
./blobforge jobs list
./blobforge workers list
./blobforge tokens list
```

See `./blobforge --help` for all commands.
