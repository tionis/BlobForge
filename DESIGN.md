# BlobForge - Design Document

## 1. Overview

BlobForge is a distributed job processing system designed initially for PDF-to-Markdown conversion, but architected to support multiple worker types (image processing, embedding generation, etc.).

### 1.1. Architecture Principles

- **Centralized Coordinator**: Single Go server manages all job state via SQLite
- **Dumb Workers**: Workers are stateless HTTP clients that poll for jobs
- **S3 for Files Only**: Object storage for source/output files, not coordination
- **Resilient by Default**: SQLite + Litestream for automatic backup/restore
- **Observable**: HTMX dashboard for real-time visibility

### 1.2. Why This Architecture?

Previous S3-only coordination had issues:
- Polling inefficiency (constant API calls)
- Complex distributed locking
- No ACID transactions
- Hard to debug without centralized view

The new architecture provides:
- **Efficient job dispatch** via long-polling
- **ACID transactions** for job state
- **Real-time dashboard** for monitoring
- **Automatic recovery** via Litestream
- **Lower S3 costs** (files only, no coordination)

## 2. System Components

```
┌─────────────────────────────────────────────────────────────────┐
│                      BlobForge Server (Go)                      │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────┐  │
│  │  Dashboard   │  │  Worker API  │  │     Litestream        │  │
│  │  (HTMX)      │  │  (REST)      │  │  (SQLite → S3)        │  │
│  └──────────────┘  └──────────────┘  └───────────────────────┘  │
│           │                │                    │               │
│           └────────────────┼────────────────────┘               │
│                            ▼                                    │
│                    ┌──────────────┐                             │
│                    │   SQLite     │                             │
│                    │  (jobs, workers, files)                    │
│                    └──────────────┘                             │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
   ┌───────────┐        ┌───────────┐       ┌───────────┐
   │  Worker   │        │  Worker   │       │  Worker   │
   │  (PDF)    │        │  (PDF)    │       │ (future)  │
   └───────────┘        └───────────┘       └───────────┘
         │                    │
         └──────────────┐     |
                        ▼     ▼
                    ┌─────────────────────────────────┐
                    │         S3 Bucket               │
                    │  (source files, output files)   │
                    └─────────────────────────────────┘
```

### 2.1. Server (Go)

Single binary containing:
- **HTTP Server**: Chi router for API and dashboard
- **SQLite Database**: Job queue, worker registry, file metadata
- **Litestream**: Embedded for continuous SQLite backup to S3
- **S3 Client**: Presigned URL generation for file transfers

### 2.2. Workers (Python, etc.)

Stateless processes that:
- Register with the server on startup
- Long-poll for jobs matching their type
- Download source files via presigned URLs
- Process files (PDF → Markdown, etc.)
- Upload results via presigned URLs
- Report completion/failure

### 2.3. S3 Bucket

Simple file storage:
```
s3://blobforge/
├── sources/
│   └── {hash}.pdf          # Original uploaded files
├── outputs/
│   └── {hash}.zip          # Processed results
└── db/
    └── blobforge.db-*      # Litestream backups
```

## 3. Database Schema

### 3.1. Workers Table

```sql
CREATE TABLE workers (
    id TEXT PRIMARY KEY,           -- UUID or human-friendly name
    type TEXT NOT NULL,            -- 'pdf', 'image', etc.
    status TEXT NOT NULL,          -- 'online', 'offline', 'draining'
    last_heartbeat DATETIME,
    current_job_id INTEGER,        -- FK to jobs (nullable)
    metadata JSON,                 -- capabilities, version, etc.
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### 3.2. Jobs Table

```sql
CREATE TABLE jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,            -- 'pdf', 'image', etc.
    status TEXT NOT NULL,          -- 'pending', 'running', 'completed', 'failed', 'dead'
    priority INTEGER DEFAULT 3,    -- 1=critical, 5=background
    
    -- Source file info
    source_hash TEXT NOT NULL,     -- SHA256 of source file
    source_path TEXT,              -- Original filename/path
    source_size INTEGER,
    
    -- Processing state
    worker_id TEXT,                -- FK to workers (nullable)
    started_at DATETIME,
    completed_at DATETIME,
    
    -- Retry tracking
    attempts INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,
    last_error TEXT,
    
    -- Metadata
    tags JSON,                     -- searchable tags
    metadata JSON,                 -- arbitrary job metadata
    result JSON,                   -- output metadata after completion
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(type, source_hash)      -- prevent duplicate jobs
);

CREATE INDEX idx_jobs_status_priority ON jobs(status, priority);
CREATE INDEX idx_jobs_source_hash ON jobs(source_hash);
CREATE INDEX idx_jobs_worker ON jobs(worker_id);
```

### 3.3. Files Table

```sql
CREATE TABLE files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,       -- FK to jobs
    type TEXT NOT NULL,            -- 'source', 'output'
    s3_key TEXT NOT NULL,          -- path in S3 bucket
    size INTEGER,
    content_type TEXT,
    checksum TEXT,                 -- SHA256
    metadata JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (job_id) REFERENCES jobs(id)
);

CREATE INDEX idx_files_job ON files(job_id);
CREATE INDEX idx_files_s3_key ON files(s3_key);
```

## 4. API Design

### 4.1. Worker API

#### Register Worker
```
POST /api/workers
{
    "id": "worker-01",           // optional, server generates if omitted
    "type": "pdf",
    "metadata": {"version": "1.0", "capabilities": ["ocr"]}
}
→ 201 {"id": "worker-01", "heartbeat_interval": 60}
```

#### Heartbeat
```
POST /api/workers/{id}/heartbeat
{
    "current_job_id": 123,       // optional
    "progress": {"stage": "converting", "percent": 45}
}
→ 200 {"status": "ok"}
```

#### Claim Job (Long-Poll)
```
GET /api/jobs/claim?type=pdf&timeout=30
→ 200 {
    "job": {
        "id": 123,
        "source_hash": "abc123...",
        "source_path": "DnD/PHB.pdf",
        "metadata": {...}
    },
    "download_url": "https://s3.../sources/abc123.pdf?signature=...",
    "upload_url": "https://s3.../outputs/abc123.zip?signature=..."
}
→ 204 (no jobs available after timeout)
```

#### Complete Job
```
POST /api/jobs/{id}/complete
{
    "worker_id": "worker-01",
    "result": {
        "pages": 320,
        "images": 45,
        "output_size": 15234567
    }
}
→ 200 {"status": "completed"}
```

#### Fail Job
```
POST /api/jobs/{id}/fail
{
    "worker_id": "worker-01",
    "error": "Marker conversion failed: out of memory"
}
→ 200 {"status": "failed", "attempts": 2, "will_retry": true}
```

### 4.2. Management API

#### List Jobs
```
GET /api/jobs?status=pending&type=pdf&limit=50
→ 200 {"jobs": [...], "total": 1234}
```

#### Create Job (Ingest)
```
POST /api/jobs
{
    "type": "pdf",
    "source_hash": "abc123...",
    "source_path": "DnD/PHB.pdf",
    "source_size": 52428800,
    "priority": 2,
    "tags": ["dnd", "5e"]
}
→ 201 {"job_id": 123, "upload_url": "https://s3.../sources/abc123.pdf?..."}
→ 409 {"error": "job already exists", "job_id": 45}
```

#### Retry Job
```
POST /api/jobs/{id}/retry
{
    "reset_attempts": true,    // optional
    "priority": 1              // optional
}
→ 200 {"status": "pending"}
```

#### Cancel Job
```
DELETE /api/jobs/{id}
→ 200 {"status": "cancelled"}
```

### 4.3. File API

#### Get Download URL
```
GET /api/files/{job_id}/source
→ 200 {"url": "https://s3.../...", "expires_in": 3600}
```

#### Get Output URL
```
GET /api/files/{job_id}/output
→ 200 {"url": "https://s3.../...", "expires_in": 3600}
```

## 5. Dashboard (HTMX)

### 5.1. Pages

- **Dashboard** (`/`): Overview with job stats, active workers, recent activity
- **Jobs** (`/jobs`): Filterable job list with actions (retry, cancel, view)
- **Workers** (`/workers`): Worker list with status, current job, actions
- **Files** (`/files`): Browse source/output files by job
- **Job Detail** (`/jobs/{id}`): Full job info, logs, files, retry history

### 5.2. Real-time Updates

- HTMX polling (`hx-trigger="every 5s"`) for dashboard stats
- Server-Sent Events (optional) for live job updates
- Optimistic UI updates for actions

## 6. Worker Protocol

### 6.1. Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│                      Worker Lifecycle                        │
└─────────────────────────────────────────────────────────────┘

    ┌──────────┐
    │  Start   │
    └────┬─────┘
         │
         ▼
    ┌──────────┐     POST /api/workers
    │ Register │────────────────────────► Server
    └────┬─────┘
         │
         ▼
    ┌──────────┐     GET /api/jobs/claim?type=pdf&timeout=30
    │  Poll    │◄───────────────────────────────────────────┐
    │  (idle)  │                                            │
    └────┬─────┘                                            │
         │ job received                                     │
         ▼                                                  │
    ┌──────────┐     Download source via presigned URL      │
    │ Download │                                            │
    └────┬─────┘                                            │
         │                                                  │
         ▼                                                  │
    ┌──────────┐     POST /api/workers/{id}/heartbeat       │
    │ Process  │     (every 60s with progress)              │
    │          │                                            │
    └────┬─────┘                                            │
         │                                                  │
    ┌────┴────┐                                             │
    ▼         ▼                                             │
┌───────┐ ┌───────┐                                         │
│Success│ │ Fail  │                                         │
└───┬───┘ └───┬───┘                                         │
    │         │                                             │
    ▼         ▼                                             │
┌───────┐ ┌───────┐                                         │
│Upload │ │ POST  │                                         │
│output │ │ /fail │                                         │
└───┬───┘ └───┬───┘                                         │
    │         │                                             │
    ▼         │                                             │
┌────────┐    │                                             │
│ POST   │    │                                             │
│/complete│   │                                             │
└───┬────┘    │                                             │
    │         │                                             │
    └─────────┴─────────────────────────────────────────────┘
```

### 6.2. Heartbeat Requirements

- Workers MUST send heartbeat every 60 seconds while processing
- Server marks workers as `offline` after 3 missed heartbeats (180s)
- Jobs from offline workers are automatically returned to queue
- Heartbeat includes optional progress info for dashboard display

### 6.3. Graceful Shutdown

1. Worker receives SIGTERM/SIGINT
2. Stop accepting new jobs (drain mode)
3. Complete current job (or release it)
4. POST to deregister endpoint
5. Exit

## 7. Litestream Integration

### 7.1. Backup Strategy

- Continuous replication to S3: `s3://bucket/db/`
- WAL streaming for minimal data loss
- Automatic restore on server startup if local DB missing

### 7.2. Configuration

```yaml
# litestream.yml (embedded in binary or external)
dbs:
  - path: /data/blobforge.db
    replicas:
      - type: s3
        bucket: blobforge
        path: db
        endpoint: ${S3_ENDPOINT}
        access-key-id: ${S3_ACCESS_KEY}
        secret-access-key: ${S3_SECRET_KEY}
```

### 7.3. Disaster Recovery

1. Server crashes
2. New server starts
3. Litestream restores latest snapshot + WAL from S3
4. Server resumes with <1 minute of potential data loss
5. Workers reconnect and resume polling

## 8. Configuration

### 8.1. Server Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOBFORGE_PORT` | `8080` | HTTP server port |
| `BLOBFORGE_DB_PATH` | `./data/blobforge.db` | SQLite database path |
| `BLOBFORGE_S3_BUCKET` | - | S3 bucket name |
| `BLOBFORGE_S3_ENDPOINT` | - | S3 endpoint (for R2, MinIO) |
| `BLOBFORGE_S3_ACCESS_KEY` | - | S3 access key |
| `BLOBFORGE_S3_SECRET_KEY` | - | S3 secret key |
| `BLOBFORGE_S3_REGION` | `auto` | S3 region |
| `BLOBFORGE_WORKER_TIMEOUT` | `180` | Seconds before worker considered offline |
| `BLOBFORGE_MAX_ATTEMPTS` | `3` | Default max job attempts |
| `BLOBFORGE_LOG_LEVEL` | `info` | Logging level |

### 8.2. OIDC Authentication Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOBFORGE_OIDC_ISSUER` | - | OIDC provider URL |
| `BLOBFORGE_OIDC_CLIENT_ID` | - | OAuth2 client ID |
| `BLOBFORGE_OIDC_CLIENT_SECRET` | - | OAuth2 client secret |
| `BLOBFORGE_OIDC_REDIRECT_URL` | - | OAuth2 callback URL |
| `BLOBFORGE_OIDC_ALLOWED_GROUPS` | - | Groups allowed access (comma-separated) |
| `BLOBFORGE_OIDC_ADMIN_GROUPS` | - | Groups with admin access |

## 9. Authentication & Authorization

### 9.1. Authentication Methods

BlobForge supports two authentication methods:

1. **OIDC (Browser)**: For dashboard access, users authenticate via OIDC provider
2. **API Tokens**: For workers and CLI, hashed tokens stored in database

### 9.2. OIDC Flow

```
┌────────┐     1. GET /auth/login      ┌─────────┐     2. Redirect     ┌──────────┐
│ User   │──────────────────────────►  │ Server  │ ─────────────────► │   OIDC   │
└────────┘                             └─────────┘                     │ Provider │
    │                                       ▲                          └──────────┘
    │                                       │                               │
    │     5. Set session cookie            │ 4. Verify + get user info     │
    │ ◄─────────────────────────────────────                               │
    │                                                                       │
    └────────────────── 3. Callback with code ──────────────────────────────┘
```

### 9.3. Group-Based Access Control

- `BLOBFORGE_OIDC_ALLOWED_GROUPS`: If set, only users in these groups can access
- `BLOBFORGE_OIDC_ADMIN_GROUPS`: Users in these groups get admin privileges
- Groups are extracted from the `groups` claim in the ID token

### 9.4. API Token Authentication

Workers and CLI use Bearer tokens:

```bash
curl -H "Authorization: Bearer bf_xxxxxxxxxxxxxxxxxxxx" \
     http://localhost:8080/api/jobs
```

Token format: `bf_` prefix + 32 random hex characters. Tokens are hashed (SHA-256) before storage.

### 9.5. Worker Authentication

Workers use dedicated per-worker secrets for authentication. This provides:

- **Individual revocation**: Disable a compromised worker without affecting others
- **Audit trail**: Know which worker performed each action
- **No credential sharing**: Each worker has unique credentials

#### Worker Credential Flow

```
┌──────────┐     1. Admin creates worker      ┌──────────┐
│  Admin   │ ────────────────────────────────►│  Server  │
└──────────┘                                  └──────────┘
     │                                             │
     │  2. Server returns one-time secret          │
     │◄────────────────────────────────────────────│
     │    bfw_xxxxxxxxxxxxxxxxxxxxxxxxxx           │
     │                                             │
     ▼                                             │
┌──────────┐     3. Configure worker env      ┌──────────┐
│  Worker  │ ────────────────────────────────►│  Server  │
│  Process │  X-Worker-ID: my-worker          └──────────┘
└──────────┘  X-Worker-Secret: bfw_xxxxx
```

#### Worker Environment Variables

```bash
export BLOBFORGE_SERVER_URL=http://localhost:8080
export BLOBFORGE_WORKER_ID=pdf-worker-01
export BLOBFORGE_WORKER_SECRET=bfw_xxxxxxxxxxxxxxxxxxxxxxxx
```

#### Admin API for Worker Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/admin/workers` | POST | Create worker, returns one-time secret |
| `/api/admin/workers/{id}/regenerate` | POST | Generate new secret, invalidates old |
| `/api/admin/workers/{id}/enable` | POST | Re-enable a disabled worker |
| `/api/admin/workers/{id}/disable` | POST | Disable worker (revoke access) |

#### Worker Database Schema

```sql
ALTER TABLE workers ADD COLUMN secret_hash TEXT;  -- SHA-256 of secret
ALTER TABLE workers ADD COLUMN enabled BOOLEAN DEFAULT TRUE;
```

### 9.6. Database Schema for Auth

```sql
-- Users (populated via OIDC)
CREATE TABLE users (
    id TEXT PRIMARY KEY,      -- OIDC subject
    email TEXT UNIQUE,
    name TEXT,
    picture TEXT,
    groups TEXT,              -- JSON array of groups
    is_admin INTEGER DEFAULT 0,
    provider TEXT,            -- 'oidc', etc.
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Sessions (for web dashboard)
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,      -- session token
    user_id TEXT NOT NULL,
    expires_at DATETIME NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- API Tokens (for workers/CLI)
CREATE TABLE api_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    token_hash TEXT UNIQUE NOT NULL,  -- SHA-256 hash
    description TEXT,
    last_used_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
```

## 10. Live Dashboard Updates (SSE)

### 10.1. Server-Sent Events

The dashboard receives real-time updates via SSE at `GET /api/events`:

```javascript
const eventSource = new EventSource('/api/events');

eventSource.addEventListener('job_created', (e) => {
    // Add new job row to table
});

eventSource.addEventListener('job_updated', (e) => {
    // Update job row in place
});

eventSource.addEventListener('worker_online', (e) => {
    // Update worker status
});

eventSource.addEventListener('stats_updated', (e) => {
    // Refresh stats counters
});
```

### 10.2. Event Types

| Event | Payload | Description |
|-------|---------|-------------|
| `job_created` | Job JSON | New job added to queue |
| `job_updated` | Job JSON | Job status/progress changed |
| `job_completed` | Job JSON | Job finished successfully |
| `job_failed` | Job JSON | Job failed (may retry) |
| `worker_online` | Worker JSON | Worker registered/reconnected |
| `worker_offline` | Worker JSON | Worker went offline |
| `stats_updated` | Stats JSON | Queue statistics changed |

### 10.3. Hub Architecture

```
┌──────────────────────────────────────────────────────────┐
│                      SSE Hub                              │
│  ┌────────────────────────────────────────────────────┐  │
│  │  clients: map[chan Event]*Client                   │  │
│  │  broadcast: chan Event                             │  │
│  │  register: chan *Client                            │  │
│  │  unregister: chan *Client                          │  │
│  └────────────────────────────────────────────────────┘  │
│            │               │               │              │
│            ▼               ▼               ▼              │
│      ┌─────────┐     ┌─────────┐     ┌─────────┐         │
│      │ Client  │     │ Client  │     │ Client  │  ...    │
│      │ (conn)  │     │ (conn)  │     │ (conn)  │         │
│      └─────────┘     └─────────┘     └─────────┘         │
└──────────────────────────────────────────────────────────┘
          │                  │                │
          ▼                  ▼                ▼
       Browser           Browser          Browser
```

## 11. Priority System

Jobs have a priority from 1-5 (lower = higher priority):

| Priority | Name | Use Case |
|----------|------|----------|
| 1 | Critical | User-requested, immediate processing |
| 2 | High | Important batch processing |
| 3 | Normal | Default priority |
| 4 | Low | Background processing |
| 5 | Background | Lowest, processed when idle |

Workers claim jobs ordered by: `ORDER BY priority ASC, created_at ASC`

Priority can be changed via:
- API: `PATCH /api/jobs/{id}/priority {"priority": 1}`
- Dashboard: Edit job priority inline

## 12. Deployment

### 12.1. Server

```bash
# Single binary deployment
./blobforge-server

# Or with Docker
docker run -d \
  -p 8080:8080 \
  -v /data:/data \
  -e BLOBFORGE_S3_BUCKET=my-bucket \
  -e BLOBFORGE_S3_ACCESS_KEY=... \
  -e BLOBFORGE_S3_SECRET_KEY=... \
  ghcr.io/tionis/blobforge-server
```

### 12.2. Workers

```bash
# PDF Worker (Python)
docker run -d \
  -v worker-cache:/root/.cache \
  -e BLOBFORGE_SERVER_URL=http://server:8080 \
  -e BLOBFORGE_WORKER_ID=pdf-worker-01 \
  ghcr.io/tionis/blobforge-worker-pdf
```

## 13. Monorepo Structure

```
blobforge/
├── server/                    # Go server
│   ├── main.go
│   ├── api/                   # HTTP handlers
│   ├── auth/                  # OIDC + API token auth
│   ├── db/                    # SQLite operations
│   ├── s3/                    # S3 client
│   ├── sse/                   # Server-Sent Events hub
│   └── web/                   # HTMX templates
├── workers/
│   └── pdf/                   # Python PDF worker
│       ├── worker.py
│       ├── requirements.txt
│       └── Containerfile
├── cli/                       # Python CLI tool
│   └── blobforge.py
├── docs/
│   └── WORK_LOG.md
├── DESIGN.md
├── README.md
└── docker-compose.yml         # Full stack for development
```

## 14. Future Extensions

- **Worker Types**: Image processing, embedding generation, OCR-only
- **Webhooks**: Notify external systems on job completion
- **Multi-tenancy**: Namespace jobs by user/project
- **Metrics**: Prometheus endpoint for monitoring
- **Distributed Server**: Multiple server instances with shared DB (via Litestream)
