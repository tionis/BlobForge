# BlobForge

**BlobForge** is a distributed, infrastructure-agnostic ingestion pipeline designed to process massive datasets (starting with RPG Rulebooks) into usable formats (Markdown/Assets).

It relies entirely on an **S3-compatible object store** for coordination, state management, and data storage. This "Serverless / No-Database" approach allows the system to run for years with near-zero maintenance, scale from 1 to 100 workers instantly, and survive complete shutdowns without state loss.

## 🚀 Key Features

*   **S3-Only Architecture:** No PostgreSQL, Redis, or RabbitMQ required. The bucket *is* the database.
*   **Git LFS Optimized:** "Materializes" PDFs from LFS pointers only when necessary, saving bandwidth and storage.
*   **Distributed Locking:** Uses S3 atomic operations (`If-None-Match`) to coordinate workers without race conditions.
*   **Priority Queues:** 5 levels: `critical`, `high`, `normal`, `low`, `background`.
*   **Manifest with Optimistic Locking:** Tracks file metadata (paths, tags, size) with `If-Match` for safe concurrent updates.
*   **Heartbeat Mechanism:** Workers send periodic heartbeats (60s), enabling fast stale detection (15 min vs 2 hours).
*   **Retry & Dead-Letter:** Failed jobs are retried up to 3 times, then moved to dead-letter queue for manual review.
*   **Resilient:** "Janitor" process recovers jobs from crashed workers automatically.
*   **Hash-Addressed:** Deduplication built-in. Processing is idempotent based on file content (SHA256).

## 🛠 Architecture

The system uses a directory structure within S3 to manage state:

```text
s3://my-bucket/
├── store/
│   ├── raw/           # Source blobs (PDFs) with metadata
│   └── out/           # Processed artifacts (Zips)
├── queue/
│   ├── todo/          # Pending jobs (with retry count)
│   │   ├── 1_critical/
│   │   ├── 2_high/
│   │   ├── 3_normal/
│   │   ├── 4_low/
│   │   └── 5_background/
│   ├── processing/    # Active locks (JSON with heartbeat)
│   ├── failed/        # Jobs pending retry
│   └── dead/          # Jobs that exceeded MAX_RETRIES
└── registry/
    └── manifest.json  # File metadata index (path→hash, tags, size)
```

### State Transitions

```
[Ingest] ──► todo ──► processing ──► done
              ▲            │
              │            ▼
              └──────── failed (janitor retries)
                           │
                           ▼ (after MAX_RETRIES)
                         dead ──► (manual retry)
```

## 📦 Installation

### Option A: Docker (Recommended for Workers)

The Docker image includes all dependencies for PDF processing (`marker-pdf`, `ocr`, `torch`).

```bash
# Build the image
docker build -t blobforge .

# Run a Worker
docker run -d \
  -e BLOBFORGE_S3_BUCKET=my-forge-bucket \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  blobforge worker
```

### Option B: uv (Recommended for CLI)

Install the CLI tool using [uv](https://docs.astral.sh/uv/):

```bash
# Install globally as a tool
uv tool install .

# Or install with PDF conversion support
uv tool install ".[convert]"

# Verify installation
blobforge --help
```

### Option C: pip

Requires Python 3.9+ and system dependencies for PDF conversion (`tesseract-ocr`, `ghostscript`).

```bash
pip install .

# With PDF conversion support
pip install ".[convert]"
```

## 💻 Usage (CLI)

BlobForge provides a unified `blobforge` command for all operations.

### 1. Ingest Data

Scans a local directory (or Git repo), calculates hashes, and queues new files.
The ingestor is **state-aware** - it checks all queues before adding jobs to prevent duplicates.

```bash
# Ingest a library with normal priority
blobforge ingest ./library/rpg-books

# Ingest urgent files
blobforge ingest ./library/hot-fixes --priority 1_critical

# Preview what would be ingested
blobforge ingest ./library --dry-run
```

### 2. Start a Worker

Workers automatically find jobs, lock them, process them, and upload results.
Worker IDs are persistent (based on machine fingerprint) so cleanup works across restarts.

```bash
# Start a worker (runs continuously)
blobforge worker

# Process one job and exit
blobforge worker --run-once

# Preview without making changes
blobforge worker --dry-run
```

*Run multiple instances on any number of machines to scale horizontally.*

### 3. Monitor Status

View queue counts, active processing jobs, and failed jobs.

```bash
# Quick dashboard
blobforge dashboard

# Detailed dashboard
blobforge dashboard -v

# Queue statistics
blobforge list -v

# Check specific file status
blobforge status <SHA256_HASH>
```

### 4. Maintenance (Janitor)

The Janitor detects stale locks (no heartbeat for 15+ minutes) and failed jobs, then re-queues them.
Jobs exceeding MAX_RETRIES are moved to the dead-letter queue.

```bash
# Run janitor
blobforge janitor

# Preview what janitor would do
blobforge janitor --dry-run

# Verbose output
blobforge janitor -v
```

### 5. Retry Failed Jobs

Manually retry jobs from the failed or dead-letter queue:

```bash
# Retry a failed job
blobforge retry <SHA256_HASH>

# Retry with higher priority
blobforge retry <SHA256_HASH> --priority 1_critical

# Reset retry counter (for dead-letter jobs)
blobforge retry <SHA256_HASH> --reset-retries
```

### 6. Search & Lookup (Manifest)

The manifest tracks all ingested files with metadata for fast lookups:

```bash
# Search by filename or tag
blobforge search "Call of Cthulhu"

# Look up by hash
blobforge lookup --hash <SHA256_HASH>

# Look up by path
blobforge lookup --path "DnD/Players Handbook.pdf"

# Show manifest statistics
blobforge manifest -v
```

### 7. Reprioritize Jobs

Change the priority of queued jobs:

```bash
blobforge reprioritize <SHA256_HASH> 1_critical
```

### 8. Manage Remote Configuration

Operational settings are stored in S3 and cached with a 1-hour TTL. This allows updating configuration without restarting workers.

```bash
# View current config
blobforge config --show

# Update settings
blobforge config --set max_retries=5 stale_timeout_minutes=20
```

### 9. List Workers

View all registered workers and their status:

```bash
# All workers
blobforge workers

# Only active workers
blobforge workers --active

# With detailed info
blobforge workers -v
```

## ⚙️ Configuration

Configuration is split into two categories:

### Local Configuration (Environment Variables)

These **must** be set as environment variables (required for S3 connectivity).

| Variable | Default | Description |
| :--- | :--- | :--- |
| `BLOBFORGE_S3_BUCKET` | `blobforge` | The target S3 bucket name |
| `BLOBFORGE_S3_PREFIX` | `pdf/` | Optional prefix for namespacing (e.g., `prod/`) |
| `BLOBFORGE_S3_REGION` | `us-east-1` | S3 region |
| `BLOBFORGE_S3_ACCESS_KEY_ID` | - | S3 access key (overrides AWS_ACCESS_KEY_ID) |
| `BLOBFORGE_S3_SECRET_ACCESS_KEY` | - | S3 secret key (overrides AWS_SECRET_ACCESS_KEY) |
| `BLOBFORGE_S3_ENDPOINT_URL` | - | For S3-compatible services (R2, MinIO, Ceph) |
| `BLOBFORGE_WORKER_ID` | *(auto)* | Worker identifier. Auto-generated from machine fingerprint if not set |
| `BLOBFORGE_LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

### Remote Configuration (Stored in S3)

These settings are stored in S3 at `{prefix}registry/config.json` and cached for 1 hour. Update via CLI:

| Setting | Default | Description |
| :--- | :--- | :--- |
| `max_retries` | `3` | Number of failures before moving to dead-letter queue |
| `heartbeat_interval` | `60` | Seconds between heartbeat updates |
| `stale_timeout_minutes` | `15` | Minutes without heartbeat before job is considered stale |
| `conversion_timeout` | `3600` | Seconds before conversion is killed (1 hour) |

```bash
# View all remote config
blobforge config --show

# Update settings (takes effect within 1 hour on all workers)
blobforge config --set max_retries=5 conversion_timeout=7200
```

### S3 Provider Compatibility

BlobForge requires S3 conditional writes (`If-None-Match` and `If-Match`). Tested providers:

| Provider | Status |
| :--- | :--- |
| AWS S3 | ✅ Full support |
| Cloudflare R2 | ✅ Full support |
| Ceph Object Gateway | ✅ Full support |
| MinIO | ✅ Full support |

## 🏗 Project Structure

```
├── pyproject.toml   # Package configuration and dependencies
├── blobforge/       # Main package
│   ├── cli.py       # Unified command-line interface
│   ├── ingestor.py  # Scans filesystem, uploads RAW blobs, queues jobs
│   ├── worker.py    # Polls S3, locks jobs, runs marker-pdf, sends heartbeats
│   ├── janitor.py   # Recovers stale jobs, manages retries
│   ├── status.py    # Reporting dashboard
│   ├── s3_client.py # Consolidated S3 operations (single source of truth)
│   └── config.py    # Shared configuration and constants
├── tests/           # Unit tests
├── DESIGN.md        # Detailed architectural decisions
└── Dockerfile       # Container build for workers
```

## 🧪 Testing

Run the test suite:

```bash
# With uv
uv run pytest tests/ -v

# Or with unittest
uv run python -m unittest tests.test_blobforge -v

# Without uv
python -m pytest tests/ -v
```

## 🔮 Future Roadmap

*   **Metrics/Monitoring:** Prometheus metrics export for job duration, success rate
*   **Batching:** Support for tarball ingestion to process thousands of small files efficiently
*   **Vector Embeddings:** Worker modules for generating embeddings from images/text
*   **SQLite + Litestream:** Optional fast manifest storage for filename→hash lookups

## 📄 License

MIT
