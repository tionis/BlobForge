# BlobForge

**BlobForge** is a distributed, infrastructure-agnostic ingestion pipeline designed to process massive datasets (starting with RPG Rulebooks) into usable formats (Markdown/Assets).

It uses **Bunny Database** for persistent coordination, a **Bunny Edge Script**
for the API and management UI, and an **S3-compatible object store** for raw PDFs and completed artifacts. The queue
can remain idle for months without losing leases, retries, metadata, or worker
history.

## 🚀 Key Features

*   **Managed Coordination:** Bunny Database and Edge Scripting replace PostgreSQL, Redis, and message-broker infrastructure.
*   **Management UI:** IndieAuth-protected queue, worker enrollment/revocation, retry, priority, cancellation, and runtime configuration controls.
*   **Portable Backups:** Admin-triggered, transaction-consistent Bunny Database exports are stored privately in S3 with checksums.
*   **Web Library:** Upload PDFs, search paths/tags/sources, filter and page through every job state, download source/result files, and safely render completed GFM Markdown with its assets and a persistent document ToC.
*   **Least-Privilege Workers:** Per-worker tokens and lease-bound presigned transfers remove bucket credentials from conversion hosts.
*   **Git LFS Optimized:** "Materializes" PDFs from LFS pointers only when necessary, saving bandwidth and storage.
*   **Fenced Leases:** Atomic SQLite statements assign expiring, opaque lease tokens and recover abandoned work on the next claim or management request.
*   **Priority Queues:** 5 levels: `critical`, `high`, `normal`, `low`, `background`.
*   **Persistent Metadata:** Bunny Database tracks paths, tags, size, source, and content hash.
*   **Heartbeat Mechanism:** Workers send periodic heartbeats (60s), enabling fast stale detection (15 min vs 2 hours).
*   **Retry & Dead-Letter:** Failed jobs are retried up to 3 times, then moved to dead-letter queue for manual review.
*   **Resilient:** Expired leases are recovered automatically without a continuously running janitor.
*   **Graceful Shutdown:** Catchable worker signals requeue active jobs immediately before exit.
*   **Conversion Timeout:** Long conversions honor `conversion_timeout` with hard timeout support on compatible platforms.
*   **Hash-Addressed:** Deduplication built-in. Processing is idempotent based on file content (SHA256).

## 🛠 Architecture

The current architecture and deployment/cutover guide are documented in
[docs/bunny_coordination_backend.md](docs/bunny_coordination_backend.md).

Set `BLOBFORGE_COORDINATOR_URL` and `BLOBFORGE_COORDINATOR_TOKEN` to use it.
Trusted ingestors use the deployment's client token. Conversion workers use a
per-worker token created in the management UI.

Worker labels become stable IDs by slugging them (`GPU Workstation` becomes
`gpu-workstation`). Duplicate or slug-colliding labels are rejected. A worker
token belongs to that single enrollment and should not be reused to represent
multiple machines.
The object store contains only immutable inputs, outputs, and portable
coordinator backups:

```text
s3://my-bucket/
├── store/raw/                    # Source PDFs
├── store/out/                    # Processed ZIP artifacts
└── backups/coordinator/          # Versioned JSON database exports
```

### State Transitions

```
[Ingest] ──► todo ──► processing ──► done
              ▲            │
              │            ▼
              └──────── failed (next claim retries)
                           │
                           ▼ (after MAX_RETRIES)
                         dead ──► (Web UI retry)
```

## 📦 Installation

### Option A: Docker/Podman (Recommended for Workers)

The container image includes all dependencies for PDF processing (`marker-pdf`, `ocr`, `torch`).

```bash
# Build the image (or pull from ghcr.io/tionis/blobforge:latest)
podman build -t blobforge .

# Create the worker in the management UI, then run its displayed command.
podman run -d \
  -v blobforge-cache:/root/.cache \
  ghcr.io/tionis/blobforge:latest \
  blobforge worker --coordinator-url https://blobforge.example --token bfw_...
```

**Important notes:**
- **Model Cache:** Mount `/root/.cache` as a volume to persist the ~3GB marker/torch models across container restarts. Without this, models are re-downloaded on every container start.
- **Worker Identity:** Coordinator workers derive their stable identity from the UI-issued token.

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

Requires Python 3.10+ and system dependencies for PDF conversion (`tesseract-ocr`, `ghostscript`). Python 3.9 is no longer supported because the compatible legacy Marker dependency branch retains known-vulnerable packages.

```bash
pip install .

# With PDF conversion support
pip install ".[convert]"
```

## 💻 Usage (CLI)

BlobForge provides a unified `blobforge` command for all operations.

### 1. Ingest Data

Ingests **PDF files only** (`.pdf` extension) and queues them for processing. Accepts files, directories, or shell globs.

**How it works:**
1. For directories: walks the tree recursively looking for `.pdf` files (case-insensitive)
2. For files: checks if they're PDFs (by extension)
3. For each PDF, determines the file hash:
   - **Git LFS pointer files**: Extracts the SHA256 from the pointer (no download needed)
   - **Regular PDF files**: Validates the `%PDF` header, then computes SHA256
4. Checks if the file is already processed, queued, or failed (prevents duplicates)
5. Uploads the raw PDF to S3 (if not already present)
6. Creates a job in the todo queue

**Git LFS Support:**
- If the path is inside a Git repo with LFS, pointer files are detected automatically
- The ingestor will `git lfs pull` individual files as needed, then revert them to pointers after upload
- This saves local disk space when processing large libraries
- Works with **smudge filter disabled** (`git lfs install --skip-smudge`)

**Input flexibility:**
- Single file: `blobforge ingest document.pdf`
- Single directory: `blobforge ingest ./library/`
- Multiple paths: `blobforge ingest file1.pdf file2.pdf ./more-pdfs/`
- Shell globbing: `blobforge ingest *.pdf ./books/*.pdf`
- Mix files and directories: `blobforge ingest urgent.pdf ./batch-folder/`

```bash
# Ingest a single PDF
blobforge ingest ./document.pdf

# Ingest a directory recursively
blobforge ingest ./library/rpg-books

# Ingest multiple paths (files and/or directories)
blobforge ingest file1.pdf file2.pdf ./more-pdfs/

# Use shell globbing
blobforge ingest *.pdf ./books/**/*.pdf

# Ingest with high priority
blobforge ingest ./urgent/*.pdf --priority 1_critical

# Preview what would be ingested (no changes made)
blobforge ingest ./library --dry-run
```

**State-aware behavior** - Files are skipped if they're:
- Already converted (output exists)
- Currently being processed by a worker
- Already in the todo queue (any priority)
- In the failed queue (janitor will retry)
- In the dead-letter queue (exceeded max retries)

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

# Only acquire work during local nighttime hours
blobforge worker --run-window 22:00-06:00

# Requeue active conversion when the window closes
blobforge worker --run-window 22:00-06:00 --abort-outside-window

# Run marker in a child process so native crashes do not kill the worker
blobforge worker --isolate-conversion
```

*Run multiple instances on any number of machines to scale horizontally.*

When using the Bunny coordinator, the management console shows live macro-stage
progress and any Marker/tqdm counters reported by the worker. Stage changes are
published promptly rather than waiting for the full heartbeat interval. Failed
and dead jobs have a **Failures** action with per-attempt worker, stage,
exception type, diagnostics, and traceback; this history remains available
after a retry.

**Run window behavior**
- `--run-window HH:MM-HH:MM` uses the worker machine's local time.
- The option may be repeated or comma-separated, for example `--run-window 06:00-08:00,22:00-23:30`.
- Outside configured windows, workers stay idle and do not acquire new jobs.
- Outside-window idle sleep lasts until the next configured opening window; workers do not poll every few seconds while waiting.
- By default, active jobs finish even if the window closes.
- With `--abort-outside-window`, active conversion is interrupted at the window boundary, requeued, and unlocked.
- `--abort-outside-window` automatically enables isolated conversion so the parent worker can kill the conversion child at the boundary.
- `--isolate-conversion` can also be used by itself to contain native marker/PyTorch crashes; it reloads marker models per job.

**Graceful shutdown behavior**
- On `SIGINT`/`SIGTERM` (and platform-available `SIGHUP`/`SIGQUIT`), workers perform graceful shutdown.
- If a job is active, the worker requeues it immediately and releases the processing lock before deregistration.
- This avoids waiting for stale-lock timeout in normal restart/deploy workflows.
- Uncatchable termination (`SIGKILL`, hard OOM kill) still relies on startup cleanup + janitor stale recovery.

### 3. Monitor Status

View queue counts, active processing jobs, and failed jobs.

```bash
# Quick dashboard
blobforge dashboard

# Detailed dashboard
blobforge dashboard -v

# A worker token can query the coordinator without S3 credentials
blobforge dashboard --coordinator-url https://blobforge.example --token bfw_...

# Queue statistics
blobforge list -v

# Check specific file status
blobforge status <SHA256_HASH>
```

### 4. Manage jobs and configuration

Use the IndieAuth management UI to retry or reprioritize jobs, recover expired
leases, revoke workers, and edit runtime configuration.

Operational settings are stored in Bunny Database. Update them in the Web UI
without restarting workers.

```bash
# View current config
blobforge config --show

```

### 5. List Workers

View all registered workers and their status:

```bash
# All workers
blobforge workers

# Only active workers
blobforge workers --active

# With detailed info
blobforge workers -v
```

### 6. Offline Conversion

Convert a PDF locally without using the distributed queue. Useful for testing or single-file processing.

```bash
# Convert a file (outputs to ./filename/)
blobforge convert document.pdf

# Convert to specific directory
blobforge convert document.pdf --output ./results/
```

### 7. Hydrate Existing PDFs

Materialize converted markdown/assets next to local PDFs by matching on file hash.
This is useful when conversions already exist in BlobForge and you want local `.md` files.

Outputs per PDF:
- `<stem>.md`
- `<stem>.assets/`

```bash
# Hydrate all PDFs under a directory
blobforge hydrate ./library

# Preview what would be written
blobforge hydrate ./library --dry-run

# Overwrite existing markdown/assets
blobforge hydrate ./library --force
```

### 8. Remove legacy coordination objects

After confirming the Bunny coordinator contains the full backlog, preview and
then remove the obsolete S3 queue and registry trees:

```bash
blobforge cleanup-legacy
blobforge cleanup-legacy --execute
```

The command never touches `store/raw/`, `store/out/`, or
`backups/coordinator/`.

## ⚙️ Configuration

Configuration is split into two categories:

### Local Configuration (Environment Variables)

Ingestors use S3 variables to upload raw PDFs. Conversion workers need only the
coordinator URL and their enrolled token.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `BLOBFORGE_S3_BUCKET` | `blobforge` | The target S3 bucket name |
| `BLOBFORGE_S3_PREFIX` | `pdf/` | Optional prefix for namespacing (e.g., `prod/`) |
| `BLOBFORGE_S3_REGION` | `us-east-1` | S3 region |
| `BLOBFORGE_S3_ACCESS_KEY_ID` | - | S3 access key (overrides AWS_ACCESS_KEY_ID) |
| `BLOBFORGE_S3_SECRET_ACCESS_KEY` | - | S3 secret key (overrides AWS_SECRET_ACCESS_KEY) |
| `BLOBFORGE_S3_ENDPOINT_URL` | - | For S3-compatible services (R2, MinIO, Ceph) |
| `BLOBFORGE_COORDINATOR_URL` | - | Bunny coordinator base URL |
| `BLOBFORGE_COORDINATOR_TOKEN` | - | Client token for ingestion/CLI or a UI-issued `bfw_...` worker token |
| `BLOBFORGE_LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

### Remote Configuration

These settings are stored in Bunny Database and edited in the management UI.

| Setting | Default | Description |
| :--- | :--- | :--- |
| `max_retries` | `3` | Number of failures before moving to dead-letter queue |
| `heartbeat_interval` | `60` | Seconds between heartbeat updates |
| `stale_timeout_minutes` | `15` | Minutes without heartbeat before job is considered stale |
| `conversion_timeout` | `3600` | Seconds before conversion timeout (hard kill via signal timer when supported) |

```bash
# View all remote config
blobforge config --show

# Update settings (takes effect within 1 hour on all workers)
blobforge config --set max_retries=5 conversion_timeout=7200
```

**Conversion timeout notes**
- Hard timeout enforcement uses `SIGALRM` + `ITIMER_REAL` when available.
- If platform/runtime constraints prevent timer signals, worker logs a warning and continues conversion without hard timeout.

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

## �🔮 Future Roadmap

*   **Metrics/Monitoring:** Prometheus metrics export for job duration, success rate
*   **Batching:** Support for tarball ingestion to process thousands of small files efficiently
*   **Vector Embeddings:** Worker modules for generating embeddings from images/text

## 📄 License

MIT
