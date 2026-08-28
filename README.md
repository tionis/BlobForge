# BlobForge

**BlobForge** is a distributed, infrastructure-agnostic ingestion pipeline designed to process massive datasets (starting with RPG Rulebooks) into usable formats (Markdown/Assets).

The new self-hosted backend uses **SQLite** for coordination and a **local
directory** for source and artifact storage. It runs as a conventional Python
service and ships with a rootless-friendly Podman Quadlet deployment. The
former Bunny/S3 backend remains only as a migration source during cutover.

## 🚀 Key Features

*   **Self-hosted Coordination:** One SQLite/filesystem service replaces Bunny and S3 without requiring PostgreSQL, Redis, or a message broker.
*   **Management UI:** OIDC/SCIM-gated job operations, worker registration, revocable admin tokens, recipe controls, and an audit feed.
*   **Web Library:** Stream-upload sources, search paths/tags/hashes, filter and page through every job state, inspect failures/artifacts, and download source/result files.
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
*   **Hash-Addressed:** New source identity is BLAKE3 with retained SHA-256 aliases for migration.
*   **Media-ready:** Sources and worker claims carry media types; PDF workers cannot accidentally claim future audio, image, or video work.

## 🛠 Architecture

The target architecture and deployment/cutover guide are documented in
[docs/local_backend.md](docs/local_backend.md). The prior Bunny design is
retained in [docs/bunny_coordination_backend.md](docs/bunny_coordination_backend.md)
as migration history.
Administrator workflows and mutation semantics are documented in
[docs/management_console.md](docs/management_console.md).

Set `BLOBFORGE_COORDINATOR_URL` and `BLOBFORGE_COORDINATOR_TOKEN` on clients and
workers. They use coordinator-issued signed transfers and require neither host
filesystem access nor S3 credentials.

Worker labels become stable IDs by slugging them (`GPU Workstation` becomes
`gpu-workstation`). Duplicate or slug-colliding labels are rejected. A worker
token belongs to that single enrollment and should not be reused to represent
multiple machines.
The durable server volume contains SQLite state and immutable objects:

```text
/var/lib/blobforge/
├── blobforge.sqlite3
├── capability.key
├── objects/sources/              # Canonical source bytes
├── objects/artifacts/            # Recipe-specific ZIP/MDAF artifacts
└── pending/                      # Lease-scoped uploads
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

### Option A: Managed Linux worker (Recommended)

The published CPU and CUDA containers include the Python environment and PDF
conversion dependencies. A setup script installs the appropriate image as a
user-level systemd service, keeps the enrollment token in a private environment
file, and persists model caches.

```bash
curl -fsSLO https://raw.githubusercontent.com/tionis/BlobForge/main/scripts/install-linux-worker.sh
chmod +x install-linux-worker.sh
./install-linux-worker.sh --coordinator-url https://blobforge.example --token bfw_...
```

See [Linux worker setup](docs/linux_worker_setup.md) for run windows, GPU
passthrough, service management, upgrades, and a native `uv` fallback.

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
# Install the project with PDF conversion support
uv sync --extra convert
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
4. Requests a signed raw-object upload URL from the coordinator (upload only if not already present)
5. Creates a job in the coordinator's todo queue

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
Worker startup validates the optional Marker conversion runtime before it
contacts the coordinator. A base-only checkout therefore exits without
claiming work; run `uv sync --extra convert` before starting a native worker.
BlobForge currently constrains production workers to Marker 1.x because Marker
2 uses a materially different VLM conversion pipeline and external inference
server. See [Conversion runtime compatibility](docs/conversion_runtime.md).
Completed archives also record exact runtime provenance and are keyed by a
stable conversion recipe, allowing future Marker generations to coexist for
A/B evaluation without overwriting current results. See
[Recipe-aware conversion provenance](docs/conversion_provenance.md).

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

### 3. Inspect and Select Conversion Artifacts

Each worker advertises its active recipe digest. Retained outputs can be listed,
downloaded, and previewed without changing the currently selected conversion.
Requesting a digest selects it immediately when that artifact already exists;
otherwise it queues the document until a compatible worker is available.

```bash
# Find worker recipe digests
blobforge workers --verbose

# List every retained output for a source PDF hash
blobforge artifacts <document-hash>

# Download or preview one historical/specific recipe
blobforge download <document-hash> --recipe-digest <recipe-digest>
blobforge preview <document-hash> --recipe-digest <recipe-digest>

# Preview, then select an existing artifact or queue an exact recipe
blobforge request-conversion <document-hash> <recipe-digest> --dry-run
blobforge request-conversion <document-hash> <recipe-digest>
```

The coordinator URL and admin token can be supplied through
`BLOBFORGE_COORDINATOR_URL` / `BLOBFORGE_COORDINATOR_TOKEN` or the commands'
`--coordinator-url` / `--token` options.

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
- The coordinator records the worker as `suspended` with its condition and next eligible timestamp. Periodic heartbeats stop until it resumes.
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
Requires `BLOBFORGE_COORDINATOR_URL` / `BLOBFORGE_COORDINATOR_TOKEN` (an admin
token); availability is checked in one bulk request and archives stream through
signed URLs.

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

For large trees, preview and then either remove those hydrated siblings or
replace each Markdown/assets pair with one standard TextPack archive:

```bash
# Both commands are previews unless --execute is supplied
blobforge hydrated clean ./library
blobforge hydrated clean ./library --execute

blobforge hydrated textpack ./library
blobforge hydrated textpack ./library --execute

# Existing .textpack files are skipped unless explicitly replaced
blobforge hydrated textpack ./library --execute --force

# Restore TextPacks to <stem>.md and <stem>.assets/, removing each archive
blobforge hydrated unpack ./library
blobforge hydrated unpack ./library --execute

# Existing Markdown/assets are skipped unless explicitly replaced
blobforge hydrated unpack ./library --execute --force

# Preview/remove PDF-anchored TextPack files
blobforge hydrated clean-textpacks ./library
blobforge hydrated clean-textpacks ./library --execute
```

Discovery is PDF-anchored, so unrelated Markdown/assets are not touched. A
TextPack is validated before its source Markdown and assets are removed. Reverse
conversion validates the archive before restoring Markdown/assets and only
removes the TextPack after the restoration succeeds.

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

Self-hosted servers use the `BLOBFORGE_SERVER_*` variables. Clients and workers
use only the coordinator URL and the token assigned to their role. The S3
variables below are legacy migration/fallback configuration.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `BLOBFORGE_SERVER_DATA_DIR` | `/var/lib/blobforge` | SQLite and local object root for `blobforge serve` |
| `BLOBFORGE_SERVER_CLIENT_TOKEN` | - | Required operator/client control-plane token |
| `BLOBFORGE_SERVER_WORKER_TOKENS` | `{}` | JSON object mapping fixed worker IDs to tokens |
| `BLOBFORGE_SERVER_PUBLIC_URL` | request origin | External URL used in signed transfer links behind a proxy |
| `BLOBFORGE_SERVER_LEASE_SECONDS` | `900` | Fenced processing lease duration |
| `BLOBFORGE_SERVER_CAPABILITY_TTL` | `900` | Signed transfer URL lifetime |
| `BLOBFORGE_SERVER_MAX_RETRIES` | `3` | Failures allowed before dead-letter state |
| `BLOBFORGE_S3_BUCKET` | `blobforge` | The target S3 bucket name |
| `BLOBFORGE_S3_PREFIX` | `pdf/` | Optional prefix for namespacing (e.g., `prod/`) |
| `BLOBFORGE_S3_REGION` | `us-east-1` | S3 region |
| `BLOBFORGE_S3_ACCESS_KEY_ID` | - | S3 access key (overrides AWS_ACCESS_KEY_ID) |
| `BLOBFORGE_S3_SECRET_ACCESS_KEY` | - | S3 secret key (overrides AWS_SECRET_ACCESS_KEY) |
| `BLOBFORGE_S3_ENDPOINT_URL` | - | For S3-compatible services (R2, MinIO, Ceph) |
| `BLOBFORGE_COORDINATOR_URL` | - | Coordinator base URL |
| `BLOBFORGE_COORDINATOR_TOKEN` | - | Client token for tools or token bound to one worker |
| `BLOBFORGE_LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

### Remote Configuration

These settings are stored in Bunny Database and edited in the management UI.

| Setting | Default | Description |
| :--- | :--- | :--- |
| `max_retries` | `3` | Number of failures before moving to dead-letter queue |
| `heartbeat_enabled` | `true` | Send idle and prompt progress heartbeats; active leases are still renewed when disabled |
| `heartbeat_interval` | `60` | Seconds between heartbeat updates |
| `lease_seconds` | `900` | Processing lease duration; lease-only mode renews one-third through this period |
| `conversion_timeout` | `3600` | Seconds before conversion timeout (hard kill via signal timer when supported) |

```bash
# View all remote config
blobforge config --show

# Configuration is edited in the management console.
```

Workers receive current configuration on registration, claims, and heartbeats.
A heartbeat interval change therefore applies immediately after the next such
request. Disabling normal heartbeats suppresses idle and prompt progress
updates; an active worker still renews its lease one-third of the way to expiry.

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

## MDAF migration and converter evaluation

BlobForge's v2 path uses canonical BLAKE3 source identities and validated MDAF
v1 Markdown artifacts while retaining SHA-256 as a legacy alias. Local Marker 1
and Docling environments are isolated and CPU-pinned so they can be compared on
the same machine without dependency conflicts:

```bash
uv sync --project evaluators/marker1
uv sync --project evaluators/docling
uv run blobforge evaluate marker1 ./book.pdf -o ./book.marker1.mdaf
uv run blobforge evaluate docling ./book.pdf -o ./book.docling.mdaf
uv run blobforge review-bundle ./book.pdf ./book.marker1.mdaf ./book.docling.mdaf \
  --pages 1-8 --output ./book-review
```

Hosted Mistral trials support a no-request `--plan` mode and require explicit
`--confirm-api-rights` plus page and spend ceilings before upload. Review
bundles keep the candidate mapping in a separate private key, expose linked
assets under neutral magic-checked raster paths, and export blinded page scores
directly from a local browser. Validate, unblind, and summarize an export with
`blobforge review-summarize RESULT.json --key CAMPAIGN.key.json`.
Use `review-bundle --random-seed` for human scoring, and start a new campaign
whenever its candidate mapping has been disclosed.

The local legacy migration is resumable and does not write to S3:

```bash
uv run blobforge migrate inventory
uv run blobforge migrate legacy --limit 20
uv run blobforge migrate legacy --jobs 2
uv run blobforge migrate verify
uv run blobforge migrate report
uv run blobforge migrate stage   # local v2 object-key tree; no upload
```

See [the migration runbook](docs/local_mdaf_migration.md),
[the MDAF redesign](docs/mdaf_redesign.md), and
[the benchmark canary](docs/converter_benchmark_results.md), and
[the adapter architecture](docs/converter_adapter_architecture.md), and
[the review workflow](docs/conversion_review.md) for safety,
provenance, object-layout, and evaluation details.

## 🏗 Project Structure

```
├── pyproject.toml   # Package configuration and dependencies
├── blobforge/       # Main package
│   ├── cli.py       # Unified command-line interface
│   ├── ingestor.py  # Scans filesystem, uploads RAW blobs, queues jobs
│   ├── worker.py    # Claims fenced jobs, runs marker-pdf, publishes artifacts
│   ├── server/      # FastAPI, SQLite coordination, signed local transfers
│   ├── local_import.py # Offline v2 MDAF/raw-source backend migration
│   ├── status.py    # Reporting dashboard
│   ├── s3_client.py # Legacy S3 migration/fallback implementation
│   └── config.py    # Shared legacy configuration and constants
├── deploy/quadlet/  # Rootless/system Podman deployment examples
├── tests/           # Unit tests
├── DESIGN.md        # Detailed architectural decisions
├── Containerfile.server # Lightweight backend image
└── Containerfile    # CPU/CUDA worker image
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
