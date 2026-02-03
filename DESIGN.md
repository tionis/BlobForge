# PDF Conversion Service - Design Document

## 1. Overview

A distributed, infrastructure-agnostic system to convert RPG rulebooks (PDF) into Markdown/Assets. The system prioritizes decoupling ingestion from processing and uses an S3-compatible object store as the single source of truth for both data and coordination (state), allowing for distributed workers without a dedicated database server.

### 1.1. S3 Provider Compatibility

The system requires S3-compatible storage with support for conditional writes (`If-None-Match: *`). Tested providers:
- **AWS S3:** ✅ Full support
- **Cloudflare R2:** ✅ Full support
- **Ceph Object Gateway:** ✅ Full support
- **MinIO:** ✅ Full support

## 2. Core Concepts

### 2.1. Hash Addressing (CAS)
- **ID:** SHA256 of the PDF content.
- **Raw Storage:** `store/raw/$HASH.pdf`
- **Output Storage:** `store/out/$HASH.zip`

### 2.2. Architecture Components
1. **Ingestor:** Scans sources (Git LFS, local folders), uploads unique PDFs to Raw Storage, and queues them for processing. State-aware to avoid duplicate queueing.
2. **Worker:** Stateless distributed agents that consume the queue, process PDFs, and upload results. Features heartbeat mechanism and retry tracking.
3. **Janitor:** Recovers stale jobs and manages the retry lifecycle.
4. **CLI:** Command-line interface for management operations.
5. **The "Bucket DB":** The S3 bucket itself acts as the database and queue using key prefixes.

### 2.3. Consolidated S3 Client
All components use a single `S3Client` class (`s3_client.py`) for consistency and maintainability. This module provides:
- Basic CRUD operations
- Atomic locking with `If-None-Match`
- Heartbeat updates
- Retry tracking
- State queries

## 3. The "Bucket DB" Structure (S3 Layout)

We use the S3 bucket to manage state.

```text
s3://my-bucket/
├── store/
│   ├── raw/
│   │   └── <HASH>.pdf        # Immutable source files (with metadata)
│   └── out/
│       └── <HASH>.zip        # Converted results
├── queue/
│   ├── todo/
│   │   ├── 1_critical/       # Priority Tiers
│   │   ├── 2_high/
│   │   ├── 3_normal/
│   │   ├── 4_low/
│   │   └── 5_background/
│   │       └── <HASH>        # Content: {"retries": N, "queued_at": ...}
│   ├── processing/
│   │   └── <HASH>            # Content: {"worker": "...", "started": ..., "last_heartbeat": ..., "priority": "...", "retries": N}
│   ├── failed/
│   │   └── <HASH>            # Content: {"error": "...", "worker": "...", "retries": N, ...}
│   └── dead/
│       └── <HASH>            # Content: {"error": "...", "total_retries": N, "reason": "exceeded_max_retries"}
└── registry/
    └── manifest.json         # File metadata index (see 3.2)
```

### 3.1. Queue State Transitions

```
                    ┌─────────────────────────────────────┐
                    │                                     │
                    ▼                                     │
[Ingest] ──► todo ──► processing ──► done                │
              ▲            │                             │
              │            ▼                             │
              │         failed ◄── (error during processing)
              │            │                             │
              │            ▼ (janitor: retry < MAX)      │
              └────────────┘                             │
                           │                             │
                           ▼ (janitor: retry >= MAX)     │
                         dead ──────────────────────────►┘
                                    (manual retry)
```

### 3.2. Manifest (File Metadata Index)

The manifest (`registry/manifest.json`) tracks metadata for all ingested files:

```json
{
  "version": 1,
  "updated_at": "2026-02-03T10:30:00Z",
  "entries": {
    "a1b2c3d4...": {
      "paths": ["DnD/Players Handbook.pdf", "backup/PHB.pdf"],
      "size": 52428800,
      "tags": ["DnD", "Players Handbook", "5e"],
      "ingested_at": "2026-02-01T08:00:00Z",
      "source": "rpg-library"
    }
  }
}
```

**Key features:**
- **Multiple paths per hash:** Tracks all locations of duplicate files
- **Optimistic locking:** Uses `If-Match` (ETag) for safe concurrent updates
- **Batched writes:** Ingestor batches updates (50 entries) to reduce write frequency
- **Searchable:** CLI supports search by filename or tag

**Concurrency model:**
- Only the ingestor writes to the manifest
- Workers read-only (for metadata enrichment)
- On conflict (412 Precondition Failed), retry with exponential backoff

## 4. Workflows

### 4.1. Ingestor (State-Aware)

The ingestor is responsible for ensuring the PDF exists in `store/raw/` and is queued if not already in the pipeline.

1. **Scan:** Walk directory tree (or clone repo with `GIT_LFS_SKIP_SMUDGE=1`).
2. **Identify:**
   - Parse LFS pointer files to get the SHA256 hash without downloading.
   - For non-LFS files, compute SHA256 hash.
3. **Check ALL States:** (prevents duplicate work and race conditions)
   - Check if `store/out/<HASH>.zip` exists → **Skip** (done)
   - Check if `queue/processing/<HASH>` exists → **Skip** (in progress)
   - Check if `queue/dead/<HASH>` exists → **Skip** (permanently failed)
   - Check if `queue/failed/<HASH>` exists → **Skip** (pending retry)
   - Check if `queue/todo/.../<HASH>` exists → **Skip** (already queued)
4. **Upload (if missing):**
   - If raw PDF is missing in S3:
     - Materialize file (git lfs pull or copy)
     - Upload to `store/raw/<HASH>.pdf`
     - **Metadata:** Attach `x-amz-meta-original-name`, `x-amz-meta-tags`, `x-amz-meta-size`
5. **Queue:**
   - Create `queue/todo/<priority>/<HASH>`

### 4.2. Worker (Distributed Loop with Heartbeat)

Workers run anywhere. They only need S3 credentials.

#### 4.2.1. Startup (Self-Cleanup)
- Generate or use persistent `WORKER_ID` (based on machine fingerprint)
- Scan `queue/processing/` for locks belonging to current `WORKER_ID`
- If found, assume crash from previous run → restore to todo queue

#### 4.2.2. Acquire Job (Atomic Locking with Improved Sharding)
- Iterate through priorities: `1_critical` → `2_high` → `3_normal` → `4_low` → `5_background`
- **Sharding:** Use 2-character hex prefix (256 shards) for better distribution
  - Random prefix like `"ab"`, `"7f"`, etc.
  - Reduces lock contention compared to single-character sharding
- **Atomic Lock:** Write `queue/processing/<HASH>` with `If-None-Match: *`
  - Content: `{"worker": "...", "started": ..., "last_heartbeat": ..., "priority": "...", "retries": N}`
- If write fails (412 Precondition Failed) → skip, try next
- **IMPORTANT:** Do NOT delete todo marker on lock acquisition (prevents race condition)

#### 4.2.3. Heartbeat
- Background thread updates `last_heartbeat` every 60 seconds
- Allows faster stale detection (15 minutes vs 2 hours)
- Optionally includes progress information: `{"stage": "converting"}`

#### 4.2.4. Process
- Download `store/raw/<HASH>.pdf` to temp directory
- Fetch S3 object metadata (original name, tags)
- Run conversion (marker/pymupdf4llm)
- Create `info.json` with enriched metadata
- Zip results

#### 4.2.5. Finalize (Atomic Completion)
- Upload `<HASH>.zip` to `store/out/`
- **Delete todo marker** (only NOW, after successful completion)
- Delete processing lock
- Clean up any previous failed marker

#### 4.2.6. Failure Handling
- If retries < MAX_RETRIES: Move to `queue/failed/<HASH>` with error details
- If retries >= MAX_RETRIES: Move to `queue/dead/<HASH>`
- Clear heartbeat and release lock

### 4.3. Janitor (Crash Recovery + Retry Management)

The janitor handles two responsibilities:

#### 4.3.1. Stale Job Recovery
- Scan `queue/processing/`
- Check `last_heartbeat` timestamp (not S3 LastModified)
- If `now - last_heartbeat > STALE_TIMEOUT` (default: 15 minutes):
  - Increment retry count
  - If retry < MAX_RETRIES: Restore to todo queue
  - If retry >= MAX_RETRIES: Move to dead-letter queue

#### 4.3.2. Failed Job Retry
- Scan `queue/failed/`
- Increment retry count
- If retry < MAX_RETRIES: Move back to todo queue
- If retry >= MAX_RETRIES: Move to dead-letter queue

### 4.4. Dead-Letter Queue

Jobs that exceed `MAX_RETRIES` (default: 3) are moved to `queue/dead/`. These jobs:
- Will not be automatically retried
- Can be manually retried via CLI: `blobforge retry <hash> --reset-retries`
- Preserve error history for debugging

## 5. Configuration

Environment variables (all prefixed with `BLOBFORGE_`):

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOBFORGE_S3_BUCKET` | `blobforge` | S3 bucket name |
| `BLOBFORGE_S3_PREFIX` | `pdf/` | Optional prefix for namespacing |
| `BLOBFORGE_WORKER_ID` | (auto-generated) | Persistent worker identifier |
| `BLOBFORGE_MAX_RETRIES` | `3` | Max failures before dead-letter |
| `BLOBFORGE_HEARTBEAT_INTERVAL` | `60` | Seconds between heartbeats |
| `BLOBFORGE_STALE_TIMEOUT_MINUTES` | `15` | Minutes until job considered stale |
| `BLOBFORGE_CONVERSION_TIMEOUT` | `3600` | Seconds before conversion killed |
| `BLOBFORGE_LOG_LEVEL` | `INFO` | Logging level |

## 6. CLI Commands

```bash
# Ingest PDFs from a directory
blobforge ingest /path/to/pdfs --priority 1_critical

# Check status of a job
blobforge status <hash>

# List queue statistics
blobforge list --verbose

# Change job priority
blobforge reprioritize <hash> 1_critical

# Retry a failed/dead job
blobforge retry <hash> --priority 2_high --reset-retries

# Run janitor
blobforge janitor --dry-run

# Show dashboard
blobforge dashboard --verbose

# Search manifest by filename or tag
blobforge search "Call of Cthulhu"

# Look up file by hash or path
blobforge lookup --hash <hash>
blobforge lookup --path "DnD/PHB.pdf"

# Show manifest statistics
blobforge manifest --verbose
```

## 7. Implementation

### 7.1. Module Structure

```
├── config.py        # Configuration and constants
├── s3_client.py     # Consolidated S3 operations
├── ingestor.py      # PDF discovery and queueing
├── worker.py        # Job processing with heartbeat
├── janitor.py       # Stale recovery and retry management
├── status.py        # Status display
└── cli.py           # Command-line interface
```

### 7.2. Technology Stack

- **Language:** Python 3.8+
- **S3 Library:** boto3
- **PDF Conversion:** marker (or pymupdf4llm)
- **Worker Identity:** Machine fingerprint (hostname + /etc/machine-id)

## 8. Scaling Considerations

The system is designed for moderate scale (~50,000 files, ~20 workers):

- **Sharding:** 256 shards (2-char hex prefix) prevents hot spots
- **Heartbeat:** 15-minute timeout balances responsiveness vs overhead
- **Listing Cost:** With 50K files and 20 workers polling every 10s, expect ~120 LIST requests/minute
- **Single Ingestor:** Design assumes single writer for ingestion (no manifest conflicts)

For larger scale, consider:
- Adding SQS/SNS as optional acceleration layer
- Using SQLite + Litestream for manifest storage
- Implementing worker coordination for shard assignment
