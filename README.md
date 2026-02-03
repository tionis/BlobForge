# BlobForge

**BlobForge** is a distributed, infrastructure-agnostic ingestion pipeline designed to process massive datasets (starting with RPG Rulebooks) into usable formats (Markdown/Assets).

It relies entirely on an **S3-compatible object store** for coordination, state management, and data storage. This "Serverless / No-Database" approach allows the system to run for years with near-zero maintenance, scale from 1 to 100 workers instantly, and survive complete shutdowns without state loss.

## 🚀 Key Features

*   **S3-Only Architecture:** No PostgreSQL, Redis, or RabbitMQ required. The bucket *is* the database.
*   **Git LFS Optimized:** "Materializes" PDFs from LFS pointers only when necessary, saving bandwidth and storage.
*   **Distributed Locking:** Uses S3 atomic operations (`If-None-Match`) to coordinate workers without race conditions.
*   **Priority Queues:** Supports `highest`, `higher`, and `normal` priority tiers.
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
│   ├── todo/          # Pending jobs (Empty marker files)
│   │   ├── 1_highest/
│   │   ├── 2_higher/
│   │   └── 3_normal/
│   ├── processing/    # Active locks (JSON content)
│   └── failed/        # Error logs
```

## 📦 Installation

### Option A: Docker (Recommended)

The Docker image includes all dependencies for PDF processing (`marker-pdf`, `ocr`, `torch`).

```bash
# Build the image
docker build -t blobforge .

# Run a Worker
docker run -d \
  -e S3_BUCKET=my-forge-bucket \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  blobforge
```

### Option B: Local Python

Requires Python 3.9+ and system dependencies (`tesseract-ocr`, `ghostscript`).

```bash
pip install -r requirements.txt  # (See Dockerfile for full list)
# OR manually:
pip install boto3 marker-pdf
```

## 💻 Usage (CLI)

BlobForge comes with a unified CLI for managing the pipeline.

### 1. Ingest Data
Scans a local directory (or Git repo), calculates hashes, and queues new files.

```bash
# Ingest a library with normal priority
python3 cli.py ingest ./library/rpg-books

# Ingest urgent files
python3 cli.py ingest ./library/hot-fixes --priority 1_highest
```

### 2. Start a Worker
Workers automatically find jobs, lock them, process them, and upload results.

```bash
python3 worker.py
```
*Run multiple instances of this script on any number of machines to scale.*

### 3. Monitor Status
View queue counts, active processing jobs, and stalled workers.

```bash
python3 cli.py list -v

# Check specific file status
python3 cli.py status <SHA256_HASH>
```

### 4. Maintenance (Janitor)
If a worker crashes, its lock remains in `processing/`. The Janitor detects stale locks (>2 hours) and re-queues them.

```bash
python3 janitor.py
```

## ⚙️ Configuration

Configuration is handled via Environment Variables or `config.py`.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `S3_BUCKET` | `my-pdf-bucket` | The target S3 bucket name. |
| `WORKER_ID` | `(random)` | ID for the worker instance. Set this for persistent identity. |
| `AWS_...` | - | Standard AWS/Boto3 credentials. |

## 🏗 Project Structure

*   `cli.py`: Unified management interface.
*   `ingestor.py`: Scans filesystem, uploads RAW blobs, queues jobs.
*   `worker.py`: The workhorse. Polls S3, locks jobs, runs `marker-pdf`.
*   `janitor.py`: Cleans up stale locks.
*   `status.py`: Reporting dashboard.
*   `config.py`: Shared constants.
*   `DESIGN.md`: Detailed architectural decisions.

## 🔮 Future Roadmap (BlobForge Expansion)

*   **Batching:** Support for tarball ingestion to process thousands of small files (images) efficiently.
*   **Vector Embeddings:** Worker modules for generating embeddings from images/text.
