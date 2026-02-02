# PDF Conversion Service - Design Document

## 1. Overview
A distributed, infrastructure-agnostic system to convert RPG rulebooks (PDF) into Markdown/Assets. The system prioritizes decoupling ingestion from processing and uses an S3-compatible object store as the single source of truth for both data and coordination (state), allowing for distributed workers without a dedicated database server.

## 2. Core Concepts

### 2.1. Hash Addressing (CAS)
- **ID:** SHA256 of the PDF content.
- **Raw Storage:** `store/raw/$HASH.pdf`
- **Output Storage:** `store/out/$HASH.zip`

### 2.2. Architecture Components
1.  **Ingestor:** Scans sources (Git LFS, local folders), uploads unique PDFs to Raw Storage, and queues them for processing.
2.  **Worker:** Stateless distributed agents that consume the queue, process PDFs, and upload results.
3.  **The "Bucket DB":** The S3 bucket itself acts as the database and queue using key prefixes.

## 3. The "Bucket DB" Structure (S3 Layout)

We use the S3 bucket to manage state.

```text
s3://my-bucket/
├── store/
│   ├── raw/
│   │   └── <HASH>.pdf        # Immutable source files
│   └── out/
│       └── <HASH>.zip        # Converted results
├── queue/
│   ├── todo/
│   │   └── <HASH>            # Empty marker file. Priority encoded in metadata?
│   ├── processing/
│   │   └── <HASH>            # Content: {"worker_id": "...", "started_at": "..."}
│   └── failed/
│       └── <HASH>            # Content: Error logs
└── registry/
    └── manifest.json         # (Optional) Global index of filename -> HASH mappings
```

## 4. Workflows

### 4.1. Ingestor (Git LFS Optimized)
The ingestor is responsible for ensuring the PDF exists in `store/raw/` and is queued if not processed.

1.  **Scan:** Clone/Pull repo with `GIT_LFS_SKIP_SMUDGE=1`.
2.  **Identify:**
    - Parse LFS pointer files to get the SHA256 hash without downloading.
    - Determine `filesize`.
3.  **Check State:**
    - Check if `store/out/<HASH>.zip` exists. If yes -> **Skip**.
    - Check if `store/raw/<HASH>.pdf` exists.
4.  **Upload (if missing):**
    - If raw PDF is missing in S3:
        - `git lfs pull --include <filepath>` (Download single file locally).
        - Upload to `store/raw/<HASH>.pdf`.
        - Delete local file.
5.  **Queue:**
    - Check if `queue/todo/<HASH>` or `queue/processing/<HASH>` exists.
    - If neither exists, create `queue/todo/<HASH>`.

### 4.2. Worker (Distributed Loop)
Workers run anywhere. They only need S3 credentials.

1.  **Acquire Job (Locking):**
    - List objects in `queue/todo/`.
    - Pick one (randomly or sorted).
    - **Atomic Lock:** Attempt to write `queue/processing/<HASH>` with `If-None-Match: *` (Ensure file doesn't exist yet).
        - *Content:* Worker ID, Timestamp.
    - If Write Fails (412 Precondition Failed): Another worker took it. **Retry loop.**
    - If Write Success:
        - Delete `queue/todo/<HASH>`.
        - **Lock Acquired.**
2.  **Process:**
    - Download `store/raw/<HASH>.pdf` to temp dir.
    - Run conversion (Markdown + Asset extraction).
    - Create `info.json`.
    - Zip result to `<HASH>.zip`.
3.  **Finalize:**
    - Upload `<HASH>.zip` to `store/out/`.
    - Delete `queue/processing/<HASH>`.
    - (Optional) Delete `store/raw/<HASH>.pdf` if "cleanup mode" is enabled (save storage costs).
4.  **Failure Handling:**
    - If crash/error: Move `queue/processing/<HASH>` to `queue/failed/<HASH>` with error log.

## 5. Recovery & Timeout
Since workers can crash holding a lock:
- **Monitor:** A simple maintenance script (or the Ingestor) checks `queue/processing/`.
- **Rule:** If timestamp > 2 hours ago:
    - Move back to `queue/todo/`.
    - Log warning.

## 6. Implementation Plan
1.  **Step 1: The Scanner (Ingestor)**
    - Python script using `git` CLI.
    - Parses LFS pointers.
    - S3 upload logic.
2.  **Step 2: The Worker**
    - S3 locking logic wrapper.
    - Stub conversion function.
3.  **Step 3: Integration**
    - Run Ingestor on repo.
    - Run multiple Workers.

## 7. Technology Choice
- **Language:** Python (excellent S3 libraries `boto3`, good PDF tools).
- **PDF Tools:** `pymupdf4llm` (great for MD extraction) or `marker`.