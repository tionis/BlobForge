# BlobForge PDF Worker

Converts PDF files to Markdown using [marker-pdf](https://github.com/VikParuchuri/marker).

## Installation

```bash
cd workers/pdf
uv pip install -e .
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOBFORGE_SERVER_URL` | `http://localhost:8080` | BlobForge server URL |
| `BLOBFORGE_POLL_INTERVAL` | `5` | Seconds between job polls |
| `BLOBFORGE_HEARTBEAT_INTERVAL` | `30` | Seconds between heartbeats |
| `BLOBFORGE_MAX_BATCH_PAGES` | `4` | Max pages per batch for conversion |

## Usage

```bash
# Run the worker
blobforge-pdf-worker

# Or run directly
python worker.py
```

## Docker

```bash
# Build
docker build -t blobforge-pdf-worker .

# Run with GPU
docker run --gpus all \
    -e BLOBFORGE_SERVER_URL=http://server:8080 \
    -v $HOME/.cache:/root/.cache \
    blobforge-pdf-worker
```
