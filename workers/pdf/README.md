# BlobForge PDF Worker

Converts PDF files to Markdown using [marker-pdf](https://github.com/VikParuchuri/marker).

## Quick Start

```bash
# Install dependencies
pip install httpx marker-pdf

# Configure
export BLOBFORGE_SERVER_URL=http://localhost:8080
export BLOBFORGE_WORKER_ID=my-pdf-worker      # Created in Admin UI
export BLOBFORGE_WORKER_SECRET=bfw_xxx...      # Shown once when creating worker

# Run the worker
python worker.py
```

## Installation

### From Source

```bash
cd workers/pdf
pip install -e .
# or
uv pip install -e .
```

### Using UV

```bash
uv pip install -e .
blobforge-pdf-worker
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOBFORGE_SERVER_URL` | `http://localhost:8080` | BlobForge server URL |
| `BLOBFORGE_WORKER_ID` | auto-generated | Worker ID (from Admin UI) |
| `BLOBFORGE_WORKER_SECRET` | - | **Required** Worker secret from server admin |
| `BLOBFORGE_POLL_INTERVAL` | `5` | Seconds between job polls |
| `BLOBFORGE_HEARTBEAT_INTERVAL` | `30` | Seconds between heartbeats |
| `BLOBFORGE_USE_GPU` | `false` | Enable GPU acceleration |
| `BLOBFORGE_MAX_BATCH_PAGES` | `4` | Max pages per batch for conversion |

### Creating Worker Credentials

1. Open BlobForge dashboard
2. Go to **Workers** page
3. Click **Create Worker** (admins only)
4. Enter a worker ID/name
5. Copy the secret (shown only once!)
6. Set `BLOBFORGE_WORKER_ID` and `BLOBFORGE_WORKER_SECRET` environment variables

Workers can be enabled/disabled and have secrets regenerated from the dashboard.

## GPU Acceleration

For faster processing with CUDA:

```bash
# Install with GPU support
pip install marker-pdf[gpu]

# Enable GPU
export BLOBFORGE_USE_GPU=true
export CUDA_VISIBLE_DEVICES=0  # Optional: select specific GPU

python worker.py
```

## Docker

### CPU

```bash
docker build -t blobforge-pdf-worker -f Containerfile .

docker run -d \
  -e BLOBFORGE_SERVER_URL=http://server:8080 \
  -e BLOBFORGE_WORKER_ID=my-pdf-worker \
  -e BLOBFORGE_WORKER_SECRET=bfw_xxx... \
  blobforge-pdf-worker
```

### GPU (NVIDIA)

```bash
docker run -d --gpus all \
  -e BLOBFORGE_SERVER_URL=http://server:8080 \
  -e BLOBFORGE_WORKER_ID=my-pdf-worker \
  -e BLOBFORGE_WORKER_SECRET=bfw_xxx... \
  -e BLOBFORGE_USE_GPU=true \
  -v $HOME/.cache:/root/.cache \
  blobforge-pdf-worker
```

## How It Works

1. Worker registers with BlobForge server (using worker credentials)
2. Polls for `pdf-convert` jobs
3. Downloads PDF via presigned URL
4. Converts to Markdown using marker-pdf
5. Uploads result via presigned URL
6. Reports completion to server

## Troubleshooting

### Worker not connecting

```bash
# Test server connectivity
curl -H "Authorization: Bearer $BLOBFORGE_API_TOKEN" \
  $BLOBFORGE_SERVER_URL/api/stats
```

### GPU not detected

```bash
# Check CUDA availability
python -c "import torch; print(torch.cuda.is_available())"
```

### Memory issues

Reduce batch size:

```bash
export BLOBFORGE_MAX_BATCH_PAGES=2
```
