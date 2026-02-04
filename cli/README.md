# BlobForge Python CLI

Simple command-line tool for submitting jobs and checking status.

> **Note:** For more features, use the Go CLI in `server/cmd/blobforge/`.

## Installation

```bash
pip install httpx
```

## Configuration

```bash
export BLOBFORGE_SERVER_URL=http://localhost:8080
export BLOBFORGE_API_TOKEN=bf_your_token_here  # Get from Admin UI
```

## Usage

### Submit a PDF

```bash
python blobforge.py submit /path/to/document.pdf
```

### Check Stats

```bash
python blobforge.py stats
```

### List Jobs

```bash
python blobforge.py jobs
```

## Getting an API Token

1. Open BlobForge dashboard
2. Go to **Admin** → **API Tokens**
3. Click **Create Token**
4. Copy the token (shown only once!)
