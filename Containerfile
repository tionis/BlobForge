FROM ghcr.io/astral-sh/uv:latest AS uv

FROM python:3.12-slim AS builder

COPY --from=uv /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY blobforge/ ./blobforge/

ARG TORCH_FLAVOR=cpu
RUN uv sync --frozen --no-dev --extra convert --extra metrics \
    && if [ "$TORCH_FLAVOR" = cpu ] && [ "$(uname -m)" = x86_64 ]; then \
         uv pip install --python .venv/bin/python --reinstall \
           --no-deps \
           --index https://download.pytorch.org/whl/cpu "torch==2.10.0" \
         && uv pip uninstall --python .venv/bin/python \
           cuda-bindings cuda-pathfinder triton \
           nvidia-cublas-cu12 nvidia-cuda-cupti-cu12 nvidia-cuda-nvrtc-cu12 \
           nvidia-cuda-runtime-cu12 nvidia-cudnn-cu12 nvidia-cufft-cu12 \
           nvidia-cufile-cu12 nvidia-curand-cu12 nvidia-cusolver-cu12 \
           nvidia-cusparse-cu12 nvidia-cusparselt-cu12 nvidia-nccl-cu12 \
           nvidia-nvjitlink-cu12 nvidia-nvshmem-cu12 nvidia-nvtx-cu12; \
       fi

FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ghostscript \
    tesseract-ocr \
    libgl1 \
    libglib2.0-0 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/var/cache/blobforge/huggingface \
    TORCH_HOME=/var/cache/blobforge/torch

VOLUME ["/var/cache/blobforge"]
ENTRYPOINT ["blobforge"]
CMD ["worker"]
