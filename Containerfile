# Base Image with Python 3.9+
FROM python:3.10-slim

# Install System Dependencies for Marker (OCR, PDF tools)
# ghostscript, tesseract-ocr, git (for LFS/marker), libgl1 (for cv2)
RUN apt-get update && apt-get install -y \
    git \
    ghostscript \
    tesseract-ocr \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Set Working Directory
WORKDIR /app

# Install Python Dependencies
# boto3 for S3 operations (marker-pdf will be installed via package extras)
RUN pip install --no-cache-dir boto3

# Copy Package
COPY pyproject.toml README.md ./
COPY blobforge/ ./blobforge/

# Install the package with all optional dependencies (metrics, convert)
RUN pip install --no-cache-dir ".[all]"

# Environment Variables (Override these at runtime)
ENV BLOBFORGE_S3_BUCKET=my-pdf-bucket
ENV PYTHONUNBUFFERED=1

# Default Command: Run Worker
CMD ["blobforge", "worker"]
