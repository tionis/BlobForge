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
# 1. marker-pdf (and its torch deps)
# 2. boto3 (for our scripts)
RUN pip install --no-cache-dir marker-pdf boto3

# Copy Package
COPY pyproject.toml .
COPY blobforge/ ./blobforge/

# Install the package
RUN pip install --no-cache-dir .

# Environment Variables (Override these at runtime)
ENV BLOBFORGE_S3_BUCKET=my-pdf-bucket
ENV PYTHONUNBUFFERED=1

# Default Command: Run Worker
CMD ["blobforge", "worker"]
