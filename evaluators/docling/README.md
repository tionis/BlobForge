# Isolated Docling evaluator

This environment pins Docling separately from BlobForge's Marker dependencies.
Prepare it with:

```bash
UV_CACHE_DIR=/tmp/blobforge-docling-cache uv sync --project evaluators/docling
```

The adapter implements `dev.tionis.blobforge.converter-bundle/v1`. BlobForge
starts it as a subprocess and owns BLAKE3 hashing, provenance assembly, MDAF
packaging, and validation. The adapter retains Docling's lossless JSON as an
opaque rendition and turns Docling page breaks into exact UTF-8 page spans.

The current standard profile is evaluated for born-digital illustrated PnP
rulebooks. Scan-heavy or VLM/OCR behavior belongs to a separately pinned recipe
and does not block promotion of this profile.

Docling models are downloaded on first use. Evaluation output is not eligible
for production publication until the model files/checksums are frozen in the
recipe and provenance rather than recorded as a mutable default alias.
