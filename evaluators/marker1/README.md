# Isolated Marker 1 compatibility evaluator

This environment freezes the legacy compatibility generation at
`marker-pdf==1.10.2`. It deliberately enables Marker pagination so new test
artifacts contain exact page-to-Markdown byte mappings; historical ZIPs did not
enable that setting and are migrated more conservatively.

This compatibility recipe is evaluated for born-digital illustrated PnP
rulebooks. Scan-heavy support is not an acceptance requirement; any future OCR
profile must be represented by a separate recipe identity.

```bash
UV_CACHE_DIR=/tmp/blobforge-marker1-cache uv sync --project evaluators/marker1
```

Model checkpoint identifiers/checksums remain a production gate even though the
Python package is pinned.
