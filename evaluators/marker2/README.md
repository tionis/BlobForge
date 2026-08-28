# Marker 2 evaluator

This environment pins Marker 2.0.0 independently from the production Marker 1
compatibility environment. On the 32-GiB CPU host, start with deterministic
text-layer mode, which requires no Surya inference server:

```bash
uv sync --project evaluators/marker2
uv run blobforge evaluate marker2 book.pdf --no-ocr -o book.marker2-no-ocr.mdaf
```

CPU `fast` mode with OCR may start Marker/Surya's lightweight services and is a
separate recipe. Balanced mode is not provisioned on this host: it requires a
GPU or an explicitly configured remote Surya backend. Every variant retains
page mappings and native Marker metadata.

The current no-OCR recipe targets born-digital illustrated PnP rulebooks with a
usable text layer. OCR-capable Marker variants remain separate recipes and are
not required to promote this digital-PDF profile.
