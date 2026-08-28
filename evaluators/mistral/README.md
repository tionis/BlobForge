# Mistral OCR 4.1 evaluator

This adapter uses the pinned `mistralai` SDK and the explicit
`mistral-ocr-4-1` model. It requires both a maximum page count and a maximum
list-price budget; it refuses the request before upload if either ceiling is
missing or too low. Uploaded temporary files are deleted in a `finally` block.
Signed URLs and credentials never enter the converter bundle or MDAF.

```bash
uv sync --project evaluators/mistral
MISTRAL_API_KEY=... uv run blobforge evaluate mistral book.pdf \
  --max-pages 500 --max-cost-usd 2.00 -o book.mistral.mdaf
```

The full native OCR response, including blocks and confidence data, is retained
as an opaque rendition; page Markdown becomes exact page-level UTF-8 mappings.

Although the provider calls this an OCR API, BlobForge initially evaluates it
on the same born-digital PnP rulebook corpus for layout and Markdown quality.
Available subscription credits may be consumed in resumable bounded batches
over several quota periods. Record list-price estimate, billed usage, and
credits applied separately; never relax `--max-pages` or `--max-cost-usd`
because a request is expected to be credited.
