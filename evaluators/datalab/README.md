# Datalab Convert evaluator

This adapter calls the Datalab Convert v1 API in `accurate` mode with paginated
Markdown and image extraction. It requires explicit page and returned-cost
ceilings plus confirmation that the PDF may be uploaded.

```bash
uv sync --project evaluators/datalab
uv run blobforge evaluate datalab book.pdf \
  --max-pages 8 --max-cost-usd 0.10 --confirm-api-rights \
  --response-cache /srv/blobforge/datalab-responses \
  -o book.datalab.mdaf
```

Datalab does not expose a pre-request quote on this endpoint. The page ceiling
bounds the submitted work and the adapter checks the response's final cost
against the requested dollar ceiling. The complete result is cached before
MDAF packaging. A cache hit does not need `DATALAB_API_KEY`.
