# Unlimited-OCR-2 GPU canary evaluator

An evidence runner for `baidu/Unlimited-OCR-2` at revision
`9cf077c4b206e0e2a31879df937b0b0bcba3661c`, built for a temporary Colab
L4/A100 session. It uses the upstream Transformers path (BF16, torch 2.13.0
cu126, transformers 4.52.1) in its own locked environment. It does not build
MDAFs: packaging and scoring run locally from the cached output.

- `run_unlimited_ocr2.py`: the `run`, `summarize` and `self-test` subcommands.
- `plan.canary.json`: Storypath (8 pages), London Falling tables (8 pages) and
  the exact retained v1 CPU-canary raster, totalling 50 page generations.
- `colab_unlimited_ocr2.ipynb`: installs the lock, downloads and verifies the
  model, runs a timing probe, then runs the full plan.

## What it records

For each unit (one page in page-local mode, or one chunk in multi-page mode)
it records:

- the raw generation with special tokens and grounding tags kept;
- the generated token IDs;
- per-page token counts, parsed blocks and invalid boxes;
- wall, prefill and model time, plus peak VRAM;
- every intervention by the logit guards.

It also records rendered-page digests, the PDF raster transform, exact package
versions, the GPU and its driver. Upstream's page-count guard forces exactly
one `<PAGE>` per input, so a matching page count proves nothing. The runner
wraps the guard and the n-gram processor in observers and flags these events
as failures: a suppressed early EOS, a forced page break and a forced EOS.
A multi-page chunk that fails is halved automatically. Results are written
atomically to Drive and never recomputed, so a disconnected session resumes.

The two modes are separate recipes (page-local: gundam tiles; multi-page: base
global views). Their `recipe_id` values are evaluation identities. They are not
registered BlobForge recipe digests.

## Output format and reanalysis

v2 emits `<BLOCK>label<LEVELn>[x1, y1, x2, y2]<FORK>text`, where boxes are
normalized to 0..999. `<MERGE>` joins a block to the next across a column
break, and multi-page output labels page-break continuations `cross_text` and
`append_text`. The parser also accepts v1's `<|det|>` syntax.

`reanalyze --out DIR` recomputes every unit's derived analysis from its
retained `raw.txt` and `token_ids.json`, verifying the recorded raw digest.
The summary reports raw-byte and text-only repeat identity separately, because
GPU repeats can move boxes by a unit or two.

## Packaging and coverage

Packaging runs locally in the BlobForge environment with no GPU:

```bash
uv run --with pillow==13.0.0 python evaluators/unlimited_ocr2/package.py \
  --evidence .blobforge-migration/evaluations/unlimited-ocr2-colab-canary \
  --run london-page-local -o london.uocr2-page-local.mdaf
```

`package.py` selects the run's top-level units, descending into halves when a
unit failed. `adapter.py` emits a converter bundle for the shared builder; its
normalization rules are in its docstring and in
`docs/unlimited_ocr2_colab_canary.md`.

The `coverage` subcommand flags likely omissions from text-layer recall and
cross-mode disagreement:

```bash
uv run --no-project --python 3.12 --with pymupdf==1.27.2.2 --with pillow \
  python -I evaluators/unlimited_ocr2/run_unlimited_ocr2.py coverage \
  --out OUT --inputs INPUTS --pair london-page-local:london-multi-8
```

## Model code review

All snapshot files are pinned by SHA-256, and the runner refuses unreviewed
`.py` files. The 2026-10-27 review of the pinned revision found no network,
subprocess or pickle use on the inference path:

- `requests` is imported but unused.
- `torch.load` is only reachable with an explicit SAM checkpoint, which
  `from_pretrained` does not use.
- The only `eval` runs on model output in upstream's `save_results`
  visualizer, which this runner never calls.
- Environment variables only toggle diagnostics.

## Local check

```bash
uv run --no-project --python 3.12 --with pymupdf==1.27.2.2 --with pillow \
  python -I evaluators/unlimited_ocr2/run_unlimited_ocr2.py self-test
```
