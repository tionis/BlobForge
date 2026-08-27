# Local Converter Requirements and Complete Evaluation Matrix

Status: evaluation design  
Updated: 2026-08-27  
Corpus: 43 exact-byte-distinct PDFs / 9,465 pages / 1,234.58 MiB

## Available machines

The initially inspected BlobForge workstation has:

- Intel i7-8650U: 4 cores / 8 threads;
- 31 GiB RAM plus swap;
- 392 GiB free disk, although the filesystem is already 90% used;
- Docker and Podman installed;
- Poppler tools installed;
- no currently usable NVIDIA device (`nvidia-smi` cannot communicate with a
  driver).

The user additionally has:

- a currently Windows machine with 24 GiB RAM and a GeForce GTX 1070 (8 GiB
  VRAM, Pascal / CUDA compute capability 6.1); and
- a GPU-less desktop with 32 GiB RAM, plus weaker older laptops.

The 32-GiB desktop is the preferred full-corpus CPU worker. The GTX 1070 host is
a useful compatibility and selected-page accelerator, but not a vLLM host:
vLLM requires compute capability 7.0 or newer. Windows should remain installed
unless there is another reason to replace it; NVIDIA supports CUDA under WSL2
on Pascal and later cards. Use a pinned CUDA 12.x stack for Pascal because CUDA
13 removed offline compilation and library support for Pascal.

These hosts are adequate for correctness runs and several complete CPU
pipelines. They are not representative performance references for modern large
VLMs over 9,465 pages.
Keep at least 100 GiB free during the evaluation because models, rendered pages,
native JSON, extracted assets, and multiple MDAFs can grow well beyond the
source PDFs.

## Isolation rule

Do not upgrade the repository's `.venv`. BlobForge deliberately pins Marker
1.10.2 and Surya 0.17.1, while Marker 2, Docling, MinerU, PaddleOCR, and olmOCR
have conflicting PyTorch, Transformers, CUDA, and model dependencies.

Each candidate gets one of:

- a separate uv project and lockfile;
- an official version-pinned container; or
- a remote provider adapter with a pinned SDK/OpenAPI contract.

Every run records the lockfile/container digest, tool revision, model revision
or checksum, complete output-affecting settings, hardware, and native output.
Never use `latest` tags or mutable model aliases in a scored run.

## Marker 2

### Requirements

Marker 2 requires Python 3.10+ and PyTorch. Its Surya VLM is served separately:

- NVIDIA: Docker, NVIDIA Container Toolkit, a working driver, and the vLLM
  backend;
- CPU/Apple Silicon: the external `llama-server` executable from llama.cpp;
- remote: an OpenAI-compatible Surya server supplied through
  `SURYA_INFERENCE_URL`.

Modes are materially different recipes:

- `fast --disable_ocr`: pure text-layer extraction and small CPU layout model;
  no inference server, but scans and equations are skipped;
- `fast`: CPU-oriented layout/text extraction with selective VLM repair; it may
  launch llama.cpp on this workstation;
- `balanced`: Surya VLM layout and broader OCR; intended for a GPU;
- `balanced --force_ocr`: full-page OCR; useful as a deliberate stress recipe,
  not a default for born-digital rulebooks;
- `--use_llm`: another separate recipe and cost/reproducibility dimension.

### What can run on the available machines

`fast --disable_ocr` can run on either desktop. Ordinary `fast` and even a
selected-page `balanced` experiment are technically possible through Surya's
llama.cpp/GGUF backend. Surya 2 is only 650M parameters, and current llama.cpp
still targets Pascal compute capability 6.1, so a CUDA-12 `llama-server` under
WSL2 may offload it to the GTX 1070. This route must be treated as an experiment
until model loading, page correctness, VRAM use, and throughput are measured.

Do not use Surya's vLLM backend on the GTX 1070: vLLM requires compute
capability 7.0+, while the card is 6.1. If llama.cpp GPU offload fails, it can
still run on the 32-GiB desktop's CPU, slowly. A remote Surya server remains the
fair way to measure production-scale Marker 2 balanced throughput.

Use a separate uv project, conceptually:

```bash
mkdir marker2-eval
cd marker2-eval
uv init --python 3.12
uv add 'marker-pdf==2.0.0'
uv run marker_single INPUT.pdf --mode fast --disable_ocr \
  --output_format json --output_dir OUTPUT
```

For CPU VLM use, install a pinned llama.cpp `llama-server` release outside the
Python environment and record its checksum. For NVIDIA use, install NVIDIA
Container Toolkit and point the adapter at the pinned Surya inference server.

The scored adapter must retain Marker JSON/debug evidence with page/block
geometry as well as final Markdown and assets. A CLI run that keeps only
Markdown is insufficient for MDAF evaluation.

### Licensing

Marker code is Apache-2.0. Its model weights use a modified OpenRAIL license
with commercial thresholds. Approve the exact deployed model license before
making it a production default.

Official references:

- <https://github.com/datalab-to/marker/releases/tag/v2.0.0>
- <https://github.com/datalab-to/marker>
- <https://github.com/datalab-to/surya/releases>
- <https://github.com/ggml-org/llama.cpp/blob/master/docs/build.md>
- <https://docs.vllm.ai/en/latest/getting_started/installation/gpu.html>

## Docling

### Requirements

Docling supports Linux x86-64 and installs through uv. The standard pipeline can
run on CPU; CUDA accelerates layout, tables, and OCR. Optional extras provide
VLM and alternate OCR engines.

An isolated standard environment is conceptually:

```bash
mkdir docling-eval
cd docling-eval
uv init --python 3.12
uv add docling
uv run docling convert INPUT.pdf --pipeline standard \
  --to md --to json --image-export-mode referenced --output OUTPUT
```

For VLM experiments, add the `vlm` extra and select an explicit preset:

```bash
uv add 'docling[vlm]'
uv run docling convert INPUT.pdf --pipeline vlm \
  --vlm-model granite_docling --to md --to json \
  --image-export-mode referenced --output OUTPUT
```

The current CLI exposes presets for GraniteDocling, SmolDocling, DeepSeek OCR,
Chandra OCR 2, dots.ocr, and several other VLMs. Each preset is a distinct
recipe with its own model license and hardware requirement.

### What can run on the available machines

The standard CPU pipeline is the best first full-corpus local run on the
32-GiB desktop. It should emit both Markdown and the lossless `DoclingDocument`
JSON. That JSON carries hierarchy, reading order, page geometry, bounding boxes,
tables, pictures, and provenance, making it especially suitable for MDAF source
maps.

The GTX 1070 is worth testing with Docling standard CUDA acceleration and the
small GraniteDocling-258M and SmolDocling-256M VLM presets. Their size is
appropriate for 8 GiB VRAM, although the actual current PyTorch/CUDA build must
first be verified on Pascal. Run this in WSL2 with a pinned CUDA 12.x-compatible
environment, or try native Windows if the locked dependency set supports it.
Docling also supports Ollama and LM Studio endpoints on Windows, creating a
second route for compatible quantized models without vLLM.

Do not force OCR across this corpus by default: all PDFs have text layers. Let
the standard pipeline OCR bitmap regions and add a force-OCR recipe only for
known problem pages.

### Licensing

Docling code is MIT. Models have their own licenses and must be reviewed per
preset.

Official references:

- <https://docling-project.github.io/docling/getting_started/installation/>
- <https://docling-project.github.io/docling/reference/cli/>
- <https://docling-project.github.io/docling/concepts/docling_document/>
- <https://github.com/docling-project/docling/blob/main/docs/usage/model_catalog.md>
- <https://github.com/docling-project/docling/blob/main/docs/usage/gpu.md>

## Hardware strategy

No new hardware is required for the first decision round:

- run Docling standard, Marker 2 no-OCR, MinerU pipeline, PP-StructureV3, and
  deterministic baselines on the 32-GiB desktop;
- use the GTX 1070 for Docling standard CUDA, GraniteDocling/SmolDocling, and a
  Surya llama.cpp selected-page feasibility probe;
- use Mistral and other hosted trials for heavyweight quality controls.

This covers deterministic text extraction, classical layout/OCR, a small local
VLM, Marker/Surya, and strong hosted OCR. Only if the results justify it, rent a
Linux NVIDIA worker with:

- 48 GiB VRAM minimum; 80 GiB is the safest common denominator for Chandra 2,
  larger Docling presets, and comfortable vLLM concurrency;
- 64-128 GiB system RAM;
- 250-500 GiB free SSD;
- a current NVIDIA driver and Docker + NVIDIA Container Toolkit;
- outbound model-registry access during a controlled hydrate step;
- immutable model snapshots/checksums captured before scoring.

The rented host is an optional heavyweight-model round, not a prerequisite for
choosing the BlobForge/MDAF architecture. The GTX 1070 cannot run the documented
olmOCR or PaddleOCR-VL minimum configurations, both of which start at 12 GiB.
Use their hosted endpoints or defer them unless the first round shows that the
missing model class matters.

## Candidate matrix

### Full-corpus candidates

These are sufficiently credible and sufficiently different to justify all
9,465 pages if setup succeeds.

| Candidate/recipe | Where to run | Native evidence for MDAF | Priority |
| --- | --- | --- | --- |
| Existing Marker 1 output | already present | old metadata, limited mapping | baseline |
| Marker 2 fast/no-OCR | current CPU | JSON blocks/geometry; misses OCR/math | baseline/control |
| Marker 2 balanced | GTX 1070 llama.cpp probe; remote for scale | JSON/debug blocks, pages, assets | primary |
| Docling standard | 32-GiB CPU desktop; GTX 1070 comparison | lossless document JSON with provenance | primary |
| Docling GraniteDocling VLM | GTX 1070 selected pages, then corpus if stable | Docling JSON plus VLM output | primary |
| MinerU pipeline | current CPU, likely slow | middle/model/content JSON, spans, assets | primary challenger |
| MinerU hybrid/VLM | GPU | native middle/model JSON and Markdown | primary challenger |
| PaddleOCR PP-StructureV3 | CPU correctness, GPU throughput | structured layout/OCR/table results | primary challenger |
| PaddleOCR-VL | GPU, 12 GiB documented minimum | structured document-parser output | primary challenger |
| olmOCR | GPU, 12 GiB+ and 30 GiB disk | page-oriented Markdown/metadata | quality challenger |
| Mistral OCR 4.1 | hosted API | Markdown, blocks, boxes, confidence, images | primary API |
| Datalab Convert accurate | hosted API | Markdown/JSON/HTML, blocks depending options | primary API |

MinerU documents a pure-CPU `pipeline` backend with 16 GiB minimum / 32 GiB
recommended RAM and about 20 GiB disk. Its GPU/hybrid modes require more. It
produces intermediate JSON and content lists that are useful MDAF renditions.

PaddleOCR's PP-StructureV3 can fall back to CPU. PaddleOCR-VL supports x64 CPU
but its practical full-corpus path is GPU; the project reports an RTX 3060 12
GiB as the smallest successfully tested NVIDIA configuration.

olmOCR documents a recent NVIDIA GPU with at least 12 GiB VRAM and 30 GiB disk;
it can also use a remote vLLM server. Its own benchmark suite is useful as one
generic metric layer in addition to the rulebook gold set.

Official references:

- <https://github.com/opendatalab/MinerU/blob/master/docs/en/quick_start/index.md>
- <https://github.com/PaddlePaddle/PaddleOCR/blob/main/docs/version3.x/pipeline_usage/PP-StructureV3.en.md>
- <https://github.com/PaddlePaddle/PaddleOCR/blob/main/docs/version3.x/pipeline_usage/PaddleOCR-VL.md>
- <https://github.com/allenai/olmocr>

### Selected-page VLM challengers

Run these first on the deeply annotated hard-page set. Promote to the full corpus
only if they beat a primary candidate or fill a unique failure mode.

| Candidate | Why evaluate | Main constraint |
| --- | --- | --- |
| Chandra OCR 2 self-hosted | strong complex tables/math/scans/multilingual claim | 9B-class model, high VRAM, restrictive weight/output license |
| DeepSeek OCR via Docling | independent VLM architecture | GPU/runtime complexity; validate model license and geometry |
| dots.ocr via Docling | strong published document benchmark challenger | GPU and model/runtime maturity |
| SmolDocling | small local VLM control | likely lower ceiling, useful efficiency point |
| Unstructured `hi_res` | established element/layout ecosystem | dependency complexity and slower PDF path |
| Surya OCR 2 directly | isolates Marker's OCR/layout foundation | not a full Markdown pipeline by itself |

Chandra's managed API and open weights are different candidates. Do not infer
the hosted service's output, corrections, licensing, or reproducibility from the
open model.

### Deterministic baselines

These are cheap and diagnostically important even though they are unlikely to
win overall:

- Poppler `pdftotext -layout`;
- PyMuPDF/PyMuPDF4LLM;
- OCRmyPDF + Tesseract for scan/OCR-layer repair;
- LiteParse or an equivalent pure text-layer Markdown baseline.

They establish whether a model improves semantic structure or merely changes
formatting, and provide fallbacks when an ML engine drops text.

### Hosted commercial controls

| Service | Useful output | 9,465-page planning note |
| --- | --- | --- |
| Mistral OCR 4.1 | page Markdown, blocks/boxes, confidence, images | $37.86 standard / $47.33 annotated ceiling |
| Datalab Convert/Chandra | Markdown/JSON/HTML and high-quality managed VLM | use $5 trial subset, then dashboard quote |
| Google Document AI Layout | structured layout/chunks | $30.60 at $10/1,000 pages |
| AWS Textract Layout + Tables | lines/words, reading-order layout, geometry, confidence, tables | about $45.90 at $0.015/page in US West example |
| Azure Document Intelligence Layout | Markdown/structure/geometry depending API version | calculate by selected region/version before run |
| Adobe PDF Extract | text, tables, figures, reading order and bounds | useful born-digital control; verify current quota/terms |
| Mathpix | strong math/STEM OCR and Markdown | selected math/table pages first |
| LlamaParse | managed multimodal parsing and Markdown | selected hard pages, then obtain exact mode price |
| Unstructured API | hosted partition/layout ecosystem | selected hard pages; retain complete element JSON |

AWS Textract is a structured OCR/layout control rather than a ready-to-use
Markdown producer. Its geometry and confidence fit MDAF, but BlobForge must
render the returned block graph deterministically. Generic Gemini, Claude, and
OpenAI vision calls belong only in the hard-page challenger set unless wrapped
in a pinned prompt/schema and span-preserving adapter; raw chat output offers
weak source-map and reproducibility guarantees.

Official references:

- <https://docs.mistral.ai/models/ocr-4-0>
- <https://documentation.datalab.to/docs/recipes/conversion/conversion-api-overview>
- <https://cloud.google.com/products/document-ai/pricing>
- <https://aws.amazon.com/textract/pricing/>

## Evaluation order

### Stage 1: no new GPU

Run the full corpus through:

1. existing Marker 1 baseline;
2. Poppler/PyMuPDF deterministic baselines;
3. Docling standard CPU;
4. Marker 2 fast/no-OCR CPU;
5. MinerU pipeline CPU;
6. PP-StructureV3 CPU on the hard-page set, expanding if runtime is acceptable;
7. Mistral OCR 4.1 API.

This stage determines whether a GPU/VLM is needed on most born-digital pages and
builds the source-map adapters before costly experiments.

### Stage 2: use the GTX 1070

Under WSL2 with pinned CUDA 12.x, test Docling standard CUDA,
GraniteDocling/SmolDocling, and Surya's llama.cpp backend on the adjudication
pages. Record whether each dependency still ships Pascal kernels, peak VRAM,
page latency, failures, and output quality. Promote stable winners to a full
corpus run only when the measured runtime is reasonable.

### Stage 3: optional universal GPU host

Only if needed, run Marker 2 vLLM, MinerU hybrid/VLM, PaddleOCR-VL, and olmOCR.
Gate Chandra 2, DeepSeek OCR, dots.ocr, and other Docling presets on the
hard-page set; only promote a challenger that adds unique wins.

### Stage 4: commercial challengers

Use Datalab's trial subset, then approve a full run only with an exact price.
Add Google Layout and AWS Textract if their structured geometry is competitive.
Use Azure, Adobe, Mathpix, LlamaParse, and Unstructured only when the hard-page
results justify adapter work.

## Fair-run requirements

Every candidate must:

- receive the same source bytes and page set;
- run under a frozen recipe and immutable model version;
- emit final Markdown, assets, native evidence, timing, resource use, and errors;
- preserve page/block geometry when available;
- package directly into a validated MDAF so no winning result is discarded;
- run twice on the stability subset;
- be scored blindly against the existing Markdown and human gold set;
- stay within a per-run time/spend cap;
- report failures instead of silently falling back to a different model.

“All options” should mean all credible architecture families and every candidate
that survives the hard-page gate. It should not mean integrating dozens of
nearly identical SaaS wrappers before any quality evidence exists.
