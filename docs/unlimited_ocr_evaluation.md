# Baidu Unlimited-OCR Candidate Assessment

Status: RAM-only canary complete; bounded GPU evaluation accepted; no production recipe yet  
Assessed: 2026-09-09

## Decision

Add Baidu Unlimited-OCR to the conversion matrix as a primary self-hosted GPU
challenger. It is not a substitute for the current Mistral recipe without
evaluation, and its multi-page mode must not be represented as an unbounded
whole-book converter.

The first implementation should be an isolated evaluator and should compare two
separately identified recipes:

1. page-local grounded parsing, which gives the clearest failure isolation and
   page mapping; and
2. bounded multi-page parsing, which exercises the model's distinctive
   long-horizon behavior but requires exact `<PAGE>` reconciliation and a safe
   fallback to smaller chunks.

Only a blinded canary on the existing hard-page set and complete representative
books can decide whether either recipe should enter production routing.

## Why it belongs in the matrix

- It is an open-weight, MIT-licensed 3B BF16 document model rather than a paid
  document API. The current Hugging Face repository is about 6.78 GB.
- It emits Markdown-like text with layout grounding tags and normalized
  coordinates. Multi-page output has explicit `<PAGE>` separators.
- Its Reference Sliding Window Attention keeps generated-token KV state bounded,
  and the authors report one-shot parsing across dozens of pages within a 32K
  context. This is directly relevant to long illustrated rulebooks.
- Self-hosting keeps source PDFs inside the worker trust boundary and replaces
  per-page provider spend with GPU capacity and operating cost.

"Unlimited" describes the attention/cache design, not BlobForge job semantics.
The released model still has a 32,768-token context, can truncate or repeat, and
cannot safely process an arbitrary book as one indivisible request.

## Architectural fit

Unlimited-OCR belongs in its own local GPU worker boundary, not the hosted API
worker image and not the coordinator environment. The official deployment path
uses a dedicated vLLM image because the architecture is not in a stable vLLM
wheel. The model also loads custom repository code, so both the runtime image and
model/code revision must be immutable and reviewed before a scored run.

The evaluator should emit ConversionBundle v1 and retain:

- the exact raw generation, including `<|ref|>`, `<|det|>`, and `<PAGE>` tokens;
- the source PDF digest and every rendered-page digest, dimensions, DPI, and
  renderer identity;
- the model repository commit, weight digests, runtime image digest, inference
  engine version, prompt, image mode, chunk size, token limit, temperature, and
  n-gram processor settings;
- parsed block type, text, page number, and original normalized bounding box;
- generated image crops as derived assets, while clearly distinguishing them
  from original embedded PDF assets; and
- elapsed time, peak RAM/VRAM, generated tokens, truncation/repetition checks,
  and per-page diagnostics.

Map valid 0..999 grounding coordinates to PDF-page coordinates only through the
recorded raster transform. Invalid, missing, overlapping, or unreconciled boxes
remain native evidence and must not create precise MDAF selectors. The clean
Markdown projection may remove grounding wrappers, but the native rendition is
immutable.

Unlike Mistral OCR, the released interface does not provide calibrated block
confidence or authoritative billing usage. BlobForge must not invent either.

## Practical self-hosting requirements

For a smoke test, use a Linux host with one modern NVIDIA GPU, a working NVIDIA
driver/container runtime, and the dedicated `vllm/vllm-openai:unlimited-ocr`
image. The official recipe says 8 GiB VRAM is sufficient for BF16 inference;
current vLLM requires NVIDIA compute capability 7.5 or newer. BlobForge should
prefer Ampere or newer for the released BF16 path. The GTX 1070 is compute
capability 6.1 and therefore cannot use this supported vLLM route.

Practical, deliberately conservative sizing for our corpus is:

- 12-16 GiB VRAM for page-local and small multi-page canaries at concurrency
  one;
- 24 GiB VRAM for comfortable bounded multi-page experiments and operational
  headroom;
- 32 GiB host RAM; and
- at least 40 GiB free SSD space for the 6.78 GB model repository, inference
  image/layers, rendered pages, caches, native output, and MDAF staging.

The host also needs Docker or Podman, NVIDIA Container Toolkit/CDI integration,
outbound network access for the initial image/model download, and a persistent
Hugging Face model cache. Runtime access can then be restricted to the private
BlobForge network. A public Hugging Face token is not normally required for the
public model, but all image and model references must be replaced by immutable
digests/revisions before a scored or production run.

The model server exposes an OpenAI-compatible HTTP endpoint. A separate small
BlobForge adapter should render PDF pages, submit image requests, retain raw
grounded output, enforce page/truncation checks, and emit ConversionBundle v1.
Do not place the model dependencies in BlobForge's main uv environment.

### RAM-only feasibility

A page-local CPU proof-of-concept fits in the 32 GiB desktop's system RAM, but
it is not a useful full-corpus worker today.

The 2026-09-09 measured canary confirms that conclusion. On an i7-8650U, a
pinned community Q4_K_M language model plus Q8 vision projector used 7.12 GiB
peak RSS, zero process swap, and 144-152 seconds for a dense 300-DPI page. Q8_0
used 7.33 GiB and 156-158 seconds. Both were deterministic within their recipe
and produced identical normalized text; their only reference word differences
were four omitted decorative bullets. Q4 saved substantial model storage but
only about 0.21 GiB peak RSS because vision encoding and the F32 R-SWA cache
dominated. See `unlimited_ocr_cpu_canary.md` for exact identities, scoring and
limitations.

There are two distinct experimental recipes:

1. The upstream BF16 repository is 6.78 GB and fits in 32 GiB with runtime
   headroom. Its published Transformers code is CUDA-specific, including
   hard-coded device operations. Community testing reports successful CPU
   inference after a device-portability patch, but about 550 seconds for one
   dense page on an M1 Pro. Any such patch becomes part of the pinned recipe.
2. Community GGUF conversions offer, for example, a 1.82 GiB Q4_K_M language
   model or 2.91 GiB Q8_0 model. The measured canary paired each with the same
   440 MiB Q8_0 vision projector.
   These can run wholly in system RAM through a DeepSeek-OCR-aware llama.cpp
   build. Support is not yet merged into stock llama.cpp, and quantization may
   change OCR accuracy, so Q4/Q8 results are separate recipes and cannot validate
   the official BF16 path.

Use the CPU path for one-page adapter development, raw-output inspection and a
small quality screen. Do not use it for the 9,465-page campaign, multi-page
throughput claims, or production routing without measured evidence. A custom
llama.cpp CUDA build might additionally offload layers to the GTX 1070, but that
Pascal path is unverified and must not be assumed to work.

## Frozen recipe dimensions

The following alter output and therefore belong in recipe identity:

- model revision and exact weight set;
- vLLM/SGLang/Transformers runtime and container digest;
- single-page `gundam` versus multi-page `base` image mode;
- PDF renderer, DPI, color conversion, and page grouping;
- prompt text and special-token handling;
- maximum generated tokens, temperature, and stop behavior;
- no-repeat n-gram size/window; and
- chunk size and oversize/truncation fallback policy.

Page-local and multi-page modes are different extraction recipes. A later
normalization improvement may reuse retained native output only when the normal
recipe-lifecycle compatibility rules permit it.

## Canary and promotion gates

1. Pin and review the exact model/code commit and dedicated inference image.
2. Run offline malicious-model-code and dependency review before enabling
   `trust_remote_code`; never allow the inference container coordinator
   credentials or unrestricted egress.
3. Probe the existing 8 GiB GTX 1070 only if a compatible non-BF16 runtime is
   demonstrated. The documented vLLM path requires BF16 and a modern CUDA GPU,
   so the current Pascal host is not the planned benchmark target.
4. On a suitable temporary GPU, run page-local mode twice on the annotated hard
   pages, including scan-heavy, multi-column, table, equation, German, decorative,
   and image-heavy strata.
5. Run bounded multi-page mode at 2, 8, and 20 pages on representative contiguous
   sequences. Require exact page count/order, no truncation, and deterministic
   source-map reconciliation; halve failed chunks until page-local fallback.
6. Build and independently validate MDAFs, then add the outputs to a newly
   blinded campaign against Mistral wiki, Datalab wiki, and the best local
   structured candidate.
7. Measure GPU-hours, energy where available, peak VRAM, throughput, artifact
   size, failure rate, repeatability, and operational recovery. Do not extrapolate
   the paper's throughput benchmark to the rulebook corpus.
8. Promote only if it supplies a quality/privacy/cost Pareto point. Production
   rollout still needs an exact capability, concurrency-one canary, resource
   admission, cancellation, lease recovery, and rollback coverage.

## References

- <https://github.com/baidu/Unlimited-OCR>
- <https://huggingface.co/baidu/Unlimited-OCR>
- <https://arxiv.org/abs/2606.23050>
- <https://recipes.vllm.ai/baidu/Unlimited-OCR>
- <https://github.com/baidu/Unlimited-OCR/issues/81>
- <https://huggingface.co/sabafallah/Unlimited-OCR-GGUF>
- <https://github.com/ggml-org/llama.cpp/pull/24975>
