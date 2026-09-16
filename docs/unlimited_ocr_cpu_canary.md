# Unlimited-OCR CPU RAM Canary

Status: completed; page-local CPU feasibility accepted, broader evaluation pending  
Executed: 2026-09-09

## Outcome

Unlimited-OCR can run page-locally on the 32 GiB desktop without a GPU. The
smaller community Q4_K_M recipe completed a dense, illustrated two-column page
in 144-152 seconds with 7.12 GiB peak resident memory and zero process swap in
the explicitly instrumented run. Q8_0 completed in 156-158 seconds with 7.33
GiB peak resident memory.

This is a useful adapter-development and quality-screen path, not a production
CPU worker. It neither validates the official BF16/vLLM recipe nor supports a
corpus throughput claim. The next meaningful quality decision remains a bounded
page-local and multi-page canary on a modern NVIDIA GPU.

## Frozen inputs and runtime

The canary used source PDF page 4 (printed page 5, "Making Copies") from
the preserved Storypath review source. The page has a full-width callout, two
body columns, decorative list bullets, colored panels, background art, a footer,
and a page number. It was rendered as a 2550 x 3300 RGB PNG at 300 DPI.

| Evidence | Identity |
| --- | --- |
| Source PDF | SHA-256 `d0463c98e081eaca662e35fd2eca3717a6ef4901d611fdfef0d50f8b0cb2d2e5`; BLAKE3 `27007958023e174e1b06d109644b19c84df6f5f3b8ae3738071b912fb464ea62` |
| Rendered page | SHA-256 `064cbc8dca03ae1913d7d946f735130e42cb465ae1236a1b21261f017796827f`; BLAKE3 `67c35986bcfa4cfe092a68c1f49baedb4b902e58d734888858e581a687f3b2b1` |
| Community GGUF repository | `sabafallah/Unlimited-OCR-GGUF` revision `99bac69ae80ff4269bac7217649e452e99e2b1f6` |
| Q8_0 language model | 3,126,139,712 bytes; SHA-256 `dc7848fcd03807dfd382462a58a8ea3659b2e4426f92ef9b7d8e2205153da29f` |
| Q4_K_M language model | 1,950,326,592 bytes; SHA-256 `9953a7b064f3bec4edb7b8934db7d27b854774471a60c350067f11c4e58189fe` |
| Q8_0 vision projector | 461,226,464 bytes; SHA-256 `04c7060ff02aa6c97589fb5a1a7a26b1127a8ae1e4e548d3425666fe5fc56e4b` |
| llama.cpp runtime | PR 24975 branch commit `a42f938f40d5665fd362cac5d3fc914040233f91`; `llama-mtmd-cli` 0.4.0-dev build 1 |
| Host | Debian Linux 6.12.96; Intel i7-8650U, four cores/eight threads; 31 GiB visible RAM |

The runtime was built CPU-only with native host optimizations. Both recipes used
the same Q8 vision projector, prompt `document parsing.`, DeepSeek-OCR chat
template, temperature zero, four inference/batch threads, 16,384-token context,
4,096-token output ceiling, flash attention disabled, no warmup, and identical
DRY repetition controls. llama.cpp promoted the R-SWA value cache to F32 as its
implementation recommends for dense tables. A 30-minute watchdog bounded each
run. The PDF never left the host; network access was used only to fetch the
pinned runtime source and GGUF artifacts.

The GGUF files are a third-party conversion of Baidu's model and the runtime is
an open, experimental llama.cpp pull request. These identities are therefore
separate extraction recipes from Baidu's official BF16 weights and vLLM image.

## Results

| Recipe | Repeats | Wall time | Peak RSS | Peak process swap | Raw-output repeatability |
| --- | ---: | ---: | ---: | ---: | --- |
| Q8_0 LM + Q8_0 projector | 2 | 156, 158 s | 7.33 GiB | not sampled | byte-identical, SHA-256 `4d7de796cf53707e2775572cd557f1d97b0def20d70c398810dc7b43e99d38b3` |
| Q4_K_M LM + Q8_0 projector | 3 | 144, 152, 145 s | 7.12 GiB | 0 KiB in the instrumented third run | byte-identical, SHA-256 `2bb2a16c3788a761052c2dfe191b89613600d36ecd6a451f04000cf2b286b033` |

Q4 reduced the combined language-model/projector storage from 3.34 GiB to 2.25
GiB and averaged 147 seconds versus Q8's 157 seconds. It reduced observed peak
RSS by only about 0.21 GiB. For this page the vision encoding and F32 R-SWA cache
substantially limit the RAM benefit of quantizing the language model further.

Both recipes emitted 16 grounded regions in the correct visual reading order:
two titles, twelve text blocks, one footer, and one page number. Their normalized
transcriptions were identical. Q4 changed 14 of the 64 box coordinates relative
to Q8, with mean absolute movement 0.234 and maximum movement 2 on the model's
0..999 coordinate scale. One dash style also differed in raw output.

## Text comparison

Poppler `pdftotext -raw` supplied comparison evidence from the embedded PDF
text; it is not a manually transcribed gold set. Poppler emitted a duplicate
footer overlay (`Making Copies 5`), which the scorer explicitly excluded. The
model's visual block order was rotated to Poppler's raw extraction order only
for CER/WER. Scoring then applied NFKC, case folding, smart-punctuation folding,
lowercase line-wrap dehyphenation, and whitespace collapse.

For both Q8 and Q4:

- order-adjusted character error rate was 0.183% (8 edits / 4,373 reference
  characters);
- order-adjusted word error rate was 0.546% (4 edits / 733 reference words);
- order-independent word precision/recall/F1 was 1.0000 / 0.9945 / 0.9973; and
- all four word edits were Poppler's `Y` substitutions for the same decorative
  diamond bullet. Unlimited-OCR retained each list label and body but omitted
  the bullet glyph itself.

There was no truncation, terminal loss, repetition loop, invented prose, or
column interleaving on this page. The bullet omission still matters: native
grounding must be retained and a future clean projection may infer list syntax
only from explicit, reviewable layout evidence.

## Comparison with retained extractors

This page is page key 3 in the completed eight-page Storypath review campaign,
so the retained Marker 1.10.2, Mistral OCR 4.1, Datalab Convert Accurate and
Docling 2.122.0 outputs can be compared against the same Poppler evidence. A
separate reproducible scorer removes Markdown presentation markers and applies
the same text normalization used above. It does not score formatting, geometry,
assets or semantic structure.

| Extractor | CER | WER | Order-independent word F1 |
| --- | ---: | ---: | ---: |
| Mistral OCR 4.1 | **0.091%** | **0.546%** | 0.9945 |
| Unlimited-OCR Q4_K_M | 0.183% | **0.546%** | **0.9973** |
| Marker 1.10.2 | 0.389% | 0.682% | 0.9959 |
| Docling 2.122.0 | 0.389% | 0.682% | 0.9959 |
| Datalab Convert Accurate | 0.457% | 0.955% | 0.9925 |

These small differences do not establish a general transcription ranking.
Mistral retained a footer/page string which Marker, Docling and Datalab omitted,
and the reference exposes the decorative diamond as `Y`: Mistral and Datalab
substitute useful diamond glyphs, Marker and Docling retain `Y`, and
Unlimited-OCR omits the glyph. Apart from those presentation details and one
spacing difference, all five retained the page's prose very well.
Unlimited-OCR's superficially highest bag-of-words F1 rewards deletion over a
nonmatching symbol; it is not evidence that losing the bullets is preferable.

The completed blinded eight-page review—not the new one-page automated
comparison—remains the stronger overall quality evidence:

| Dimension | Marker 1 | Mistral | Datalab | Docling | Unlimited-OCR |
| --- | ---: | ---: | ---: | ---: | --- |
| Text | 4.625 | **5.000** | **5.000** | 4.625 | not blindly scored |
| Inline formatting | **5.000** | **5.000** | **5.000** | 3.000 | plain text in this canary |
| Reading order | 5.000 | 5.000 | 5.000 | 5.000 | correct on one page |
| Lists | **5.000** | 4.750 | 4.500 | **5.000** | four separate text blocks; no bullets/list syntax |
| Assets | 4.143 | **4.857** | 3.714 | 4.714 | not extracted |
| Wiki utility | 4.000 | **5.000** | 3.000 | 4.000 | not packaged or scored |

Unlimited-OCR preserves stronger raw page geometry than plain Markdown alone:
all 16 regions have normalized boxes and the model distinguishes titles, body,
footer and page number. It does not express callout containment, heading levels,
inline italics/bold, or list semantics. Mistral remains the demonstrated quality
leader because it combines excellent text with inline formatting, useful glyph
decoding, assets, page mappings and a validated MDAF. Unlimited-OCR is presently
a promising grounded transcription challenger, not the best complete extractor.

Throughput comparisons also favor every established path, although the runs
were not synchronized and full-document engines amortize startup across eight
pages:

| Extractor/run | Observed elapsed | Approximate average |
| --- | ---: | ---: |
| Poppler, eight pages | 0.7 s | 0.09 s/page |
| Mistral, eight pages | 6.9 s | 0.86 s/page |
| Datalab provider processing, eight pages | 13.12 s | 1.64 s/page |
| Docling CPU, eight pages | 269.3 s | 33.7 s/page |
| Marker 1 CPU, eight pages | 519.2 s | 64.9 s/page |
| Unlimited-OCR Q4 CPU, one page | 144-152 s | 147 s/page mean |

Marker and Docling also completed the same book in 100.8 and 115.0 seconds on a
faster enterprise CPU host, reinforcing that these are deployment observations,
not intrinsic model speeds. Unlimited-OCR's local privacy and zero API charge
match Marker/Docling, but its current CPU recipe is materially slower. Mistral
cost $0.032 list price for this eight-page book; Datalab billed $0.06.

## Evidence location and limitations

The ignored local evidence tree is
`.blobforge-migration/evaluations/unlimited-ocr-cpu-q8-storypath-page-4/`. It
contains the input raster, pinned models, run scripts, raw generations, stderr,
RSS/swap metrics, untouched Poppler references, and JSON scoring reports,
including `output/extractor-comparison.json`. Large
model and evaluation artifacts remain outside Git by repository policy; this
document preserves their identities and evaluation method.

This single born-digital page is deliberately only a feasibility gate. It does
not establish scan-heavy OCR, tables, equations, German text, image crops,
multi-page `<PAGE>` reconciliation, MDAF validity, GPU throughput, cancellation,
or worker recovery. Do not route jobs to this recipe or extrapolate roughly 2.5
minutes per page across the 9,465-page corpus.

## Decision

The RAM-only probe is complete. Keep Q4_K_M plus the Q8 projector as the cheaper
CPU smoke-test recipe because its transcription matched Q8 on this page, its
artifact footprint is smaller, and it demonstrated zero process swap. Keep Q8
as comparison evidence; this one page is insufficient to claim that Q4 and Q8
are generally quality-equivalent.

The production evaluator remains open. It must preserve the raw grounding and
raster transform, validate ConversionBundle/MDAF output, and run the broader
hard-page and bounded multi-page GPU campaign defined in
`unlimited_ocr_evaluation.md`.

## References

- <https://github.com/baidu/Unlimited-OCR>
- <https://huggingface.co/baidu/Unlimited-OCR>
- <https://huggingface.co/sabafallah/Unlimited-OCR-GGUF/tree/99bac69ae80ff4269bac7217649e452e99e2b1f6>
- <https://github.com/ggml-org/llama.cpp/pull/24975>
