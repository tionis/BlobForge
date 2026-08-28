# Converter Adapter and Rulebook Evaluation Architecture

Status: proposed implementation boundary  
Date: 2026-08-27  
Related: `mdaf_redesign.md`, `converter_evaluation.md`, MDAF v1

## Decision

Use the priority rulebooks in two roles:

1. production inputs whose best available conversions are valuable immediately;
2. a stable acceptance corpus used to compare every credible conversion recipe.

Every scored conversion produces a validated MDAF. Native output folders or
provider responses are intermediate evidence, never the comparable result. This
makes the evaluation exercise the same packaging, provenance, source-map, and
Vulcan-import contracts that production will use.

The complete books are the acceptance corpus, not automatically ground truth.
Existing Marker output is a baseline and review aid, not a gold answer. A
smaller labeled page set and blinded human review provide quality judgments.

The present program targets born-digital illustrated pen-and-paper rulebooks.
Local compatibility/enrichment recipes may explicitly require usable embedded
PDF text and need not support image-only scans. OCR-capable engines remain
valuable comparison candidates for digital layout quality, but scan-heavy
document support is a separate future recipe class rather than a promotion
gate for the current program.

## Boundary

BlobForge has three independent layers:

```text
source + frozen recipe
        |
        v
converter adapter in isolated environment
        |
        v
ConversionBundle v1
        |
        v
shared normalizer / MDAF builder / validator
        |
        v
validated <artifact-identity>.mdaf
        |
        +--> Vulcan import test
        +--> automated and blinded evaluation
```

Converter adapters do not write MDAF directly. The shared builder alone owns
canonical BLAKE3 calculation, final UTF-8 byte offsets, member declarations,
logical identity, archive safety, portable provenance, and schema validation.
This prevents backend-specific interpretations of the MDAF contract.

## Package layout

The intended core layout is:

```text
blobforge/converters/
  contract.py        request/result types and adapter protocol
  registry.py        configured local capability registry
  runner.py          fenced subprocess execution and progress
  marker1.py         compatibility adapter
  marker2.py         Marker 2 / Surya adapter
  docling.py         standard and VLM recipes
  mistral.py         Mistral OCR API adapter
  datalab.py         Datalab API adapter
  deterministic.py   Poppler/PyMuPDF controls

blobforge/mdaf/
  digest.py          tagged BLAKE3 and canonical JSON
  builder.py         directory/ZIP construction and logical identity
  schemas/           reviewed MDAF v1 schemas and fixtures
  source_map.py      final UTF-8 byte-span ledger
  provenance.py      activity DAG construction and secret checks
  validation.py      structural and semantic validation

blobforge/evaluation/
  corpus.py          immutable corpus manifest and strata
  run.py             source/recipe experiment matrix
  metrics.py         automated measurements
  report.py          blinded review bundle and comparison report
```

The first vertical slice now exists. `blobforge/converters/contract.py` validates
the versioned filesystem bundle, including confined paths, UTF-8 Markdown, NUL
rejection, unique artifact paths, tool versions, native members, mappings,
models, parameters, and diagnostics. `blobforge/converters/runner.py` executes
the adapter as a bounded subprocess and is the sole path from a bundle to the
shared MDAF builder. `blobforge evaluate
{poppler,marker1,marker2,docling,mistral} <pdf>` exposes the implemented
first-round adapters. When an adapter has no richer native hierarchy, the
shared packager derives a conservative `outline.json` from non-empty ATX
Markdown headings after all text normalization is complete. An
adapter-supplied geometry-backed outline takes precedence; the fallback never
fabricates source locators.

The environments are independently locked below `evaluators/marker1/` and
`evaluators/docling/`. Both select PyTorch's CPU-only wheel index on the 32-GiB
host; the default PyPI resolution attempted to install several GiB of irrelevant
CUDA 13 libraries and is intentionally forbidden. Marker is pinned to 1.10.2.
Docling is pinned to 2.122.0 and retains lossless Docling JSON. Both adapters
request explicit page separators and convert them to final UTF-8 page spans.
Model aliases/checksums remain a production gate even with pinned packages.

These are logical modules, not one dependency environment. Heavy local adapters
run as subprocess entrypoints in separately locked uv projects or pinned
containers. API adapters run in a small API-worker environment. The parent
loads only the contract and runner, never Docling, Marker, Paddle, and their
conflicting ML stacks together.

## Adapter contract

An adapter advertises immutable capabilities locally and implements one
operation:

```python
class ConverterAdapter(Protocol):
    def describe(self) -> ConverterDescriptor: ...
    def convert(
        self,
        request: ConversionRequest,
        progress: ProgressReporter,
    ) -> ConversionBundle: ...
```

`ConverterDescriptor` includes adapter ABI, media types, recipe identifiers,
tool/model requirements, native evidence types, mapping capability, network
requirement, and whether the adapter can run on the current host. Coordinator
data cannot supply an executable or import path; worker configuration provides
an allowlisted adapter registry.

`ConversionRequest` includes only:

- verified local source path and declared media type;
- canonical source identity and permitted alternate digests;
- frozen recipe JSON and digest;
- attempt workspace and explicit resource/spend limits;
- source metadata allowed in portable output.

Credentials, signed URLs, and private endpoint topology are process-local and
must not appear in the request, bundle, native rendition, or MDAF.

## ConversionBundle v1

The subprocess writes a private attempt directory:

```text
bundle.json                    required result manifest
text.md                        required proposed primary Markdown
assets/...                     optional files referenced by text.md
mappings.json                  optional candidates bound to text.md bytes
outline.json                   optional completely aligned candidate outline
renditions/<namespace>/...     complete sanitized native evidence
environment/...                optional lock, model manifest, or SBOM
```

The bundle is not public and is not an MDAF. `bundle.json` records:

- contract version and completion state;
- exact adapter, tool, model, and returned-model identity;
- sanitized output-affecting parameters;
- input/output digests and byte counts;
- assets and native evidence with media types and schemas;
- provider usage, estimated/returned cost, timing, warnings, and failures;
- which activity produced every member;
- mapping precision (`none`, `page`, `region`, or `polygon`) and method.

The adapter must finish all Markdown normalization that can change bytes before
emitting mappings. The shared builder may validate or copy `text.md`, but must
not silently rewrite it. Shared asset-link and Markdown-fragment helpers can be
used inside adapters so all engines follow the same policy while retaining
correct offsets.

Where a provider returns page Markdown, the adapter can emit page-level byte
spans immediately. Where a block tree is available, it can emit region mappings
for reliable blocks. Unsupported or uncertain geometry is omitted; the full
native response remains under `renditions/`.

## Isolation and dependency policy

Each local engine has its own uv lock or pinned image:

```text
evaluators/marker2/
evaluators/docling/
evaluators/mineru/
evaluators/paddle/
evaluators/olmocr/
```

The contract is a versioned JSON/filesystem ABI so these environments need not
share Python package versions. The parent process enforces timeout, disk and
output limits, progress protocol, exit classification, and cancellation. An
adapter crash cannot kill the lease owner. Network is disabled for local
adapters after model hydration; API adapters receive network access and enforce
provider-specific retry and spend limits.

## Rulebook corpus protocol

Freeze a corpus manifest containing source BLAKE3 and SHA-256, media type, byte
size, page count, language, layout strata, permission notes, and evaluation
eligibility. Filenames and paths are metadata, never identities.

Use three nested sets:

- **full corpus:** 43 exact-byte-distinct PDFs / 9,465 pages, used for whole-book
  failures, consistency, throughput, cost, artifact size, and Vulcan import;
- **adjudication set:** approximately 5-10 difficult and ordinary pages per
  source or canonical edition, labeled for text, order, headings, tables, assets, and page/region
  provenance;
- **hidden holdout:** a smaller page subset not used while tuning adapters.

Every recipe runs twice on the adjudication set. Stable, affordable recipes are
promoted to all complete books. API requests use a hard page/spend cap. A
failure produces a recorded failed attempt and never silently invokes another
engine under the same recipe.

Provider subscriptions and promotional credits can be consumed in resumable
bounded batches over time. The experiment ledger records provider usage,
notional list-price cost, actual billed cost, and credits applied separately.
This preserves a fair long-term comparison while allowing free quota to fund
the initial Mistral and Datalab corpus runs.

## Comparison outputs

The experiment key is `(source_digest, recipe_digest, repeat)`. Preserve:

- the validated MDAF and its logical identity;
- text, source-map, outline, and native-evidence capability summary;
- engine/model provenance and reproducibility warnings;
- page and document metrics, elapsed time, peak RAM/VRAM, and API cost;
- Vulcan validation/import result and generated-wiki diagnostics;
- blinded side-by-side review decisions and correction time.

Comparison views should hide producer names by default. Automated metrics
supplement rather than replace review: several legitimate Markdown renderings
can represent the same page.

## First vertical slice

Build the shared path before adding expensive engines:

1. vendor the reviewed MDAF v1 schemas and fixtures;
2. implement tagged BLAKE3, canonical parameters, MDAF writer, and validator;
3. define ConversionBundle v1 and a fixture adapter;
4. add deterministic Poppler/PyMuPDF and current Marker 1 adapters;
5. generate MDAFs for `assets/lorem.pdf` and one representative rulebook;
6. validate them with both BlobForge and `vulcan artifact validate`;
7. implement the corpus manifest and comparison result schema;
8. add Docling standard on the 32-GiB desktop;
9. add Mistral annotated API with a hard spend cap;
10. add Marker 2 and the remaining challengers through the same ABI.

This order proves the durable contract with cheap converters. No API call or
large model download is necessary until the artifacts and measurements are
already reusable.

## Acceptance gates

An adapter is usable only when:

- its MDAF validates in BlobForge and Vulcan;
- repeat runs retain honest tool/model provenance;
- native evidence is complete and secret-free;
- Markdown asset references resolve;
- published source mappings bind to the final `text.md` BLAKE3 digest and valid
  UTF-8 byte boundaries;
- cancellation, timeout, malformed output, and partial API responses fail
  closed;
- whole-book resource and spend limits are enforced;
- the adapter does not import its heavy runtime into the parent worker.
