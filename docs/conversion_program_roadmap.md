# Conversion and Enrichment Program Roadmap

Status: proposed execution plan

Date: 2026-08-27

Related: `pdf_enrichment_pipeline.md`, `converter_adapter_architecture.md`,
`converter_evaluation.md`, `local_mdaf_migration.md`, `mdaf_redesign.md`

## Objective

Turn BlobForge into a recipe-driven, source-neutral artifact producer while
improving the already converted PDF collection. The program must produce
validated MDAF v1 artifacts with honest provenance, useful
Markdown-to-source mappings, and enough evidence for Vulcan to build and
maintain rulebook wikis.

The program has four outcomes:

1. enrich the 1,377 legacy conversions with the strongest defensible PDF
   location and structure evidence available;
2. implement interchangeable local and hosted conversion recipes;
3. compare them on a frozen rulebook corpus using repeatable automated and
   blinded human evaluation;
4. deploy routing that selects an appropriate recipe per source while retaining
   an administrator override.

`TODO.md` is the status authority. This document records ordering,
dependencies, deliverables, and completion gates.

## Current baseline

- The local migration mirror contains 1,808 PDF sources and 1,377 paired legacy
  Marker ZIPs.
- All paired ZIPs are conservative MDAF v1 artifacts retaining old Markdown,
  assets, metadata, unavailable historical Marker/model versions, and any
  trustworthy page-anchor or exact TOC-heading evidence.
- That migration preserved evidence; it was not complete PDF post-processing.
  Most Markdown spans still lack page or region mappings.
- The frozen 43-document rulebook corpus has 9,465 pages. It is both a
  production-priority corpus and stable acceptance corpus, but still needs a
  smaller labeled adjudication set.
- The converter subprocess ABI, ConversionBundle v1, shared MDAF builder and
  validator, initial adapters, exact recipe selection, and recipe-aware claims
  already exist.
- The modular Poppler/Markdown enrichment vertical slice and resumable local
  derived-artifact catalog are implemented. Its first 10-rulebook/153-page
  automated canary is structurally valid. Manual mapping adjudication rejected
  the first recipe because coarse regions, future-anchor jumps, and repeated
  labels admitted unsupported mappings; a corrected recipe and repeat review
  remain the Phase 1 gate before the full legacy backfill.

## Governing decisions

### Enrichment is a versioned transformation

Legacy post-processing is not an in-place metadata repair. It consumes an exact
source PDF and base artifact and produces a new immutable MDAF. Its recipe
includes the evidence extractor, alignment algorithm, normalization,
confidence, outline, and packaging policies.

Existing conservative MDAFs remain available for comparison and rollback. The
historical converter versions remain `unavailable`; current alignment must not
be presented as evidence about how Marker originally generated the Markdown.

### Conversion and enrichment are composable

```text
source + frozen recipe
        |
        +--> converter-native output/evidence
        +--> source-specific evidence extraction
                           |
                 normalized evidence model
                           |
             normalization and alignment
                           |
                outline and references
                           |
               MDAF build and validation
                           |
                      publication
```

A converter with trustworthy native geometry supplies it directly. A
Markdown-only converter uses PDF alignment. Both normalize into the same
evidence contract and shared builder. Sanitized backend-native responses remain
MDAF renditions so information outside the normalized v1 model is not lost.

### Evaluation precedes default selection

Marker 1 is a baseline, not ground truth. Quality dimensions, resource use,
privacy, reproducibility, failures, and cost remain visible instead of being
hidden in one score. Different source classes may select different recipes.

## Execution phases

### Phase 0: Contract and rubric freeze

Deliverables:

- approved enrichment contract in `pdf_enrichment_pipeline.md`;
- versioned normalized evidence model and stage interfaces;
- canonical experimental recipe documents;
- hard-page adjudication set, hidden holdout, and API-rights classification;
- frozen automated metrics and blinded review rubric.

Exit gate: a fixture travels from source plus synthetic evidence through
normalization, mapping, MDAF validation, and evaluation without
backend-specific behavior in the packager.

### Phase 1: Legacy enrichment and backfill

Implement PDF evidence extraction, Markdown segmentation, alignment, outline
generation, diagnostics, and resumable derived-artifact production. Start with
10-20 complete born-digital rulebooks representing difficult corpus strata.
The legacy/Marker compatibility recipes target digitally generated PDFs with
usable embedded text; scan-heavy PDFs require a separate future OCR recipe and
do not block this phase.

The canary must establish that final byte spans, sampled page/region mappings,
reading order, page furniture, ambiguity reporting, provenance, repeatability,
restart behavior, and independent BlobForge/Vulcan validation are correct.

Exit gate: freeze `pdf-enrichment/v1` only after agreed coverage and accuracy
thresholds pass. Then process all 1,377 base artifacts without overwriting them.

### Phase 2: Recipe adapters

The existing Marker 2, Docling, and Mistral evaluation paths are starting
points, not yet production recipe implementations. This phase promotes them to
the shared evidence contract, complete provenance, and worker dispatcher.

| Recipe | Role | Location evidence strategy |
| --- | --- | --- |
| Legacy Markdown + enrichment | Backfill/control | Common PDF alignment seeded by legacy evidence |
| Marker 1 + enrichment | Compatibility baseline | Native evidence plus common alignment |
| Marker 2 + enrichment | New local/VLM candidate | Native output plus common normalization/alignment |
| Docling | Structured local candidate | Preserve hierarchy, provenance, geometry, and lossless JSON |
| Datalab API | Hosted Datalab/Marker family | Preserve returned evidence, identity, geometry, and usage |
| Mistral OCR | Independent hosted candidate | Preserve page blocks, regions, labels, confidence, and usage |

MinerU is the first conditional challenger. PP-StructureV3 and more expensive
VLM systems remain second-round options only if the initial set exposes a
specific deficit.

API adapters enforce rights approval, idempotency, bounded retries, per-attempt
and per-job spend ceilings, response sanitization, and checkpoints that prevent
packaging retries from repeating billable calls.

Promotional credits or subscription quota may fund the Mistral and Datalab
evaluation incrementally over several months. Scheduling is quota-aware and
resumable by `(source_digest, recipe_digest)`. Reports retain both normalized
list-price cost and actual billed usage/credit consumption so temporary credits
do not distort the long-term recipe comparison. Credentials and account credit
balances remain runtime secrets and never enter recipe JSON or MDAFs.

Exit gate: every eligible recipe emits a BlobForge- and Vulcan-valid MDAF,
records reproducibility information or an explicit experimental warning, and
runs through the same evaluation harness.

### Phase 3: Rulebook evaluation

Run every recipe at least twice on identical inputs. Complete books form the
acceptance corpus, labeled pages provide ground truth, and a hidden holdout
validates selection. Human review is blinded to backend identity and order.

Report text fidelity; reading order; document structures; tables, equations,
assets, captions, and references; source-map coverage and accuracy; Vulcan/wiki
utility; latency, resources, failures, artifact size, and cost; repeated-output
stability, model pinning, and native-evidence completeness.

Exit gate: the benchmark identifies Pareto-optimal recipes per source class,
documents limitations and fallback behavior, and confirms results on holdout
data. Attractive Markdown on a few pages is insufficient.

### Phase 4: Routing and rollout

Add a versioned policy above exact recipe selection. Inputs may include media
type, native-text/scan ratio, layout complexity, language, tables/equations,
quality tier, privacy, cost ceiling, and worker availability. Policy resolves to
an exact recipe digest, not an ambiguous backend alias.

Administrators retain per-job overrides. Every automatic or manual decision
records policy revision, observed features, recipe, rationale, and actor. A
fallback can run only when policy permits its quality, data-handling, and cost.

Exit gate: production canaries validate claiming, checkpoints, publication,
MDAF/Vulcan acceptance, accounting, and rollback before capacity expands.

Implementation status: advisory `pdf-rulebooks` policy revision 1, its
recomputed/audited coordinator apply endpoint, and the isolated multipurpose
recipe worker are implemented. The only deployable entry is the bounded
Mistral wiki-v2 canary; the production canary, rollback exercise, hidden
holdout, provider accounting/checkpoint gate, and exact local/privacy recipe
remain. See `routing_and_recipe_workers.md`.

## Parallelism and ordering

After Phase 0 freezes, enrichment implementation, adapter work, ground-truth
labeling, review tooling, and routing design can proceed concurrently. Bulk
legacy enrichment can run alongside adapters after its own canary passes.

These dependencies remain ordered:

- no bulk enrichment before its recipe and acceptance gates freeze;
- no scoring definition after candidate outputs are visible;
- no paid upload before rights and spend limits are recorded;
- no default selection before holdout and production canaries pass;
- no deletion or overwrite of legacy artifacts during this program.

Significant decisions belong in focused documents and `AGENTS.md` Findings.
Execution sessions go in `docs/WORK_LOG.md`; immutable run manifests accompany
the human report in `docs/converter_benchmark_results.md`. Scope or ordering
changes update this roadmap and the canonical `TODO.md` section together.
