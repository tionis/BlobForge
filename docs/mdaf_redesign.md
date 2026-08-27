# BlobForge MDAF and BLAKE3 Redesign

Status: proposed architecture for review  
Date: 2026-08-26  
Consumer contract: Vulcan MDAF v1

## Executive decision

BlobForge should become a source-neutral artifact production service rather
than a Marker-specific PDF-to-ZIP queue. The durable output is a validated MDAF
v1 artifact; the extractor is a recipe-selected adapter. BLAKE3 becomes the
canonical digest for sources, recipes, members, and MDAF identities. Existing
SHA-256 values remain permanent aliases so old coordinator rows, raw objects,
links, and archives can be resolved without pretending that SHA-256 and BLAKE3
are interchangeable.

The conversion engine is deliberately not selected by this architecture.
Marker 2, Docling, Mistral OCR, and at least one high-quality VLM challenger
must be evaluated on representative rulebooks using the protocol in
`converter_evaluation.md` and the adapter boundary in
`converter_adapter_architecture.md`. The winning engine can change later without a
schema migration because producer-native evidence is an opaque MDAF rendition
and the normalized boundary remains stable.

No legacy object should be deleted during this program. Production should move
through additive schema changes, dual identity resolution, MDAF canaries, and
verified migration before any retirement decision.

## Goals

- Make MDAF v1 the immutable publication and Vulcan handoff boundary.
- Make the default 256-bit BLAKE3 digest the canonical source identity.
- Preserve SHA-256 as a tagged alternate identity and migration lookup.
- Publish Markdown-to-PDF page and region mappings whenever the extractor
  provides evidence reliable enough to normalize.
- Preserve complete, sanitized extractor-native evidence for future use.
- Record exact tools, models, revisions, output-affecting settings, activity
  dependencies, attempts, and validation results.
- Allow local, remote API, and hybrid extractors to coexist in independent
  worker pools.
- Repackage old artifacts as honest, limited-capability MDAF artifacts without
  inventing source maps or provenance.
- Make conversion quality and operating cost measurable before committing the
  fleet to an engine.

## Non-goals

- MDAF does not become a universal internal document-block schema.
- Vulcan does not learn about Marker, Docling, Mistral, or another extractor.
- The first release does not merge competing extractors' block trees.
- The first release does not require page-level distributed fan-out. Adapters
  may batch pages internally, but one document is published atomically.
- A recipe digest is not treated as an artifact digest, and a source digest is
  neither of those.
- Missing historical evidence is never reconstructed speculatively.

## Four identities

The current untagged 64-character `file_hash` is simultaneously a file key, job
key, API identifier, and output namespace. That coupling must be removed.

| Identity | Meaning | Canonical representation | Cardinality |
| --- | --- | --- | --- |
| Source | Exact input bytes | `blake3:<64 lowercase hex>` | One source can have many names and recipes |
| Recipe | Output-affecting pipeline contract | BLAKE3 of canonical recipe JSON | One recipe applies to many sources |
| Job | Desired source/recipe result | coordinator-generated ID plus unique `(source, recipe)` | Has many attempts and at most one selected artifact |
| Artifact | Logical contents of one MDAF package | MDAF v1 logical BLAKE3 identity | A job can observe multiple valid outputs, one selected |

An API or VLM may be nondeterministic. In addition, MDAF provenance can contain
activity timestamps, so repeat attempts are not guaranteed to have the same
artifact identity even when `text.md` is identical. The `(source, recipe)` job
is the cache key; the MDAF identity proves exactly what was published. The
coordinator selects the first valid result by default and retains later results
only when an explicit evaluation or replacement policy allows it.

All externally visible digests are algorithm-tagged. URL routes use separate
algorithm and hexadecimal path components, for example
`/api/v2/sources/blake3/<hex>`, rather than placing `:` in a path segment.

## Recipe identity

Recipe schema 2 should include every output-affecting stage:

- extractor adapter name and adapter ABI;
- extractor tool version and build revision;
- model provider, requested identifier, returned identifier, immutable revision
  or checksum, and resolution state;
- extraction mode and semantic settings;
- Markdown normalization policy and version;
- asset extraction, naming, encoding, and filtering policy;
- source-map generation and confidence publication policy;
- outline generation policy;
- native-rendition and redaction policy;
- MDAF schema version and BlobForge packager version.

Performance-only concurrency, batch size, cache paths, device placement, log
verbosity, and worker count remain runtime provenance rather than recipe input
unless they are demonstrated to change output. Recipe JSON uses the same
canonical JSON rules as MDAF `parameters_digest`, restricts integers to the
portable JavaScript-safe range, and encodes semantically significant fractional
values as strings. The digest changes from the current SHA-256 recipe digest to
tagged BLAKE3; schema 1 digests remain aliases for old artifacts.

A mutable model alias is never sufficient for a reproducible production recipe.
The worker records both requested and returned identifiers. An unresolved alias
may be used in an explicitly experimental recipe, but it must emit a
reproducibility warning and cannot become the default recipe.

## Target MDAF profile

BlobForge produces a ZIP representation whose filename ends in `.mdaf`. It
contains:

```text
info.json
text.md
provenance.json
source-map.json                         when reliable mappings exist
outline.json                            when completely aligned
assets/...                              referenced by text.md
renditions/<producer-namespace>/...     sanitized native response
environments/...                        optional lock/SBOM/runtime inventory
extensions/dev.tionis.blobforge/...    diagnostics not represented by core MDAF
sources/source.pdf                      optional, policy-controlled
```

The normal service profile does not embed the source PDF because BlobForge
already stores it content-addressably. Export or archival recipes may embed it.
An MDAF remains self-contained for interpretation: absence of embedded source
only prevents source rendering, not validation of declared source identity.

The packager must implement the archive path, Unicode, size, count, compression,
symlink, digest, manifest, provenance, and semantic rules from the authoritative
MDAF v1 specification. BlobForge should bundle a reviewed copy of the v1 schemas
and fixtures with a recorded upstream revision. CI must run both its own writer
tests and `vulcan artifact validate` against generated fixtures. Runtime workers
must not depend on a sibling Vulcan checkout or a network fetch.

### Source maps

The adapter returns an ordered intermediate extraction bundle rather than only
a Markdown string:

```text
ExtractionBundle
  source metadata and page geometry
  ordered blocks/segments with native IDs and source locators
  assets and the blocks that reference them
  native response files
  extractor diagnostics and usage
```

Normalization emits final Markdown while maintaining a byte-span ledger. Every
published span is measured only after final UTF-8 serialization and asset-link
rewriting. This prevents character offsets, code-point offsets, and UTF-8 byte
offsets from being confused. Page selectors are zero-based, half-open intervals
with `unit: page`; rectangles use top-left normalized coordinates where the
source evidence supports them. Original coordinate systems remain in the native
rendition.

Mappings can be partial and overlapping. A low-quality rectangle or inferred
alignment is omitted rather than presented as fact. Page-only mappings are
useful and may be published even when block geometry is unavailable. Confidence
and a reverse-domain-namespaced method identify the evidence path. There is no
fabricated whole-document mapping for legacy outputs.

Markdown links that represent a real source cross-reference are emitted in
`source-map.json.references`; ordinary external hyperlinks remain Markdown.

### Outlines

Markdown headings remain the default hierarchy authority in Vulcan. An
`outline.json` is emitted only if every node is aligned to final Markdown byte
spans and the forest is complete. The native engine hierarchy is always kept in
its rendition even when it does not meet the normalized outline bar.

### Native evidence and secrets

API responses and local engine JSON are stored byte-for-byte after a mandatory,
adapter-specific secret filter. Redaction is schema-aware, records the field
location and reason in `provenance.json`, and includes the original field digest
only when computing it does not retain or expose secret material. Requests,
responses, signed URLs, authorization headers, API keys, private endpoint names,
and absolute local paths are forbidden from portable core data.

## Worker architecture

Workers become small orchestration hosts with extractor adapters. Heavy
dependencies live in engine-specific images rather than one universal image:

- `blobforge-worker-marker2` for Marker 2 and its Surya inference backend;
- `blobforge-worker-docling` for Docling pipelines;
- `blobforge-worker-api` for Mistral and other remote APIs;
- `blobforge-worker-legacy` for deterministic old-ZIP migration.

Each worker advertises exact recipe digests and capabilities. A lease is bound
to one source digest and one recipe digest. The coordinator never assigns a
recipe the worker did not advertise. API credentials stay on the API worker and
are never returned by the coordinator.

One attempt executes these durable stages:

1. Claim the `(source, recipe)` job with a fenced lease.
2. Download and stream-verify the source's BLAKE3 digest; verify SHA-256 too when
   an alias is present during migration.
3. Inspect source media and page geometry without trusting filename extensions.
4. Run the selected extractor and record provider usage, model response IDs,
   timing, and native evidence.
5. Sanitize and freeze the native response.
6. Normalize Markdown and assets while building the final UTF-8 byte-span
   ledger.
7. Generate eligible `source-map.json` and `outline.json` sidecars.
8. Generate the activity DAG and optional environment inventory.
9. Package the `.mdaf`, calculate member and logical identities, and validate it.
10. Request a lease-bound publication URL using artifact identity and size,
    upload once, then complete with the validated identity and summary metrics.

Stage transitions are append-only attempt events and drive progress updates.
An expensive native API response may be checkpointed under a private,
attempt-scoped object with a TTL so packaging or upload retries do not repeat a
billable call. A checkpoint is never a published artifact and is reusable only
after digest, recipe, attempt, and redaction-policy validation.

The child-process isolation contract remains. Local native crashes fail or
release the attempt without killing the lease-owning parent. API rate limits,
timeouts, retry-after values, and non-billable client errors get distinct failure
classes. Retries must be idempotent at the stage level and enforce a per-attempt
and per-job spend ceiling.

## Coordinator data model

Use additive v2 tables rather than altering the meaning of v1 columns in place:

```text
sources(
  source_digest PK, media_type, size_bytes, object_key,
  created_at, verified_at
)
source_aliases(
  algorithm, digest, source_digest FK,
  provenance, UNIQUE(algorithm, digest)
)
source_names(source_digest FK, name, path, tags, source, timestamps)
recipes(recipe_digest PK, schema_version, canonical_json, status, created_at)
jobs_v2(
  job_id PK, source_digest FK, recipe_digest FK, status, priority,
  selected_artifact_digest, lease fields, retry fields, timestamps,
  UNIQUE(source_digest, recipe_digest)
)
attempts(
  attempt_id PK, job_id FK, worker_id, state, stage,
  started_at, ended_at, usage_json, cost_json, runtime_json, error fields
)
attempt_events(attempt_id FK, sequence, stage, progress_json, created_at)
artifacts_v2(
  artifact_digest PK, source_digest FK, recipe_digest FK, object_key,
  size_bytes, text_digest, capabilities_json, producer_json,
  validation_status, created_at, worker_id, attempt_id
)
job_artifacts(job_id FK, artifact_digest FK, disposition, created_at)
```

Tagged digests should be stored in full unless query performance proves a need
for separate algorithm and bytes columns. Database constraints validate the
tagged form. The schema must not assume every future algorithm has 64 hex
characters.

The v2 API resolves canonical BLAKE3 and SHA-256 aliases. Legacy `/api/v1/jobs`
routes remain read-only compatibility views during rollout. Done-set hydration
must become artifact-aware: completion means a selected, validated MDAF exists,
not merely that one output object exists for a source.

The completion protocol is changed because the final artifact identity is not
known at claim time. After local validation, the worker sends a publication
intent containing lease token, MDAF identity, size, and text digest. The
coordinator returns a narrowly scoped upload URL and refuses an identity already
bound to conflicting metadata. Object keys include both identity and a unique
attempt suffix so a failed or malicious upload cannot overwrite a prior valid
artifact. The database, not key parsing, is the artifact index.

## Object layout

New objects use sharded algorithm-tagged namespaces:

```text
store/v2/sources/blake3/ab/<source-hex>
store/v2/recipes/blake3/ab/<recipe-hex>.json
store/v2/artifacts/mdaf/v1/blake3/cd/<artifact-hex>/<attempt-id>.mdaf
store/v2/checkpoints/<attempt-id>/<stage>/blake3/<digest-hex> private, TTL-managed
store/v2/migrations/<run-id>/manifest.json
store/legacy/raw/sha256/<sha256>.pdf                  compatibility alias/path
store/legacy/out/sha256/<sha256>/...                  existing ZIPs unchanged
```

Source and artifact media type lives in the database and manifest rather than
being inferred from an extension. Existing S3 keys are not renamed in place.
New BLAKE3 source rows may initially point to old SHA-256 raw keys; an optional
later copy migration can change `object_key` after readback verification.

Object-store versioning and retention should be enabled before migration.
Checkpoint garbage collection and losing duplicate attempt objects require a
dry-run report, minimum age, database reachability check, and audit entry.

## Ingestion and hashing migration

The new streaming hasher calculates BLAKE3 and SHA-256 in one pass during the
transition. The local SQLite hash cache keys values by `(path, size, mtime_ns,
algorithm)` and migrates existing SHA-256 rows without discarding them. Xattrs
become algorithm-specific (`user.checksum.blake3` and the existing
`user.checksum.sha256`) with common stat validation.

Git LFS pointers expose only SHA-256. BLAKE3 cannot be derived from SHA-256.
Ingestion therefore follows this order:

1. Read the LFS SHA-256 pointer.
2. Ask the coordinator alias endpoint whether it already maps to a verified
   BLAKE3 source.
3. If mapped, reuse the canonical source without materializing the file.
4. If unknown, materialize once, stream both hashes, register the alias, upload
   or verify the raw object, then restore the pointer.

All raw downloads are rehashed by workers. Coordinator alias registration is
accepted only with an uploaded/verifiable source or an authenticated migration
proof; arbitrary clients cannot bind a SHA-256 value to a BLAKE3 source.

## Legacy ZIP to MDAF migration

The old archive normally contains `content.md`, `assets/`, `info.json`, and
possibly `marker_meta.json`. It lacks normalized source maps and may lack exact
tool/model/runtime provenance.

Migration is resumable and read-only with respect to old objects:

1. Resolve the old SHA-256 to raw source bytes and calculate BLAKE3. If the raw
   source is missing, quarantine the row; a conforming canonical source digest
   cannot be invented.
2. Download the old ZIP, reject unsafe paths/duplicates, and digest every input.
3. Copy `content.md` bytes to `text.md`; copy only referenced safe assets.
4. Preserve `marker_meta.json` under a BlobForge native rendition and old
   `info.json` under a namespaced extension after secret/path filtering.
5. Emit a legacy-import activity and a packaging activity. Use explicit
   `unavailable` tool/model revisions and a diagnostic extension when the old
   archive cannot prove them.
6. Omit `source-map.json` and `outline.json` unless the archive itself contains
   sufficient valid evidence. Do not infer mapping from page count or filenames.
7. Include the source's old SHA-256 as `alternate_digests`.
8. Package, validate locally and with the Vulcan conformance suite, upload as a
   new object, read it back, and validate again before recording success.

The original ZIP is retained through at least one full backup cycle and until a
sampled Vulcan import comparison succeeds. It is not listed in `derived_from`
because that field accepts MDAF logical identities, not arbitrary ZIP hashes.
Embedding the complete old ZIP as a rendition is an optional archival recipe;
the default migration avoids doubling storage while retaining the old external
object during the rollback window.

Legacy MDAFs advertise no source-map or outline capability. Their visible
limitations are a feature: consumers can distinguish migrated Markdown from
new evidence-rich extraction without branching on the producer name.

## Rollout phases and exit gates

### Phase 0: freeze and baseline

- Keep workers stopped.
- Take and checksum a coordinator backup and object inventory.
- Record counts and bytes for raw sources, recipe artifacts, orphan objects,
  missing raw sources, and duplicate SHA-256 identities.
- Freeze a representative evaluation corpus and current Marker 1 outputs.

Exit: inventories reconcile and restore is rehearsed.

### Phase 1: MDAF core and evaluator

- Add BLAKE3 dependency and streaming multi-hash utility.
- Implement MDAF writer, path safety, logical identity, canonical parameters,
  and validation.
- Vendor pinned schemas/fixtures and add Vulcan cross-project conformance CI.
- Build the evaluation runner before any engine adapter is promoted.

Exit: generated minimal and rich fixtures validate identically in BlobForge and
Vulcan; the published identity test vector passes.

### Phase 2: additive coordinator v2

- Add v2 tables, backup format, alias APIs, recipe registration, claims,
  attempts/events, publication intent, artifact selection, and downloads.
- Keep v1 data and routes intact.
- Migrate coordinator rows to source aliases without changing object keys.

Exit: backup/restore and migration are idempotent; every v1 file is mapped,
quarantined, or explicitly reported.

### Phase 3: adapters and bake-off

- Implement Marker 2, Docling, Mistral OCR, and challenger adapters.
- Retain native output and normalize mappings for each adapter.
- Run the frozen evaluation and cost model twice per candidate.

Exit: one default and one fallback recipe meet all quality, provenance,
reproducibility, legal, privacy, latency, and cost gates.

### Phase 4: canary production

- Start a small v2 worker pool with an explicit source allowlist and spend cap.
- Produce MDAFs without changing the selected v1 artifact.
- Validate, inspect, and import samples into disposable Vulcan destinations.

Exit: no integrity failures, no unbounded cost/retry behavior, and downstream
review accepts the canary corpus.

### Phase 5: legacy migration and dual read

- Backfill SHA-256/BLAKE3 aliases.
- Convert old ZIPs with the resumable migration worker.
- Make CLI/API/hydration prefer selected MDAF and fall back to legacy ZIP.

Exit: every eligible legacy artifact has a validated MDAF or a documented
quarantine reason; sampled byte/content/import comparisons pass.

### Phase 6: cutover

- Make v2 ingestion and MDAF the default.
- Keep SHA-256 alias resolution and legacy downloads for a defined deprecation
  period.
- Decide separately whether old objects can be archived; never couple deletion
  to schema deployment.

Exit: restore rehearsal, rollback rehearsal, monitoring, operating runbook, and
cost alerts are complete.

## Observability and controls

Record per attempt:

- pages, source bytes, output bytes, extracted characters, assets and mappings;
- wall time and stage time;
- CPU, RAM, GPU, VRAM, and accelerator utilization where available;
- provider request/response/model IDs, billed pages or tokens, retry count, and
  estimated/actual cost;
- source-map coverage and rejected-mapping counts;
- MDAF member, text, source, recipe, and artifact digests;
- validation diagnostics and completion readback result.

Global controls include per-recipe concurrency, rate limits, circuit breakers,
per-job spend caps, daily API budgets, maximum pages/bytes, checkpoint TTLs, and
a kill switch that suspends claims without heartbeat polling. Provider errors
must never trigger an unlimited billable retry loop.

## Immediate implementation slice

The safest first code change is intentionally narrow:

1. add BLAKE3 as a core dependency;
2. implement algorithm-tagged digest types and a streaming BLAKE3+SHA-256 file
   hasher;
3. implement an MDAF packager/validator module against pinned Vulcan fixtures;
4. package the existing `assets/lorem.pdf` Marker 1 result as a limited but
   conforming MDAF in tests;
5. add no coordinator or object-store mutations yet.

That slice proves the format and identity boundary before persistence and fleet
work begin.
