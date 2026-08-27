# TODO List

## Self-hosted Backend

- [x] Implement a conventional FastAPI coordinator using SQLite WAL, local
  immutable object storage, fenced leases, and short-lived HMAC transfer URLs.
- [x] Make source persistence and job claims digest/media-aware while limiting
  the current Marker worker to `application/pdf`.
- [x] Add a fail-closed, idempotent importer from the verified local v2 stage
  into BLAKE3-backed local source storage and `mdaf/v1` artifact records.
- [x] Add a companion verified raw-source importer so the 431 sources without
  legacy artifacts are queued rather than stranded during backend cutover.
- [x] Add a lightweight server container, Podman Quadlet/volume examples, GHCR
  server/worker image builds, and a full-test GitHub Actions workflow.
- [x] Add exact-recipe/backend conversion selection, a recipe registry, and
  multi-capability claim routing while retaining the current worker protocol.
- [x] Add Authlib OIDC login plus SCIM 2.0 user/group provisioning with
  SCIM-backed role and account-lifecycle enforcement.
- [x] Provision the initial authorized administrator through Authentik and
  force-reconcile the `blobforge-admin` membership into production SCIM state.
- [x] Replace raw browser JSON and reused-callback 500 responses with private,
  recovery-oriented HTML error pages while preserving API/SCIM JSON errors.
- [ ] Close Authentik's filtered-SCIM first-membership gap with ordered,
  event-driven user-before-membership provisioning; retain a short bounded
  full-sync safety net without widening BlobForge's directory scope.
- [x] Add an authenticated self-hosted root landing page with OIDC redirect,
  queue counts, and API navigation instead of FastAPI's default 404.
- [x] Implement the Gandalf BlobForge role/service inventory, vaulted secrets,
  private Authentik SCIM backchannel, Caddy route, and quiesced Citadel backup;
  compile generated inventory and run a check-mode deployment before cutover.
- [x] Activate the validated Caddy configuration with an explicitly approved
  shared-ingress restart and verify public TLS, OIDC, and SCIM denial.
- [x] Apply Citadel's shared Restic role to install the declared BlobForge
  profile, then run and verify its first backup/restore test.
- [ ] Add public digest-alias resolution and switch new ingestion keys from the
  SHA-256 compatibility key to canonical BLAKE3.
- [ ] Replace environment-only worker bootstrap with revocable token CRUD and a
  narrowly scoped management UI/API.
- [ ] Add private collections, SCIM-group collection roles, normalized
  discovery tags, scoped service-account tokens, worker-only token enforcement,
  and deny-by-default authorization tests as specified in `docs/access_control.md`.
- [ ] Add SQLite online-backup plus referenced-object manifests, restore
  verification, retention, and orphan/pending-object cleanup.
- [ ] Replace the production PDF worker's legacy ZIP publisher with staged MDAF
  generation/validation and add the isolated multipurpose adapter dispatcher.
- [ ] Generalize filesystem ingestion beyond `.pdf`, including safe media-type
  detection, adapter selection, and source-type-specific limits.
- [x] Run the complete 1,377-artifact plus 431 raw-source local import and audit.
- [x] Transfer the verified recovery unit to Citadel, verify its complete
  BLAKE3 manifest plus SQLite/object counts, and start a healthy coordinator
  before changing the canonical DNS record. Bunny/S3 remains preserved.

## MDAF / BLAKE3 Redesign

- [ ] Approve the target architecture and staged migration plan in
  `docs/mdaf_redesign.md` before changing production persistence contracts.
- [ ] Build a representative, rights-cleared evaluation corpus and frozen
  scoring harness for Markdown quality, source-map fidelity, assets, tables,
  equations, reading order, and reproducibility.
- [x] Add a read-only corpus inventory that records source/document counts,
  pages, bytes, legacy object coverage, and SHA-256/BLAKE3 alias status so API
  conversion costs can be calculated from real page totals.
- [ ] Benchmark the shortlisted local and API converters and record quality,
  latency, failure rate, peak resources, artifact size, and normalized cost.
- [x] Approve and implement the versioned converter subprocess ABI and
  ConversionBundle v1 described in `docs/converter_adapter_architecture.md`;
  heavy engines must remain in separately locked environments.
- [ ] Freeze the 43-PDF corpus manifest and select 5-10 adjudication pages
  per source/canonical edition plus a hidden holdout; record rights before sending any source to an
  external API.
- [x] Implement the shared MDAF builder/validator and prove it with a fixture
  adapter, `assets/lorem.pdf`, and one rulebook before integrating paid APIs.
- [x] Build or install the current Vulcan checkout so the documented `artifact
  validate` and dry-run import gates are available; the installed 0.1.0 binary
  predates those commands.
- [x] Add deterministic and Marker 1 compatibility adapters as the first cheap
  end-to-end MDAF baselines, then add Docling standard and Mistral OCR.
- [ ] Create isolated, pinned evaluation environments/adapters for Docling
  standard, Marker 2 fast/no-OCR, MinerU pipeline, and PP-StructureV3 on the
  current CPU workstation.
- [ ] Configure the Windows GTX 1070 host with WSL2 and a pinned CUDA 12.x
  evaluation stack; probe Docling standard CUDA, GraniteDocling/SmolDocling,
  and Surya llama.cpp on selected hard pages, recording Pascal compatibility,
  VRAM, latency, and quality.
- [ ] Only if first-round results justify it, rent a 48-80 GiB NVIDIA evaluation
  host for Marker 2 vLLM, MinerU hybrid, PaddleOCR-VL, olmOCR, and gated
  Chandra/DeepSeek/dots.ocr comparisons.
- [ ] Add hosted evaluation adapters with hard spend caps for Mistral OCR 4.1 and
  Datalab, followed by Google Layout/AWS Textract structured controls if their
  hard-page results justify full-corpus runs.
- [ ] Resolve whether to restore the two absent Trinity Continuum PDFs before
  paid runs; the bookmarked Rigger 5.0 PDF is the current canonical candidate.
- [x] Re-inventory the expanded priority corpus, detect exact duplicates, and
  refresh current Mistral/Google/AWS API budgets.
- [x] Implement and test an MDAF v1 writer/validator against Vulcan's schemas,
  fixtures, and logical-identity test vector.
- [ ] Add streaming BLAKE3 source hashing while retaining SHA-256 as a migration
  alias; version the local hash cache and coordinator lookup contracts.
- [x] Add one-pass BLAKE3+SHA-256 streaming, algorithm-specific xattrs, and an
  additive algorithm-keyed local SQLite digest cache; coordinator alias lookup
  remains part of the v2 persistence task.
- [ ] Replace monolithic conversion with staged extraction, normalization,
  source-map/outline generation, packaging, validation, and publication.
- [ ] Extend coordinator persistence and leases for source, recipe, activity,
  attempt, and immutable MDAF artifact identities.
- [x] Implement loss-aware legacy ZIP-to-MDAF migration with explicit
  unavailable provenance/mapping diagnostics and resumable verification.
- [ ] Run a dual-read/dual-identity canary before retiring SHA-256 and legacy
  ZIP endpoints or deleting any legacy object.
- [x] Mirror the complete `blobforge:pdf` prefix locally with read-only rclone
  copy semantics and inventory all 1,808 sources / 1,377 paired legacy ZIPs.
- [x] Add a fail-closed local v2 staging command that requires complete verified
  migration state and materializes source/artifact keys plus a checksummed run
  manifest without exposing any remote upload or deletion operation.
- [x] Add CPU-only locked Poppler, Marker 1, Marker 2, and Docling evaluator
  environments plus a pinned Mistral OCR 4.1 adapter with hard spend ceilings.
- [x] Freeze the 43-document rulebook corpus manifest with BLAKE3/SHA-256,
  9,465 pages, byte sizes, and a canonical manifest identity.
- [x] Add structural MDAF comparison metrics and validate Poppler, Docling, and
  Marker 1 fixture outputs with both BlobForge and Vulcan.
- [ ] Freeze downloaded local model inventories/checksums and replace mutable
  Marker/Docling model aliases before any production publication.
- [ ] Add selected hard-page labels, blinded review bundles, and semantic
  quality scoring; structural output counts are diagnostics, not a winner.

## High Priority
- [ ] Add unit tests for S3 operations and queue state transitions.
- [ ] Test heartbeat mechanism under load.

## Normal Priority
- [ ] Before enabling Marker 2, require its inference backend to expose a pinned model revision or verifiable manifest checksum and include it in conversion recipe identity; never accept a mutable model alias as sufficient provenance.
- [ ] Update README.md with full configuration reference.
- [ ] Add a separate dynamic-registration token flow for intentionally reusable bootstrap credentials, issuing distinct incremented worker IDs and per-worker credentials at registration time.
- [ ] Consider adding optional SQS/SNS integration for larger scale.

## Low Priority
- [ ] Optionally add conversion-artifact history and recipe selection controls to the management console; equivalent CLI/API operations are available.

## Done
- [x] Add `blobforge hydrated clean-textpacks` and safe `unpack` reverse
  conversion with validation, dry runs, and overwrite protection.
- [x] Add dry-run-first `blobforge hydrated clean` and `textpack` maintenance
  operations for PDF-anchored Markdown/assets outputs.
- [x] Inventory the 17 priority rulebooks and calculate full-corpus Mistral,
  Google Layout, retry, repeated-run, and local-runtime budget scenarios.
- [x] Add CLI commands for listing, downloading, previewing, selecting, and requesting recipe-specific conversion artifacts.
- [x] Include effective output-affecting Marker/Surya settings in recipe identity while excluding performance-only worker tuning.
- [x] Add canonical recipe-aware conversion identity, exact runtime/model provenance, composite artifact storage, recipe-bound leases, legacy artifact preservation, and explicit artifact selection/reconversion APIs.
- [x] Constrain native conversion installs to the tested Marker 1.x generation and validate Surya's external llama.cpp/vLLM prerequisites before coordinator contact in already-drifted environments.
- [x] Diagnose the `llama-server binary not found` worker failure as an unlocked Marker 2.0 / Surya 0.22 environment using Surya's CPU llama.cpp OCR backend instead of the repository's locked Marker 1.10 / Surya 0.17 stack.
- [x] Fix the four correctness regressions found in the inclusive `7ff1c5f3...` review: dry-run uploads, orphaned raw enqueue recovery, hydration hash persistence, and coordinator-scoped done mirrors.
- [x] Make workers validate Marker before coordinator contact, classify late conversion-runtime failures as host configuration errors, release rather than fail affected leases, and suppress stale post-release heartbeats.
- [x] Diagnose the native `uv run blobforge worker` failure loop when the optional Marker conversion dependency is absent.
- [x] Review the inclusive change range from `7ff1c5f3bc01f5eb0382278c7f4f0c481b44d335` through the current working tree for correctness and regressions.
- [x] Add revocable per-operator admin tokens and an optimized bulk job-status API so `ingest`/`hydrate`/`download`/`preview` need no direct S3 credentials.
- [x] Add a public, CDN-cacheable Edge Script documentation landing page.
- [x] Reduce coordinator heartbeat traffic with suspension-aware workers, dynamic intervals, and lease-only mode.
- [x] Hide revoked workers from normal fleet views and expose them separately.
- [x] Add CPU/CUDA worker images plus a no-clone Linux systemd installation workflow.
- [x] Remediate the 33 Dependabot alerts with patched universal-lock constraints, Python 3.10+, conversion-tested Pillow override, and documented non-applicable Transformer/Torch advisories.
- [x] Derive worker IDs directly from label slugs and reject duplicate/colliding enrollment labels.
- [x] Make coordinator progress updates prompt and add persistent per-attempt failure diagnostics with a Web UI history viewer.
- [x] Replace the preview's handwritten Markdown subset with Marked + DOMPurify and add a sticky/collapsible active-section ToC.
- [x] Add Web UI PDF ingestion, paginated library search/filtering, PDF/ZIP downloads, and client-side result previews.
- [x] Add application-level Bunny Database backups to S3 and a management UI trigger.
- [x] Remove manifest/log/Telegram dependencies and add dry-run-first legacy S3 cleanup.
- [x] Add per-worker UI enrollment/revocation, coordinator-issued S3 transfer URLs, and coordinator-backed worker/dashboard CLI views.
- [x] Replace unavailable Bunny admin session cookies with a fragment-bootstrap, browser-stored signed session and authenticated API header.
- [x] Make Bunny admin session cookies scheme-independent, disable CDN auth caching, and add an auth transport diagnostic endpoint.
- [x] Preserve strict CSP while allowing the IndieAuth form redirect through script-driven top-level navigation.
- [x] Fix Bunny IndieAuth cross-edge login sessions and add normalized profile input with a multi-admin allowlist.
- [x] Replace S3 queue coordination with Bunny Edge Scripting + Bunny Database, IndieAuth management UI, fenced leases, and legacy-state migration.
- [x] Isolate scheduled-abort worker conversions in a child process so native marker crashes do not kill the worker.
- [x] Add worker run-window scheduling with optional active-job abort/requeue.
- [x] Requeue all current failed, dead-letter, and stale processing jobs after follow-up investigation.
- [x] Investigate current failed/dead-letter jobs after rerun.
- [x] Remove the two PDFium data-format failure jobs from dead-letter, raw store, manifest, and logs.
- [x] Requeue non-PDFium dead-letter jobs for retry while leaving PDFium data-format failures in dead-letter.
- [x] Investigate current failed/dead-letter PDF queue and assess local retry viability.
- [x] Add `blobforge repair-metadata` to restore stripped raw-object metadata from manifest entries after S3 provider migrations.
- [x] Investigate `blobforge dashboard` `unknown.pdf` display and confirm whether it reflects missing data or only missing raw-object metadata.
- [x] Optimize `blobforge hydrate` with local hash preflight + single manifest prefilter to reduce per-file remote checks.
- [x] Add `blobforge hydrate` command to materialize `<stem>.md` and `<stem>.assets/` from completed conversions.
- [x] Document worker graceful shutdown and conversion timeout behavior in `README.md`.
- [x] Keep shutdown signal handlers active until cleanup completes and route unexpected loop exceptions through graceful shutdown.
- [x] Requeue active jobs before heartbeat join wait to release processing locks promptly on shutdown.
- [x] Resolve startup recovery retry undercount by reconciling lock + todo retry metadata.
- [x] Enforce conversion timeout in worker conversion path (platform-supported hard timeout with signal timers).
- [x] Add subprocess-level SIGTERM integration test for worker loop graceful shutdown path.
- [x] Add signal-aware graceful worker shutdown that requeues the in-flight job before exiting.
- [x] Count recovered processing locks as failed attempts during worker startup (increment retry, dead-letter when exceeded).
- [x] Enhanced Heartbeat Metadata (CPU/RAM, page progress, elapsed time)
- [x] Richer Dashboard / Status Display (filenames, sizes, progress)
- [x] Job Throughput Metrics (jobs_completed, avg_time, bytes_processed)
- [x] Job Logs / Error Details (store logs, `blobforge logs <hash>`)
- [x] Better CLI Experience (`blobforge watch`, cancel, download, preview)
- [x] Worker Management (health status, active filter, metrics display)
- [x] Queue Management (bulk retry, clear dead, search by filename)
- [x] Output/Results Features (`blobforge download`, `blobforge preview`)
- [x] Add S3 namespacing support via `S3_PREFIX`.
- [x] Refactor scripts to use central config.
- [x] Initialize agent protocols and documentation structure.
- [x] Consolidate S3Client into single module.
- [x] Implement sharding logic (256 shards with 2-char prefix).
- [x] Fix race condition in job acquisition.
- [x] Add heartbeat mechanism with configurable timeout.
- [x] Add retry limits and dead-letter queue.
- [x] Make ingestor state-aware (check all queues).
- [x] Add CLI retry command.
- [x] Generate persistent worker ID from machine fingerprint.
- [x] Fix DESIGN.md and document all features.

## Done
- [x] Add S3 namespacing support via `S3_PREFIX`.
- [x] Refactor scripts to use central config.
- [x] Initialize agent protocols and documentation structure.
- [x] Consolidate S3Client into single module.
- [x] Implement sharding logic (256 shards with 2-char prefix).
- [x] Fix race condition in job acquisition.
- [x] Add heartbeat mechanism with configurable timeout.
- [x] Add retry limits and dead-letter queue.
- [x] Make ingestor state-aware (check all queues).
- [x] Add CLI retry command.
- [x] Generate persistent worker ID from machine fingerprint.
- [x] Fix DESIGN.md and document all features.
