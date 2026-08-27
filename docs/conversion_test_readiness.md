# 32-GiB Conversion Test Readiness

Status: ready for bounded, reusable CPU conversion runs; not yet ready for a
fully scored 9,465-page campaign  
Assessed and implemented: 2026-08-27

## Decision

The 32-GiB machine is ready for comparable Poppler, Marker 1, Marker 2 no-OCR,
and Docling standard runs that produce independently validated MDAF v1 files.
The shared builder, subprocess ABI, isolated locks, frozen corpus manifest, and
structural comparison command now exist. Continue with bounded canaries rather
than launching all 9,465 pages: human labels, peak-resource capture, immutable
model snapshots, and cancellation/resume coverage remain launch gates for a
scored full-corpus campaign.

## Ready components

- Current corpus: 43 readable, exact-byte-distinct PDFs; 9,465 pages; 1,234.58
  MiB.
- System tools: `b3sum`, Poppler (`pdfinfo`, `pdftotext`), Docker, and Podman.
- Existing local baseline: Marker 1.10.2 / Surya 0.17.1 in BlobForge's `.venv`.
- Authoritative MDAF v1 specification, schemas, examples, and identity vector in
  the Vulcan checkout.
- Current Vulcan source contains MDAF inspect/validate/import code and tests.
- Documented converter subprocess/ConversionBundle architecture and evaluation
  protocol.

## Implemented since the initial audit

1. The 43-document manifest freezes BLAKE3/SHA-256, paths, sizes, and 9,465
   pages under identity `blake3:44b252c25c8a61dc2771c337cfca9d6b43734cefbac44f2d50b8e5130a3e2b35`.
2. BlobForge builds deterministic MDAF ZIPs and validates Vulcan schemas plus
   archive, digest, source-map, UTF-8 span, provenance, and Markdown-asset
   semantics.
3. ConversionBundle v1 isolates Poppler, Marker 1, Marker 2, Docling, and
   spend-capped Mistral adapters; one shared runner packages their output.
4. The current Vulcan CLI independently accepts the fixture and rulebook MDAFs.
5. Marker 1, Marker 2, and Docling use separate uv locks and CPU-only PyTorch.
6. `blobforge compare-mdaf` records structural counts for common artifacts.

## Remaining full-campaign gates

1. Select and label 5-10 hard pages per book plus a hidden holdout.
2. Capture peak RAM, failures, cancellation/resume behavior, and artifact sizes
   in a durable run ledger rather than relying on console elapsed time.
3. Freeze/checksum downloaded model snapshots; package pins and mutable aliases
   do not meet production provenance requirements.
4. Import the representative books into a disposable Vulcan vault and perform
   blinded wiki-quality review.
5. Add MinerU and PP-StructureV3 only after the current four-engine canary set is
   reviewed; paid APIs additionally require rights approval and explicit spend.

## Minimum launch sequence

The machine becomes ready for useful tests after this vertical slice:

1. Generate and commit a read-only corpus manifest with BLAKE3/SHA-256, size,
   pages, media type, and selected/excluded status.
2. Vendor the reviewed MDAF v1 schemas and identity fixture into BlobForge.
3. Implement the minimal MDAF directory/ZIP writer and local validator.
4. Build or install the current Vulcan CLI and validate its synthetic MDAF
   example.
5. Implement ConversionBundle v1 plus a fixture adapter and produce a valid MDAF
   from `assets/lorem.pdf`.
6. Add a deterministic Poppler/PyMuPDF adapter and convert one representative
   rulebook end to end.
7. Create a separate locked Docling environment and pass a one-page, one-book,
   and failure/cancellation smoke test.
8. Only then schedule complete-corpus Docling and baseline runs.

## Launch gates

Before a full 9,465-page recipe starts:

- the recipe and source list are immutable and content-addressed;
- every output is a BlobForge-valid and Vulcan-valid MDAF;
- a conversion can resume or fail without silently changing engines;
- native output, timing, peak RAM, errors, and recipe provenance are retained;
- disk projections and per-run timeout are set;
- the one-book canary imports into a disposable Vulcan vault successfully.

Paid APIs add rights review, secret redaction, idempotency, and hard spend caps
to these gates.
