# Agent Instructions

All LLM agents operating in this repository MUST adhere to the following protocols:

- **Documentation:** Document all architectural decisions, system components, and significant logic in the `docs/` directory.
- **Task Management:** Use `TODO.md` in the root directory to store, track, and look up all pending and completed todo items.
- **Activity Logging:** Log all actions, tool executions, and progress in `docs/WORK_LOG.md`.
- **Knowledge Sharing:** Note all significant findings, codebase insights, or updated protocols directly in this file (`AGENTS.md`) under the "Findings" section.

## Development Environment

This project uses **uv** as the Python package manager. Always use `uv` commands instead of `pip` or `python -m pip`:

```bash
# Install the project in development mode
uv pip install -e .

# Install with optional dependencies
uv pip install -e ".[metrics]"      # For psutil system metrics
uv pip install -e ".[convert]"      # For marker-pdf conversion
uv pip install -e ".[all]"          # All optional dependencies

# Install as a CLI tool
uv tool install .

# Run commands in the virtual environment
uv run blobforge --help
uv run python -m pytest tests/

# Add a new dependency
uv add <package>

# Sync dependencies
uv sync
```

The virtual environment is located at `.venv/` and should be activated automatically by uv or can be activated manually with `source .venv/bin/activate`.

## Findings

- **2026-08-30:** Coordinator-native MDAF upgrades now use explicit artifact
  inputs rather than masquerading as source conversions. Jobs retain one active
  row per source but bind `input_kind`, exact parent artifact ID, and parent
  recipe; capabilities declare `source` and/or `artifact`, and unregistered or
  source-only capabilities cannot claim artifact jobs. Previewing bulk
  reprocessing is read-only; execution atomically queues only compatible,
  non-processing parents without target artifacts and is available through
  API, CLI, and management UI. The coordinator independently validates an
  uploaded MDAF's logical identity, embedded target recipe, and exact
  `derived_from` parent before publication; logical identity is stored as the
  artifact identity while physical ZIP BLAKE3 remains separate. A keyless real
  Storypath coordinator-style canary queued the wiki-v2 parent as artifact
  input and deterministically reproduced derivative `blake3:e984145a...` with
  zero provider calls. The canary initially passed the extensionless internal
  object path directly to the suffix-checking validator; actual leases stage it
  as `parent.mdaf`, and the corrected canary passed. The exact-recipe worker
  also previously imported legacy S3 priorities (`1_critical`/`5_background`)
  while the coordinator uses `1_urgent` through `4_low`; its claim list now
  matches the coordinator contract.

- **2026-08-30:** Recipe lifecycle schema v3 now separates expensive
  extraction from repeatable post-processing. The semantic recipe major must
  equal the extraction major; same-major targets must permit automatic
  upgrades but explicitly allowlist every predecessor recipe digest and retain
  the exact extraction recipe. New lifecycle MDAFs embed canonical recipe JSON
  and record separate extraction/normalization activities. Offline
  `blobforge reprocess` validates the parent, requires declared native
  evidence, rejects family/major/extraction changes and overwrites, and emits a
  self-contained immutable derivative with `derived_from`, prior recipe,
  parent manifest/provenance, and byte-identical raw renditions. Mistral wiki
  `1.2.0` is recipe
  `blake3:3f504116b8747b311f07310ea48b53eddaf4a37330ffe6c29e015f06d4185139`.
  A credential-free Storypath direct replay produced
  `blake3:1cb473b433c901cd2b5259ead4c309ade7b2605459dc06299c52d8fbe74997a5`;
  upgrading the frozen wiki-v2 artifact produced derivative
  `blake3:e984145a8dab8e737134395b1a8d92ced890885f09b3b04ea35b4d9028c917eb`.
  Both retain the same 169,550-byte paid response and normalized Markdown.
  Routing policy v1 remains immutable on wiki-v2; v2 selects wiki-v3.
  Coordinator artifact-input/bulk upgrade jobs remain a distinct rollout task
  and must not be simulated by silently rerunning paid source conversions.

- **2026-08-30:** Mistral wiki-v2 freezes evidence-backed list normalization at
  recipe `blake3:bdd3e060e88f64277834245a42528a54b6b077774123c3806bdd827cf8ea3026`.
  A keyless Storypath replay produced valid MDAF
  `blake3:aedfe70488c3a376371e64e368dd51b2c3e224d1cf8aa4cea8ad1a23e30e4f0d`,
  removed 20 decorative glyphs following existing Markdown list markers, and
  recovered 10 items from consecutive provider-typed glyph-leading text
  blocks. It preserves lone ambiguous blocks, inline `At ♦` mechanics, and
  headings containing `• TO ••`; never globally replace ordinary `Y` or glyphs.
  Advisory routing policy v1 resolves eligible born-digital rulebook canaries
  to that exact digest with rights, applicability, language/layout, spend, and
  canary gates. The new isolated recipe worker can advertise and alternate
  multiple recipe/media capabilities, but only Mistral wiki-v2 is currently a
  deployable canary. The coordinator recomputes applied decisions and audits
  their actor, features, policy identity, recipe, estimate, status, and
  rationale; it also recomputes tagged BLAKE3 capability recipe JSON and does
  not treat offline/revoked workers as route availability. Local/privacy
  routing, holdout, billing, provider checkpoint, and production rollback gates
  remain.

- **2026-08-30:** Storypath keyless cache replay establishes Mistral-wiki as a
  strict improvement over raw Mistral on this prose canary. Artifact
  `blake3:5b1074c707e16069c8ea0172cd90557f57c4eee32c77ff4c886c0d96bca35568`
  removes exactly 16 provider-typed running-footer blocks / 166 UTF-8 bytes and
  nothing else: heading sequence, outline title/levels, image links/assets,
  lists, emphasis, prose, 8 mappings, and 19 outline nodes remain. Datalab-wiki
  `blake3:646bb02b391704d6f27af4e52eb0bc8ba01efc3c7c9d78a879bbd2d821ba36ea`
  has byte-identical Markdown to raw Datalab because none of its description or
  asset defects satisfies conservative structural rules. Both replay
  deterministically and pass Vulcan validation/import. Do not spend reviewer
  effort on an unchanged Datalab candidate or already-adjudicated Mistral
  footer removal. Use Mistral-wiki as the hosted quality-tier leader for this
  class; Marker 1 remains the local/privacy fallback and list-syntax leader.

- **2026-08-30:** The wiki-normalized London Falling campaign is
  decision-complete after two fully rated pages exposed the repeated structural
  difference; do not copy scores onto pages 3-8. Strict validation accepted 26
  ratings, 14 N/A values, and 40/160 slots. A=Mistral-wiki and B=Datalab-wiki.
  Both score 5.0 for text, reading order, mapping, and the one applicable asset
  rating. Mistral wins tables 5.0/3.0 and wiki utility 5.0/4.0; Datalab wins
  hierarchy 5.0/3.0. Select Mistral-wiki for complex-table routing because it
  consistently emits the reviewed semantic HTML; Datalab correctly leaves most
  inconsistent grids unchanged and remains a hierarchy-strong challenger.
  Datalab's unexplained `阴森` token originates in its native English image alt
  text, not BlobForge normalization; provider descriptions are unverified
  evidence.

- **2026-08-30:** Semantic-table review previews must set foreground and
  background colors together. The surrounding review UI is dark and its text
  color otherwise inherits into light table cells, producing unreadable
  white-on-white content. The current preview uses explicit light body/header
  palettes scoped under `.table-previews`; preserve this invariant in future
  themes and generated campaigns.

- **2026-08-30:** Wiki-oriented Mistral and Datalab composites are now
  evaluation-ready without repurchasing provider work. Artifact recipe identity
  is separated from the frozen raw-provider digest used as the durable cache
  key. Mistral wiki v1
  (`blake3:52d29542b2171c154f877d59e4e16019b85296ac4d12a6de97d2080a81a18dba`)
  uses typed blocks/geometry; Datalab wiki v1
  (`blake3:fcc851f8e84d0c22e44200208ccd50d76319c5aec6d3bc1de6bc9b026d3ac502`)
  uses only exact caption duplication and conservative recurring-image
  evidence. Both serialize validated merged grids as allowlisted semantic HTML,
  declare `raw-html`/`semantic-html-table-v1`, retain native responses, and
  calculate page spans after cleanup. Keyless real-cache replay produced
  deterministic London Falling artifacts
  `blake3:1a6b3dad11b78eb1c2912bab9f87b6c23aeb77dde383a33c011bc183f8866534`
  and
  `blake3:bbed7f449c82e4c53f7aa552f1431ab434fe840f5c8b9d5c5159b6effc4b3fab`;
  both pass Vulcan validation/import with 8/8 mappings, one referenced asset,
  and preserved tables/colspans. The fresh blinded campaign is
  `blake3:efd4e84ff559de4e497fb51ae406b288f7de91224bb223288bc84fb0af8853ce`.
  Blank pipe cells are currently an evaluated colspan hypothesis, not ground
  truth; human review and a hidden holdout remain production gates.

- **2026-08-30:** The blinded London Falling hosted table result is
  decision-complete with three numerically rated pages and a reviewer finding
  that the remaining five repeat the outcome. Strict validation accepted 80
  ratings, 36 N/A values, and 116/320 slots; do not extrapolate scores after
  unblinding. A=Marker, B=Mistral, C=Datalab, D=Docling. Table means are
  1.0/5.0/4.0/1.0 and wiki utility 1.0/4.0/4.0/1.0. Mistral is the provisional
  complex-table backend; Datalab is the challenger after description isolation.
  Mistral retains repeated headers; Datalab descriptions bleed; Mistral,
  Datalab, and Docling retain irrelevant footer logos; Docling also emits table
  screenshots. Complex merged headers need an output representation beyond
  pipe Markdown. Use allowlisted semantic HTML tables for required
  `colspan`/`rowspan` only after Vulcan and renderer compatibility tests, escape
  provider content, and compute final source spans after deterministic table
  serialization.

- **2026-08-30:** The four-way London Falling table challenger is ready for
  blinded review under campaign
  `blake3:9a366ab22d1557b1f665b7c76f08ab90db14b670ad0c4d823ed043a8a6b0d3a1`.
  Its 8-page fixture maps to original pages 12, 23, 31, 38, 64, 78, 90, and 92;
  all page pairs are raster-identical at 72 DPI. Marker/Docling/Mistral/Datalab
  took 736.1/192.8/7.5/25.2 seconds and emitted 303/235/299/299 apparent table
  rows, but counts are not fidelity. Mistral list cost was $0.032 and Datalab
  billed $0.06. All artifacts pass Vulcan with 8/8 mappings. Datalab had the
  same live-versus-cache JSON key-order drift previously found in Mistral;
  always sort native response keys and regression-test native bytes. Canonical
  Datalab identity is
  `blake3:3a4551a34a4ba805287e16ac9a1a4b4794d48bcb720dec05ca28b7046076dafa`
  and replays byte-identically without keys or repurchase.

- **2026-08-30:** The complete hosted Storypath review-v2 export validates at
  8/8 pages, 252 numeric ratings, 68 N/A values, and 320/320 completed slots.
  Unblinding maps A=Marker 1.10.2, B=Mistral OCR 4.1, C=Datalab Convert
  accurate, and D=Docling 2.122.0. Mistral is the canary quality leader: text
  5.0, inline formatting 5.0, assets 4.857, and wiki utility 5.0. It still
  retains repeated footers and occasionally emits non-Markdown list symbols.
  Marker has excellent inline formatting and lists but retains some dingbats as
  `Y`; Docling loses inline formatting; Datalab descriptions contaminate body
  text, uses nonstandard bullets, and can extract a useless whole-page asset.
  The blinded London Falling table verdict is that both local candidates are
  unusable for structured wiki tables. Candidate B unblinds as Docling: its
  screenshots preserve readability but not cell semantics. Test hosted
  table-focused challengers before selecting table routing.

- **2026-08-30:** The complete frozen `pdf-enrichment/v1` backfill is accepted.
  Catalog state reached all 1,377 converted with no pending, processing, or
  failed derivative. The independent read-only `migrate enrich-verify` pass
  checked every archive/schema/source/base/recipe lineage: 1,377 valid and zero
  invalid. Phase 1 is complete; subsequent recipe work must not rewrite these
  immutable derivatives.

- **2026-08-30:** The first hosted Storypath canaries are complete and cached.
  Mistral OCR 4.1 processed 8 pages in 6.9s at a $0.032 list-price estimate and
  packages as
  `blake3:cb906843d778f3328175fa869251e39520015d1aec8bddef59b9cab5915112e8`.
  Datalab Convert accurate reported 13.12s provider runtime and an exact $0.06
  final charge, packaging as
  `blake3:2071347f7728035763d51c2de451dd6fde7c0542fb9e30891f3abc5e4982522f`.
  Its real response omitted documented list cost, parse-quality score, and
  model versions, so those remain unavailable rather than inferred. Both pass
  BlobForge/Vulcan with 8/8 mappings and produce byte-identical cache replays
  with both keys unset. Mistral native response JSON must be serialized with
  sorted keys: live SDK insertion order otherwise changed MDAF identity versus
  cache replay. The first unscored output was superseded without repurchase.

- **2026-08-30:** The canonical four-candidate Storypath review-v2 campaign is
  `blake3:4f10cea83474b0a728199b05707d5eb3188bb0854bc798759c9aeb2cf5a900cc`.
  It compares Marker 1, Docling, Mistral, and Datalab accurate across pages 1-8
  without public engine names. The London Falling Marker/Docling review-v2
  campaign is
  `blake3:f31eabad8aacc5f4b10ebb96976d5a5491048252a6813df593d74458cab26d67`
  on pages 12, 23, 31, 38, 64, 78, 90, and 92. Do not inspect either key before
  complete score export.

- **2026-08-30:** Enterprise Marker 1 completed 98-page London Falling in
  3,260.3s with 819,143 Markdown bytes, 65,546 words, 487 outline nodes, 1,584
  table rows, 89 assets, and 98 exact page mappings. Docling produced 1,723,253
  bytes, 115,960 words, 467 nodes, 1,311 table rows, and 163 assets. The large
  discrepancy may be retention, duplication, captions, or overcapture and must
  be decided by the blinded campaign, never structural counts alone.

- **2026-08-29:** The first Datalab hosted evaluator is frozen for quality
  testing in `accurate` mode at recipe
  `blake3:c1dc8c06bf29a7a5f1639a4a0bdfc8be1250745d5f6e13438c68b1e38df9bc6f`.
  It caches submitted and complete Convert v1 responses, validates exact
  pagination/assets/costs, and enforces the official 200 MB / 7,000-page limits.
  Datalab exposes no pre-request quote or provider-side dollar cap: the local
  page cap bounds exposure, while `final_cost_cents` can only be asserted after
  purchase. Keep the first run inside the $5 trial. Both API keys remained
  invisible to local and SSH subprocesses during the guarded plans, so no paid
  Mistral or Datalab request occurred.

- **2026-08-29:** The Debian 13 enterprise desktop is a viable 32-GiB CPU
  evaluation worker. On the eight-page Storypath canary, Docling took 115.0s
  and Marker 1 took 100.8s; their Markdown/assets/source maps/native evidence
  exactly match the earlier runs. On 98-page London Falling, Docling completed
  in 1,015.9s with 98 exact page mappings, 467 outline nodes, 1,311 table rows,
  and 163 assets and passed independent Vulcan validation. Docling used about
  3.0-3.5 GiB RSS. The concurrent Marker run completed in 3,260.3s and reached
  about 13.8 GiB RSS. Concurrency fits but distorts timing and
  reduces safety margin; schedule at most one large Marker job at a time on a
  32-GiB worker.

- **2026-08-29:** Review format v2 adds `inline-formatting` to the original nine
  dimensions. New campaigns default to v2; stored v1 keys/results continue to
  validate against their exact original dimension contract. A real generated
  v2 smoke contains ten dimensions without changing earlier campaigns.

- **2026-08-29:** The reviewer observed during the blinded Storypath review that
  one structured candidate preserved emphasis better, but omitted it from the
  browser export. Unblinding attributes that candidate to Marker 1. A subsequent
  artifact check supports the observation: Marker emits 39 bold and 17 italic
  Markdown spans, including meaningful list labels, ability names, headings,
  and a quotation; Docling emits none. Preserve this as qualitative blinded
  evidence without reconstructing a numeric score after export. Add an
  `inline-formatting` dimension in review format v2 without invalidating stored
  v1 campaigns.

- **2026-08-29:** The reblinded Storypath pages 2-8 result validates with all 7
  pages reviewed, 54 numeric ratings, 27 N/A values, and qualitative notes on
  the four pages whose repeated selectors were intentionally not filled. Its
  private mapping is A=Docling 2.122.0, B=Marker 1.10.2, C=Poppler 25.03.0.
  Combining the independently blinded numeric pages 1-4: Marker leads text
  4.50 and wiki utility 4.75; Docling leads reading order 4.75 and hierarchy
  4.75; both score lists 4.00 and page mapping 5.00; Poppler scores 1.00 except
  mapping 5.00. Marker is the provisional quality leader for this rulebook but
  not a default-recipe decision. It over-promotes headings and mishandles one
  cross-page list continuation. Docling flattens one important heading
  relationship and uses inaccurate middle dots. Neither preserves a boxed
  callout container. Marker produces better page-eight structure and a
  1632x1275 image versus Docling's 611x470 image. Tables and references remain
  unevaluated. The browser now offers explicit previous-page rating copy: it
  deep-copies ratings only, never notes, never scores a page on view, disables
  itself without a rated predecessor, and confirms before replacing ratings.

- **2026-08-29:** The first submitted blinded human score covers page 1 of the
  eight-page Storypath canary. Unblinding maps A to Poppler, B to Docling
  2.122.0, and C to Marker 1.10.2. Docling scored 4 for text/reading order while
  Marker scored 3; both scored 4 for hierarchy, lists, asset-link presentation,
  and wiki utility. Poppler scored 1 on every rated content/structure dimension
  but 5 for page mapping; every candidate scored 5 for page mapping. This is a
  single-page directional result, not a recipe verdict. Asset fidelity was not
  actually observable in the current raw-Markdown UI, so those scores describe
  embedding syntax only. All candidates expose a `Y` list prefix and the PDF
  embeds `FantasyRPGDings`; treat this as a font/layout-aware normalization
  target, never a global character substitution. The next reviewer revision
  added inline anchors, explicit N/A, blinded magic-checked raster inspection,
  partial-score resume, and strict result ingestion. Its `local-v6` bundle
  retains the same campaign digest, so the first export remains compatible.
  New private keys retain the raw label seed: result summarization recomputes
  the seed hash, campaign digest, deterministic label assignment, allowed
  pages/dimensions/candidates, score values, and N/A coverage before unblinding.
  Older keys that cannot prove their assignment fail closed.
  Once a campaign is unblinded it must not collect more human scores; use
  `--random-seed` to create a fresh private assignment for the remaining pages.

- **2026-08-28:** BlobForge has a runnable blinded review vertical slice.
  `blobforge review-bundle` validates that two or more MDAFs share the supplied
  source digest, derives exact page text from final UTF-8 source-map spans,
  rejects ambiguous mappings that cover multiple pages, deterministically
  shuffles labels, copies the source PDF, and emits a local
  browser UI with nine scoring dimensions, notes, autosave when available, and
  JSON export. Public files omit engine names, paths, artifact identities,
  tools, and models; the separate unblinding key is mode `0600`. The first real
  8-page Poppler/Marker 1/Docling campaign is
  `blake3:77957f19a06b1ddf8288840aa59f2992482eeeab004314134496c9f90e33a468`
  under `.blobforge-migration/evaluations/reviews/...local-v3/`. Generated JS
  was executed with jsdom: 8 page choices, 3 columns, 27 score controls, and a
  page-1 PDF target. The matching Mistral plan is 8 pages / $0.032 list price
  with an explicit $0.04 ceiling; it remains blocked only because
  `MISTRAL_API_KEY` is not configured. Hosted execution now requires
  `--confirm-api-rights`; `--plan` never contacts the provider.

- **2026-08-28:** The full enrichment backfill found that Poppler 25.03.0 can
  emit raw XML-forbidden C0 glyph bytes (observed `0x18`) inside bbox XHTML;
  ElementTree then rejects an otherwise usable PDF. Before parsing, BlobForge
  now removes only `00-08`, `0B`, `0C`, and `0E-1F`, retaining XML whitespace
  and all other bytes, and records a nonzero removal count in native evidence.
  A real 227-page failure then extracted 1,757 blocks after removing 9 bytes.
  This compatibility fix retains the frozen recipe digest because it is a
  byte-for-byte no-op for every previously successful input and no artifact
  existed for the formerly undefined failure path. Failed attempts remain in
  the append-only ledger; restarting `enrich --all` retries only non-converted
  rows. A workspace `flock` now enforces one enrichment runner, and startup
  closes abandoned attempt rows as `interrupted` before creating retries.

- **2026-08-28:** The Mistral OCR 4.1 evaluator is quota-safe for bounded
  trials. Frozen recipe
  `blake3:982a97ca1d45f5a0ac30dd8c7507efb594688d1b949f406ef4620f3352e723c7`
  selects SDK 2.9.4, `mistral-ocr-4-1`, blocks, block confidence, images,
  deterministic asset rewriting, and page mappings. Before any local
  validation or packaging, a successful response is atomically cached by
  exact source SHA-256 plus recipe/request identity; a per-key Linux kernel
  lock prevents concurrent duplicate purchases, cache hits need no API key,
  and corrupt/incomplete entries fail without repurchase. The native rendition
  retains blocks, geometry, usage, and returned model, but published mappings
  remain page-only until exact block-to-final-Markdown spans are proven.
  Actual billing/credits require a separate ledger, and Mistral exposes no
  immutable checkpoint digest, so this is evaluation-ready rather than fully
  production-promoted.

- **2026-08-28:** `pdf-enrichment/v1` is frozen for born-digital legacy
  backfill at
  `blake3:0e7e6c1ba4bb6a8920a58cd08fe3c957bd48b729cbccc5733ffec3d47876a569`.
  Canonical JSON is packaged at `blobforge/recipes/pdf-enrichment-v1.json`, and
  execution fails closed unless Poppler `pdftotext` is exactly 25.03.0.
  Enrichment attempts append duration, process-tree peak RSS, page count,
  output size, status, and error to SQLite; immutable PDF page counts are
  cached. Concurrent work uses isolated processes and schedules at most one
  large input (300+ pages or 64+ MiB) at once. A real 8/70/256-page canary
  measured 51.8/138.0/354.2 MiB peak RSS and retained all prior identities;
  all 15 reviewed artifacts remained valid. The 32-GiB host is approved for
  `--jobs 2`, not higher concurrency. The append-only `--all` backfill is
  authorized; verify zero failed/processing rows and all artifacts afterward.

- **2026-08-28:** The current PDF-enrichment and local compatibility program is
  explicitly scoped to born-digital illustrated pen-and-paper rulebooks with
  usable embedded text. Image-only and scan-heavy PDFs are not an acceptance
  gate; any future BlobForge OCR path must be a distinct recipe with its own
  provenance and evaluation. Mistral and Datalab API evaluation may be spread
  across subscription/quota periods using available promotional credits.
  Provider responses must be cached by `(source_digest, recipe_digest)` before
  packaging, and every attempt must retain page usage, normalized list-price
  cost, billed amount, and credits applied separately. Hard page/spend caps and
  rights checks still apply, and credentials/account balances never enter
  recipe JSON or MDAFs.

- **2026-08-28:** Corrected PDF-enrichment recipe
  `blake3:0e7e6c1ba4bb6a8920a58cd08fe3c957bd48b729cbccc5733ffec3d47876a569`
  passes the native-text canary but is not yet frozen for bulk backfill. Its
  generation-2 aligner uses nearest trusted anchors on both sides, monotonic
  pages, retained Poppler line/word evidence, disjoint word-region refinement,
  separately gated region publication, and honest page-only fallback. Across
  15 complete books / 1,957 pages it mapped 20,047 of 31,997 blocks: 13,044
  regions and 7,003 page-only. All artifacts pass BlobForge, catalog/lineage,
  and independent Vulcan validation; repeat runs preserve MDAF identity; an
  invariant audit found zero page regressions and zero duplicate rectangles;
  and 51 visually adjudicated mappings were correct at advertised precision.
  A five-book/1,804-page expansion took about 23 minutes with concurrency two,
  with 400–500-page books taking roughly 15–18 minutes. Scan/OCR coverage was
  subsequently declared outside this born-digital recipe's applicability; add
  runtime/peak-memory recording and size-aware concurrency before freezing or
  starting the complete backfill.

- **2026-08-27:** Manual inspection rejected enrichment recipe
  `blake3:cf33db6438b2a2fbe1e44538bf05cb64a40bf9d88e3f211b1276933c580e1598`
  for bulk backfill despite all ten canary artifacts passing structural and
  Vulcan validation. A 35-mapping visual audit across all documents found two
  page-order regressions and six reused-rectangle groups involving 13 mappings;
  lower-confidence mappings often published whole Poppler blocks for partial
  Markdown, while exact repeated labels could still score 1.0 at the wrong
  occurrence. Of 79 mappings below confidence 0.90, 68 had reconstructed text
  similarity below 0.90 and 60 had normalized length ratio below 0.80. The next
  recipe must use preceding and following anchors, word/line geometry,
  separately calibrated page/region confidence, honest page-only fallback, and
  explicit split/join or geometry-reuse handling. The current ten derivatives
  remain immutable experimental evidence; the remaining 1,367 stay gated.

- **2026-08-27:** PDF enrichment now has an implemented, local-only vertical
  slice. Poppler bbox-layout evidence, loss-aware Markdown segmentation,
  token-indexed monotonic alignment, exact UTF-8 spans, conservative
  page/rectangle publication, clipping diagnostics, native evidence, and
  derived-artifact lineage feed the shared MDAF builder. The catalog tracks
  `(legacy_sha256, enrichment_recipe_digest)` resumably, explicit hashes or
  `--limit` bound canaries, and unbounded work requires `--all`. BlobForge now
  rejects unknown provenance inputs/outputs/dependencies like Vulcan. A real
  10-rulebook/153-page canary under recipe
  `blake3:cf33db6438b2a2fbe1e44538bf05cb64a40bf9d88e3f211b1276933c580e1598`
  produced 10/10 dual-validator-valid artifacts with 59.9% block and 64.7%
  semantic-byte coverage. Coverage is not accuracy: the subsequent manual
  review rejected this recipe, so a corrected recipe and repeat review remain
  the gate before freezing `pdf-enrichment/v1` or running the remaining 1,367
  artifacts. Earlier experimental rows remain in the ignored local catalog as
  honest failed/interrupted development evidence.

- **2026-08-27:** The conversion program now has an ordered, repository-backed
  roadmap. Contract and rubric freeze precede a 10-20-document enrichment
  canary; only a reviewed `pdf-enrichment/v1` may run across all 1,377 legacy
  artifacts. Enrichment produces a new immutable derived artifact from the
  exact source and base artifact, never an in-place rewrite or invented
  recovery of unavailable historical Marker/model versions. Conversion and
  enrichment share a normalized evidence boundary: trustworthy native geometry
  is preserved, while Markdown-only output uses modular PDF evidence extraction
  and monotonic alignment. Accuracy and coverage remain separate; unsupported
  precision stays unmapped. The first evaluation round is legacy/Marker 1,
  Marker 2, Docling, Datalab, and Mistral OCR, with MinerU conditional on an
  identified quality gap. Routing and production canaries follow blinded
  corpus and holdout evaluation.

- **2026-08-27:** The self-hosted root is now a complete, admin-only operations
  console rather than a JSON-navigation landing page. It manages paginated
  jobs, streaming source uploads/downloads, fenced requeue/retry/priority/delete
  actions, failure/artifact history, dynamic `bfw_` worker credentials,
  immutable recipe selection/retirement metadata, and revocable `bfa_` admin
  tokens. Plaintext credentials are shown once and stored hashed. Environment
  workers remain deployment-owned; deleted object bytes move to recoverable
  local trash. OIDC mutations require exact same origin and all administrative
  mutations are audited. Recipe canonical JSON remains immutable because its
  digest is an output identity, not editable application configuration.

- **2026-08-27:** Authentik 2026.8 does dispatch outgoing SCIM membership
  changes in real time, but the membership path only resolves existing
  `SCIMProviderUser` mappings. The first addition to an application-filtered
  access group makes a user newly in scope, so the task may report success
  after only reading the remote group and never create the user or update its
  membership. The built-in full sync runs every four hours. Preserve the
  narrow access-group scope; use Gandalf's forced full sync for immediate
  recovery and pursue ordered user-before-membership provisioning plus a short
  reconciliation safety net. BlobForge browser errors are now content
  negotiated: browser navigation receives no-store HTML recovery pages, while
  API and SCIM requests retain JSON.

- **2026-08-27:** BlobForge OIDC authorization intentionally consults only its
  active local SCIM user/group state; Authentik group membership or identity
  claims alone cannot authorize a session. After changing membership in the
  Authentik `blobforge-admin` access group, operators can immediately converge
  production with Gandalf's targeted `blobforge_scim` tag and
  `blobforge_scim_force_sync=true`. The private SCIM readiness check succeeding
  only proves reachability; verify the expected membership count in
  `scim_group_members` when diagnosing a callback denial.

- **2026-08-27:** The self-hosted server root must not fall through to
  FastAPI's JSON 404. It now redirects unauthenticated OIDC deployments to
  `/auth/login` and serves authorized sessions/client tokens a private,
  no-store coordinator overview with queue counts and API navigation under a
  restrictive CSP. This is deliberately a landing page, not a claim that the
  Bunny-era file library, worker enrollment, or token-management console has
  been ported. Production access still requires membership in the configured
  SCIM role group.

- **2026-08-27:** The complete local recovery unit is verified on Citadel at
  `/srv/blobforge`: all 3,188 manifest entries match, SQLite `quick_check` is
  `ok`, and counts remain 1,808 sources/jobs, 3,616 aliases, 1,377 legacy MDAFs,
  431 queued raw-only jobs, 1,808 source objects, 1,377 artifact objects, and
  zero pending/orphan objects. GH Actions run `33069776111` published the
  digest-pinned server image
  `ghcr.io/tionis/blobforge@sha256:97f764f71d329c25c0783617595d7ee4b3ec5c586a2e3d481d7612c0ab56f330`
  (revision `6b8aa75`).
  Citadel's coordinator and private SCIM backchannel are healthy; Authentik and
  BlobForge both restrict interactive management to `blobforge-admin`.
  Quadlet environment values must escape backslashes and double quotes because
  canonical JSON otherwise becomes invalid in the generated Podman command.
  Canonical DNS now replaces the legacy Bunny Pull Zone record with a CNAME to
  Citadel. The explicitly approved shared Caddy restart activated the validated
  configuration; public TLS/API health, Authentik redirect, public SCIM 404,
  and an existing Citadel endpoint pass. Deployment credentials were
  rotated after an unsafe status diagnostic rendered their environment; never
  use full unit status/ExecStart output for secret-bearing Quadlets.
  The declared Restic profile is installed: its first quiesced snapshot
  succeeded in 55 seconds, committed 715 new repository bytes through existing
  deduplication, and resumed BlobForge healthy. The isolated restore recovered
  32.165 GiB and passed restored SQLite verification in 201 seconds. Daily
  backup and weekly restore-test timers are enabled and publish success metrics.

- **2026-08-27:** The local coordinator migration is complete at the ignored
  `.blobforge-migration/local-server-data` recovery unit. Two fail-closed full
  stage passes validated all 1,377 MDAF/source pairs; the first imported all
  1,377 and the idempotency pass skipped all 1,377. The raw recovery imported
  the remaining 431 of 1,808 sources, and its repeat skipped all 1,808. SQLite
  quick-check passes with 1,808 sources/jobs, 3,616 aliases, 1,377 done legacy
  MDAFs, 431 queued raw-only jobs, and no pending/orphan objects. Every artifact
  is explicitly legacy/Marker/unavailable-version and carries the canonical
  migration recipe and partial mapping strategy. A coordinator restart canary
  opened the state and persisted its capability key. The 3,188-file, ~33 GB
  recovery unit has a verified relative BLAKE3 manifest whose digest is
  `b654923b59e24bd5709aab3e8a9803b351f5c03cba48596baf3df876c36ddf23`.
  Interactive management can be limited to one exact SCIM group by mapping
  only that group to `admin`; Gandalf should independently bind the Authentik
  application to the same group. Existing tags are descriptive only and must
  not become ACLs; use explicit private collections and SCIM-group role
  bindings for resource-level authorization.

- **2026-08-27:** The local coordinator now catalogs migrated MDAFs with an
  explicit legacy flag, Marker backend, unavailable historical converter
  version, canonical migration recipe, and recovered mapping/version summary;
  the MDAFs themselves already retained the original Markdown/info rendition
  and complete known provenance. Page-span coverage is evidence-limited: page
  anchors are exact spans, while books without anchors receive only exact
  TOC-heading page/polygon matches. Converter selection accepts an exact recipe
  digest or an unambiguous active backend. Workers advertise multiple
  `(backend, recipe, media types, artifact type)` capabilities and the lease
  returns the selected capability, enabling future interleaved media work on
  one host; the current implementation still dispatches Marker/PDF only. The
  self-hosted API supports OIDC browser sessions and SCIM 2.0 lifecycle/group
  provisioning. Authorization rechecks active SCIM state on every request, and
  OIDC `sub` must match SCIM `externalId`. Citadel deployment should follow
  Gandalf's Todo Quadlet/OIDC/SCIM/backup pattern, with SCIM private-only and
  `/srv/blobforge` backed up as one quiesced recovery unit.

- **2026-08-27:** The self-hosted backend now has a compatibility-preserving
  vertical slice: FastAPI exposes the existing worker/client control protocol,
  SQLite WAL owns sources, digest aliases, media types, jobs, fenced leases,
  worker credentials, failures, and immutable artifact records, while a local
  directory provides streamed atomic source/artifact storage through scoped
  HMAC transfer URLs. The service is intentionally single-instance per data
  directory; multiple conversion workers are supported, but active-active API
  replicas require a future PostgreSQL move. Current Marker workers advertise
  only `application/pdf`. The verified v2 importer records BLAKE3 canonically,
  retains historical SHA-256 aliases/compatibility keys, imports `mdaf/v1`
  artifacts idempotently, and never mutates Bunny/S3. GHCR `latest` is the
  lightweight server; heavy images use `worker` and `worker-cuda`. Automatic
  Bunny deployment is disabled during cutover. The MDAF stage covers 1,377 of
  the 1,808 mirrored raw sources; a separate importer verifies and queues the
  remaining 431 with `metadata-unavailable`, because omitting them would make a
  seemingly successful backend cutover incomplete. Container context exclusions
  must retain `.blobforge-migration`, `references`, `evaluators`, `dist`, and
  dot-prefixed environment files: this checkout currently contains about 46 GB
  of local corpus/model data plus provider credential files that must never be
  sent to an image builder.

- **2026-08-27:** The complete legacy migration now has 1,377/1,377 converted
  artifacts and zero failures. The fail-closed local v2 stage contains 1,377
  source objects, 1,377 MDAFs, one canonical migration recipe, and one
  checksummed run manifest under the exact proposed relative S3 keys. Its recipe
  digest is `blake3:8822289b4860301f73b64a2139a3559f2026793a48135fc13b83bc84a67b0c39`.
  A staged 111-member artifact passed Vulcan with 415 interval/polygon mappings
  and 415 outline nodes. No production object or coordinator row was changed.
  Full-corpus validation must cache immutable schemas and test UTF-8 span
  boundaries via continuation bytes; decoding every prefix is quadratic on
  heading-heavy books.

- **2026-08-27:** Comparable converter output needs a backend-neutral semantic
  outline even when an engine returns only Markdown headings. The shared MDAF
  packager now derives UTF-8-byte-aligned outline nodes from non-empty ATX
  headings unless the adapter supplies richer hierarchy evidence; it never
  fabricates source locators. Vulcan dry-run import of the corrected eight-page
  Docling canary successfully planned one root, 18 section notes, and two assets.
  On that book Docling took 269.3s versus Marker 1's 519.2s, with nearly equal
  word/heading/asset counts; those structural counts still require blinded
  reading-order and wiki-quality review.

- **2026-08-27:** Full migration acceptance requires a read-only second pass,
  not merely successful writes. `blobforge migrate verify` independently reads
  each converted MDAF and cross-checks archive/schema semantics, logical MDAF
  identity, source BLAKE3, and the legacy SHA-256 alias against the WAL catalog.
  Two corpus artifacts also revealed image-only/HTML-only Markdown headings;
  they remain intact in primary Markdown but must be omitted from the semantic
  outline because MDAF v1 requires non-empty outline titles.

- **2026-08-27:** The local MDAF transition vertical slice is operational. A
  read-only `rclone copy` mirrored all 3,634 `blobforge:pdf` objects (31.98 GiB):
  1,808 raw SHA-256 PDFs and 1,377 legacy ZIPs, with every ZIP paired. The
  resumable SQLite migrator verifies the legacy SHA-256, calculates BLAKE3,
  retains original Markdown/metadata/assets as native evidence, records the
  unknown historical Marker/model versions as `unavailable`, derives complete
  Markdown outlines, and publishes only exact page anchors or exact
  TOC-heading page/polygon matches. It never writes S3. A 20-artifact canary had
  no failures; a representative real artifact passed both BlobForge's bundled
  Vulcan-schema validator and Vulcan's Rust validator with 19 source mappings
  and 19 outline nodes. The final local bulk run is resumable through
  `.blobforge-migration/catalog.sqlite3`.

- **2026-08-27:** Converter tests now cross a versioned filesystem ABI and one
  shared MDAF builder; adapters cannot package artifacts themselves. Poppler,
  Marker 1.10.2, Marker 2.0.0, Docling 2.122.0, and Mistral SDK 2.9.4 have
  separate uv locks. Local ML environments must explicitly select PyTorch's CPU
  index: unconstrained PyPI resolution selected CUDA 13 and several GiB of
  unusable NVIDIA libraries. On `assets/lorem.pdf` (2 pages), Poppler took 0.9s,
  Docling 40.2s, and Marker 1 175.6s; all produced valid two-page MDAFs accepted
  by Vulcan. The 8-page rulebook Docling run took 262.9s and preserved two
  images. Docling's referenced-image export writes absolute temporary paths,
  so the adapter must rewrite them before final byte-span calculation and the
  validator now rejects absolute/file Markdown targets. Exact downloaded model
  checksums remain required before production; package pins alone are not
  sufficient provenance.

- **2026-08-27:** The frozen priority corpus is 43 PDFs / 9,465 pages /
  1,294,553,125 bytes with manifest identity
  `blake3:44b252c25c8a61dc2771c337cfca9d6b43734cefbac44f2d50b8e5130a3e2b35`.
  BlobForge now has one-pass BLAKE3+SHA-256 hashing, algorithm-specific xattrs,
  and an additive algorithm-keyed SQLite digest cache. The v2 relative object
  namespace is `store/v2/{sources,recipes,artifacts,checkpoints,migrations}`
  with explicit algorithms and two-hex sharding; the coordinator database, not
  key parsing, remains authoritative.

- **2026-08-27:** The 32-GiB machine has enough hardware and system tooling for
  the CPU evaluation tier, but scored conversion tests are not ready. BlobForge
  has no MDAF writer/validator, converter subprocess ABI, frozen BLAKE3 corpus
  manifest, isolated Docling environment, or executable comparison harness;
  running now would only create legacy Marker ZIPs. The installed Vulcan 0.1.0
  binary lacks `artifact`, although the current Vulcan checkout contains the
  validator/import implementation and tests. The corpus changed once more when
  redundant Rigger variants were removed: it is now 43 exact-byte-distinct PDFs,
  9,465 pages, and 1,234.58 MiB, with the bookmarked Rigger source remaining.
  The minimum launch sequence and gates are documented in
  `docs/conversion_test_readiness.md`; current costs are $37.86 Mistral standard,
  $47.33 annotated ceiling, $94.65 Google Layout, and $141.98 AWS Layout+Tables.

- **2026-08-27:** Local hydrated-output maintenance is grouped under
  `blobforge hydrated`. `clean` recursively removes only PDF-anchored sibling
  `<stem>.md` and `<stem>.assets/` outputs; `textpack` replaces each pair with a
  TextBundle v2 compressed `<stem>.textpack` containing `text.md`, `info.json`,
  and `assets/`. Both operations are previews unless `--execute` is supplied.
  TextPack creation uses a same-directory temporary archive, validates required
  entries, metadata, and CRCs before atomic publication, skips existing targets
  unless `--force` is explicit, and rejects symlinks in source Markdown or
  assets so recursive maintenance cannot escape the selected tree.
  `clean-textpacks` removes only same-stem TextPacks next to discovered PDFs.
  `unpack` validates a Markdown TextBundle v2 archive, rejects unsafe ZIP
  members, restores `<stem>.md`/`<stem>.assets/`, and removes the archive only
  after success; it skips existing outputs unless `--force` is explicit.

- **2026-08-27:** The expanded priority corpus currently contains 45 readable
  PDF paths, 9,853 raw pages, and 1,272.64 MiB. SHA-256 found one exact 194-page
  duplicate pair (`Rigger_5.0_in_Tits-o-Vision` and `Rigger_5.0_with_bookmarks`),
  leaving 44 exact-byte-distinct sources and 9,659 billable pages. A third
  194-page Rigger PDF is byte-distinct but should be reviewed as a possible
  duplicate edition. Compared with the 2026-08-26 inventory, 30 paths were
  added and the two Trinity Continuum PDFs (547 pages) are absent. At current
  published rates the deduplicated corpus costs $38.64 for Mistral OCR 4.1
  standard, $48.30 at the conservative annotated rate, $96.59 for Google Layout,
  or about $144.89 for AWS Layout+Tables in the published US West example.
  Mistral's general pricing table lists OCR Batch at $0.40/1,000 pages while its
  Batch guide says 50% off; treat the lower projected $3.86 total as unverified
  until a metered pilot. The readiness finding above supersedes these transient
  45-path counts after the redundant Rigger variants were removed. Full details
  are in `docs/rulebook_corpus_cost.md`.

- **2026-08-27:** The 17 priority rulebooks will serve as both valuable
  production inputs and the stable full-book converter acceptance corpus, but
  are not themselves ground truth. Existing Markdown is a regression baseline;
  approximately 5-10 pages per book form a labeled adjudication set with a
  smaller hidden holdout. Every engine must run through a versioned subprocess
  adapter and emit a private ConversionBundle; one shared builder alone creates
  and validates MDAF, canonical BLAKE3 identities, final Markdown byte mappings,
  and provenance. Heavy engine dependencies remain in separate uv environments
  or images. The design and first vertical slice are in
  `docs/converter_adapter_architecture.md`. The expanded-corpus finding above
  supersedes the counts and adjudication-set size in this initial decision.

- **2026-08-27:** The available fleet also includes a Windows machine with 24
  GiB RAM and a GeForce GTX 1070 (8 GiB, Pascal/compute capability 6.1), plus a
  GPU-less 32-GiB desktop. The desktop is the preferred full-corpus CPU worker.
  The 1070 can be used under WSL2 for Docling's standard pipeline, its small
  GraniteDocling-258M/SmolDocling-256M VLMs, and an experimental Surya 2
  llama.cpp/GGUF path. It cannot use vLLM, which requires compute capability
  7.0+, and should use pinned CUDA 12.x because CUDA 13 removed Pascal library
  and offline-compilation support. A rented 48-80 GiB GPU is now an optional
  heavyweight-model tie-breaker, not a prerequisite; hosted APIs cover those
  model classes in the first evaluation round.

- **2026-08-26:** The current BlobForge workstation is an Intel i7-8650U
  (4C/8T) with 31 GiB RAM and 392 GiB free disk; Docker/Podman and Poppler are
  present, but no NVIDIA device is currently usable. It is suitable for Docling
  standard, Marker 2 fast/no-OCR, MinerU pipeline, PP-StructureV3 CPU
  correctness, and deterministic baselines, but not a representative throughput
  host. Marker 2 fast with OCR additionally needs a pinned llama.cpp
  `llama-server`; balanced needs a Surya vLLM server and is intended for NVIDIA.
  The repository `.venv` must remain on Marker 1, so every evaluation engine
  uses an isolated uv project or pinned container. This initial finding's
  48-80-GiB recommendation applies only to a single host intended to cover every
  heavyweight candidate; the 2026-08-27 fleet-specific plan supersedes it for
  the first evaluation round. The full candidate matrix, setup requirements,
  and staged evaluation are in `docs/local_converter_evaluation.md`.

- **2026-08-26:** The priority corpus at
  `/home/eric/rulebooks/rulebooks` contains 17 readable PDFs, 3,060 pages, and
  488.5 MiB. Every PDF yields substantial text-layer output and every source has
  an existing sibling Markdown file and asset directory, providing a useful
  historical quality baseline. `Cthulhu_7_Grundregelwerk.pdf` is AES-encrypted
  but has no user password and permits copy/print, so local extraction works;
  remote API acceptance still needs a preflight. At current published rates a
  complete Mistral OCR 4.0 pass is $12.24 standard or $15.30 using the
  conservative annotated-page rate; two annotated passes plus 10% retry margin
  are $33.66. The low full-corpus price favors evaluating every page rather than
  sampling. Exact inventory and budget scenarios are recorded in
  `docs/rulebook_corpus_cost.md`.

- **2026-08-26:** The MDAF redesign requires four distinct identities: canonical
  BLAKE3 source bytes, a BLAKE3 pipeline recipe, a coordinator job for the
  `(source, recipe)` desire, and the logical BLAKE3 identity of the validated
  MDAF. The existing untagged SHA-256 `file_hash` spans database primary keys,
  jobs, API routes, local caches, raw/output object keys, and hydration, so it
  must be migrated additively through permanent SHA-256 aliases rather than
  renamed in place. Git LFS pointers expose only SHA-256; an unseen pointer must
  be materialized once to calculate BLAKE3, while a verified alias can avoid
  that download. New workers should be recipe-specific adapter pools that emit
  final UTF-8-byte-aligned source maps, retain sanitized native evidence, build
  and locally validate MDAF, and request a publication URL only after the MDAF
  identity is known. Legacy ZIPs can be repackaged honestly only when raw source
  bytes are available for the canonical source digest; they omit mapping and
  outline capabilities and record missing provenance explicitly. Current
  converter pricing/research and the phased target design are in
  `docs/converter_evaluation.md` and `docs/mdaf_redesign.md`.

- **2026-08-21:** The post-implementation provenance audit found two rollout
  follow-ups. Production images build without `.git`, so the container workflow
  now injects `${{ github.sha }}` as the runtime
  `BLOBFORGE_BUILD_REVISION`; the Python release version is aligned with the
  coordinator at 0.4.0. Custom images must pass the same build argument rather
  than accepting the `unknown` fallback. Recipe schema 1 now records the
  converter generation, dated Surya checkpoint identifiers, and effective
  output-affecting Marker/Surya settings (render format, flattening, DPI,
  detection thresholds, recognition padding, layout slicing, and limits), while
  excluding performance-only batch/cache/worker settings. Marker 2 deployment
  still requires a pinned, verifiable model revision or manifest checksum; a
  mutable model alias is insufficient. Artifact operations are available via
  API, Python client, and CLI (`artifacts`, recipe-specific download/preview,
  and `request-conversion`); management-console controls remain optional UX.

- **2026-08-21:** Conversion output identity is now `(document_hash,
  recipe_digest)` rather than implicitly just the source hash. Recipe schema 1
  canonically hashes the Marker compatibility generation, BlobForge Markdown
  output schema, configured Surya model/checkpoint identifiers, and explicit
  output-affecting options. Recipe JSON permits only safe integers (fractional
  settings must be strings) so Python and JavaScript hash identically; exact
  package versions, BlobForge revision,
  Python/platform, and inference details are stored separately as provenance in
  worker registration, archive `info.json`, and Bunny's `conversion_artifacts`
  rows. Workers must advertise a canonical recipe digest to claim work; the
  lease binds the job to it, retries retain it, and completion is fenced against
  mismatched recipe/provenance. New objects use
  `store/out/<document_hash>/<recipe_digest>.zip`. Existing hash-only ZIPs use
  the reserved all-zero legacy digest and are lazily persisted before a job is
  retargeted, so they remain selectable without invented provenance. Artifact
  list/download and explicit conversion-selection APIs enable Marker 1/2 A/B
  evaluation; backup format version 2 includes the new artifact table.

- **2026-08-21:** A native `.venv` can drift beyond the tested conversion lock
  when installed with `uv pip install -e ".[convert]"`: the permissive
  `marker-pdf>=0.2.0` requirement resolved Marker 2.0.0 / Surya 0.22.1 even
  though `uv.lock` pins Marker 1.10.2 / Surya 0.17.1. Surya 0.22's OCR is a
  vision-language model served through an OpenAI-compatible inference backend;
  backend auto-detection selects `llamacpp` when no NVIDIA GPU is visible, and
  that backend requires the external `llama-server` executable plus Surya's
  GGUF model. `uv run blobforge ...` without the `convert` extra can refresh
  BlobForge while leaving those unmanaged optional packages in place. Restore
  the vetted stack with `uv sync --extra convert` and run workers with the
  extra enabled. Public conversion extras now constrain Marker to
  `>=1.10.2,<2`. Startup preflight also recognizes the newer Surya inference
  contract in already-drifted environments: an external URL bypasses local
  tooling, `llamacpp` requires `llama-server`/`LLAMA_CPP_BINARY`, and `vllm`
  requires Docker. These checks happen before coordinator contact. Marker 2's
  VLM pipeline can materially change scanned/complex-document text, block
  boundaries, reading order, tables, equations, and whitespace, so adopting it
  requires representative corpus A/B review and explicit backend provisioning.

- **2026-08-21:** Real workers validate the optional Marker runtime before any
  coordinator identity request, registration, heartbeat startup, or lease
  acquisition; native repository checkouts should run `uv sync --extra
  convert`. Isolated children use exit code 78 for a conversion-host
  configuration failure, and the parent releases the active lease without
  incrementing the document's retry count before stopping. Prompt heartbeat
  publication revalidates both hash and lease token after its coalescing delay,
  preventing the stale post-release heartbeat that previously produced a 409.

- **2026-08-21:** The four regressions found by the inclusive `7ff1c5f3...`
  review are fixed with dedicated regression coverage. Ingestion honors
  `--dry-run` before PUT/enqueue and recreates coordinator metadata after an
  orphaned raw upload; LFS uploads refresh their signed URL after potentially
  lengthy materialization. Hydration bulk-persists newly computed file hashes.
  Its done rows and version-3 watermarks are keyed by normalized coordinator
  URL; migration discards ambiguous legacy done data but preserves file hashes.

- **2026-08-18:** Hydration keeps a persistent local SQLite index
  (`~/.cache/blobforge/hash_index.sqlite3`, overridable via `BLOBFORGE_CACHE_DIR`
  or `BLOBFORGE_HASH_INDEX_PATH`) with file hashes keyed by `(path, size,
  mtime_ns)` — so unchanged files are reused without re-reading on any
  filesystem, unlike the xattr cache which silently misses on mounts without
  `user_xattr` — plus per-coordinator done-set mirrors (`done_hashes` table) and
  `(since_ms, cursor)` watermarks in a `meta` table. Hydration reconciles the
  done-set incrementally with the coordinator's `GET /api/v1/jobs/done-since`
  endpoint: each run pulls only hashes completed after the last watermark
  (keyset-paginated over `(completed_at, done_seq)`, default page 5,000, max
  20,000), merges them into the mirror, and answers membership locally. The
  watermark is versioned (`{version: 3, scope, since, cursor}`); unscoped or
  pre-`done_seq` data forces a safe full resync for that coordinator.
  There is no status TTL — content-addressed outputs are immutable, so
  known-done hashes never expire; `--refresh-status` resets the mirror and
  watermark and re-syncs from scratch; a signed download rejected definitively
  (coordinator 404/409) drops the hash from the mirror, while transient
  failures keep it so the next run retries. The done-sync client refuses to
  loop if keyset pagination ever fails to advance. The S3 done-hash index and
  per-hash existence checks remain only as fallbacks when no coordinator is
  configured. A full range-based reconciliation protocol was considered and
  rejected: the candidate payload is only ~2 MB for tens of thousands of
  hashes, so the client-side delta snapshot is the fitting optimization.

- **2026-08-18:** Client-side ingestion and hydration now use revocable
  per-operator admin tokens (`bfa_...`) instead of direct S3 access. Admins
  create them in the management console (`POST /api/v1/admin/tokens`); each is
  bound to one ID, shown once, stored only as a SHA-256 hash, revocable, and
  valid for job enqueue/read, bulk status, and signed raw-upload/output-download
  URLs. `POST /api/v1/jobs/status` answers completion for up to 5,000 hashes in
  one request (the client chunks larger sets automatically), so `blobforge
  hydrate` resolves availability in bulk calls at any scale and prints progress
  for hashing and status resolution. `POST /api/v1/jobs/{hash}/raw-upload-url`
  and `POST /api/v1/jobs/{hash}/download-url` issue signed PUT/GET URLs, and
  `blobforge ingest`/`hydrate`/`download`/`preview` stream through them with no
  `BLOBFORGE_S3_*` credentials. The S3 done-hash index and per-hash existence
  checks remain only as fallbacks when no coordinator is configured.

- **2026-08-18:** Coordination hardening review fixes are in. `GET
  /api/v1/jobs/done-since` pages over `(completed_at, done_seq)` with a
  `file_hash` prefix filter on `completed_at` only for backwards compat;
  `fail()`/`release()` fence on the lease token (`lease_token`/`worker_id`
  match, 409 on mismatch), while expired leases are recovered only by
  `recoverExpiredLeases()` (a `count` is now returned); `snapshot()` is pure and
  never mutates lease state. `POST /api/v1/jobs/{hash}/fail` and `/release`
  return 409 for fencing violations. `ensureSchema()` tolerates ALTER races on
  replica SQLite. The CLI management commands that need admin
  mutation endpoints (`reprioritize`, `retry`, `janitor`, `retry-all`,
  `clear-dead`, `cancel`) are thin stubs (`--management-ui` required) because
  admin mutations use IndieAuth session auth that per-worker `bfa_` tokens
  cannot perform. `app.ts` `fetch()` uses `return await` on every handler call
  so rejected promises stay inside the try/catch and become 4xx ClientErrors
  instead of unhandled rejections. `blobforge/utils.py::rewrite_asset_paths`
  centralizes the markdown asset-link rewrite (previously copy-pasted in
  worker.py, conversion_child.py, cli.py) and only rewrites markdown link
  targets that name a known extracted image.
- **2026-08-18:** The management console JavaScript bundle is served at the
  versioned `/static/app-v9.js` path and the stylesheet at `/static/app-v8.css`;
  any change to `management_ui.ts`/`ui.ts` must bump those paths (and the ETag
  names) together with the inline `<script>`/`<link>` references in `ui.ts` and
  the coordinator tests, because the old paths are cached immutable for one
  year. Version constants (`APP_JS_VERSION`, `APP_CSS_VERSION`,
  `LOGIN_JS_VERSION`, `MARKDOWN_JS_VERSION`, `BRAND_SVG_VERSION`) are
  centralized in `bunny/src/ui.ts` and drive the routes/ETags in `app.ts`
  (`DOCS_VERSION` drives the docs route too). Sign-out now POSTs
  `/auth/logout` (clearing the cookie/session) instead of a GET. The viewer CSS
  no longer conflicts with the console layout. The `%PDF-` sniff scans the
  first 1024 bytes. A pre-existing 0.3.0 database failed to upgrade to 0.4.0
  with `no such column: done_seq` on every API request: the SCHEMA batch's
  `CREATE INDEX jobs_done_since_idx ON jobs(status,done_seq)` ran before the
  `ALTER TABLE jobs ADD COLUMN done_seq` migration, so `ensureSchema()` threw
  and every route 500'd with "Internal error". The index must be created only
  inside the migration block (after the column exists), never in the static
  SCHEMA batch.

- **2026-07-21:** The Bunny Edge Script root is a public static BlobForge
  handbook; administrator login is `/login` and the private application shell is
  `/console`. Public HTML, robots, IndieAuth metadata, and versioned `/static/`
  assets are routed before database initialization and carry explicit browser,
  CDN, surrogate, and ETag caching. Versioned assets are immutable for one year;
  their path version must change with their bytes. Public documents use a short
  browser TTL and one-day edge TTL. Auth, console, API, and error responses stay
  private/no-store, and unknown non-API routes avoid database initialization.
- **2026-07-21:** Coordinator worker traffic is transition- and lease-driven.
  Run eligibility uses a reusable condition contract; blocked workers publish
  one `suspended` state with reason/optional resume timestamp and send no
  periodic traffic until a one-shot resume. Runtime heartbeat configuration is
  piggybacked on register, claim, and heartbeat responses, so interval changes
  apply after the next request. Disabling normal heartbeats suppresses idle and
  prompt progress updates while active jobs still renew at one-third of the
  lease duration. Claims use a single `{ job, config }` response contract,
  including `job: null` for empty queues; obsolete worker protocols are not
  supported. Ordinary fleet queries exclude revoked credentials, which
  have a separate admin endpoint/view. Linux workers use a no-clone systemd
  installer and persistent cache; `latest` is multi-architecture CPU and
  `latest-cuda` is amd64 CUDA because bundling CUDA made the default image 16.1
  GB uncompressed versus 1.88 GB for the tested CPU image.
- **2026-07-17:** Dependabot remediation raised the supported Python floor to 3.10 because Python 3.9 forced `marker-pdf 0.2.17` and vulnerable legacy resolutions. Patched floors are centralized in `[tool.uv].constraint-dependencies`; Pillow uses an explicit override because `marker-pdf 1.10.2`/`surya-ocr 0.17.1` cap it below the first safe release. A real `assets/lorem.pdf` conversion passed with Pillow 12.3. Transformers must remain on 4.57 because Surya imports removed v4-private APIs; its three alerts concern untrusted model initialization/Trainer paths BlobForge does not invoke. Torch remains at CUDA-compatible 2.10; the remaining unpatched `jit.script` alert is outside BlobForge's conversion path.
- **2026-07-17:** UI-created worker IDs are the deterministic lowercase ASCII slug of their label (for example `GPU Workstation` becomes `gpu-workstation`); duplicate or slug-colliding labels return HTTP 409, including collisions with revoked enrollments. Enrollment tokens remain bound to exactly one worker ID. Any intentionally reusable bootstrap credential must be implemented separately as a dynamic-registration token that exchanges registration requests for distinct worker identities.
- **2026-07-17:** Coordinator failure diagnostics are stored append-only in `job_failures`, one row per failed attempt, instead of only overwriting `jobs.error_message`. Records retain the worker, attempt, timestamp, traceback, context, and last coordinator progress snapshot. Worker stage changes wake the heartbeat publisher immediately (coalesced to at most one update every two seconds), while the normal heartbeat interval remains the liveness fallback. Isolated conversions report reliable coarse checkpoints through an atomic JSON file watched by the parent because the parent's in-process tqdm hook cannot observe child-process state.

- **2026-07-16:** Result previews now use a self-hosted browser bundle of Marked and DOMPurify instead of the handwritten Markdown subset. Marked provides GFM parsing while DOMPurify sanitizes generated/raw HTML before it enters the document; archive image URLs are resolved only after sanitization. Sanitized headings receive stable, duplicate-safe IDs and drive an always-visible sticky ToC with active-section highlighting on desktop; the same ToC becomes a collapsible drawer on narrow screens. `bunny/scripts/generate-markdown.mjs` regenerates the committed browser bundle during `npm run check` and `npm run build`.

- **2026-07-16:** The Bunny management console now treats jobs as a paginated file library instead of relying on the 250-row operational snapshot. Admins can search hashes, filenames, paths, tags, and sources; filter by state/priority (including the complete done set); upload validated PDFs directly to narrowly presigned raw-object URLs before coordinator enqueue; download signed source PDFs or result ZIPs; and preview `content.md` plus archive assets client-side. Direct browser upload/preview keeps large bodies off Edge Scripting but requires object-store CORS for the console origin (`PUT` and `GET`, with `Content-Type` allowed).

- **2026-07-16:** Bunny Database is now the sole coordination and file-metadata authority. The S3 manifest, registry job logs, Telegram bot, legacy migration endpoint, and their CLI surfaces were removed. Admins can create a consistent, versioned JSON database export from the Web UI or `POST /api/v1/admin/backups`; exports are written to `{prefix}backups/coordinator/` with a returned checksum. `blobforge cleanup-legacy` previews, then optionally deletes only the obsolete `{prefix}queue/` and `{prefix}registry/` trees.

- **2026-07-16:** Bunny coordination now supports least-privilege conversion workers. IndieAuth admins create and revoke per-worker credentials in the web UI; plaintext tokens are shown once, SHA-256 hashes are stored in Bunny Database, and each token is bound to one server-generated worker ID. Workers initialize no S3 client and use a claim-time signed raw-object GET plus a lease-bound, just-in-time output PUT URL. The coordinator verifies output existence before completion and detects uploads whose completion call was lost. `blobforge dashboard` and `blobforge workers` can use the coordinator with the same worker token. Trusted ingestion remains separately authenticated by `CLIENT_API_TOKEN` and retains raw-bucket write access; browser upload is deferred.

- **2026-07-16:** Live Bunny IndieAuth diagnostics showed `cookie_present: false` after a successful authorization callback even with a secure host-only cookie and cache bypass. The management UI no longer depends on response cookies: the callback passes its HMAC-signed session in a URL fragment to the public `/console` shell, same-origin JavaScript stores it and immediately erases the fragment, and admin APIs validate it through `Authorization: BlobForge-Session`. Queue data remains server-protected, mutations retain same-origin checks, and `/auth/status` now distinguishes cookie from session-header transport.

- **2026-07-16:** Hardened Bunny admin session transport after a successful IndieAuth callback still returned to the login view. Session cookie naming no longer depends on the Edge Script's internal `request.url` protocol: it is always the host-only `__Host-blobforge_session` with `Secure`, `HttpOnly`, `SameSite=Lax`, and `Path=/`. Auth and HTML responses now emit browser, CDN, and surrogate no-store directives plus `Vary: Cookie`. Added `/auth/status` to report whether the cookie arrived and whether its signed token validates without exposing the token. A live header probe of the prior deployment showed Bunny returning `cache-control: no-cache`, `cdn-cache: MISS`, and omitting the application's original cookie variance, motivating explicit Bunny/CDN cache directives.

- **2026-07-16:** Fixed the Bunny IndieAuth profile form being blocked by CSP. Although the form action itself was same-origin, the `/auth/login` response redirects to an external authorization endpoint and browsers enforce `form-action 'self'` across that redirect chain. The login page now uses a same-origin `/login.js` module to prevent the form submission and initiate a top-level navigation to `/auth/login`; the subsequent IndieAuth redirect is allowed while the strict `form-action 'self'` policy remains unchanged.

- **2026-07-16:** Fixed Bunny IndieAuth returning to the login page by removing the authentication flow's immediate cross-request Bunny Database reads, which can be vulnerable to replica visibility timing when the callback and redirect land on different edge instances. PKCE state and admin sessions are now self-contained HMAC-signed tokens using a dedicated `SESSION_SIGNING_SECRET`; worker credentials cannot forge admin sessions. The login page accepts a profile URL, prefixes bare domains with `https://`, rejects non-HTTPS URLs, and checks the normalized identity against comma-separated `ADMIN_MES` before IndieAuth discovery. The callback must return the exact requested, currently allowed identity. Multiple administrators are supported without database state.

- **2026-07-16:** Replaced the Cloudflare coordination implementation with a Bunny-native design after the platform decision changed. A standalone Bunny Edge Script now provides the stable HTTP API, IndieAuth + PKCE management UI, sessions, and authorization; Bunny Database (managed libSQL/SQLite, currently public preview) stores files, queue state, fenced leases, retries, workers, config, logs, and audit records. Atomic `UPDATE ... RETURNING` claims prevent double assignment across globally distributed stateless script instances. Because Bunny Edge Scripting has no background alarm equivalent, expired leases are recovered lazily and atomically before claims/snapshots or through the UI, which is sufficient for polling external workers and preserves scale-to-zero operation. Python coordinator environment variables and API paths remain unchanged, so clients need only point `BLOBFORGE_COORDINATOR_URL` at the Bunny script. The old `cloudflare/` project and deployment documentation were removed and replaced by `bunny/` plus `docs/bunny_coordination_backend.md`.

- **2026-07-15:** Implemented an optional Cloudflare coordination plane for BlobForge: one SQLite-backed Durable Object owns persistent file/job state, priority claims, fenced expiring leases, retries/dead-letter state, worker registry, configuration, alarms, sessions, and audit records; Bunny/S3 remains the raw/output blob store. The Worker also serves an IndieAuth + PKCE management UI restricted to `https://eric.wendland.dev/`. Python ingestion, workers, status, config, and CLI reads select this backend when `BLOBFORGE_COORDINATOR_URL` and `BLOBFORGE_COORDINATOR_TOKEN` are set, while retaining legacy S3 fallback. A separately authenticated, repeatable `coordinator-migrate` command preserves the existing S3 backlog and terminal states; old processing locks are safely requeued. Workers detect output-upload/completion-call ambiguity and finalize an existing content-addressed ZIP without reconversion.

- **2026-07-09:** Hardened worker scheduled aborts after a marker native crash (`corrupted double-linked list`) killed the whole worker and left job `f829c114cc29...` in `PROCESSING`. `--abort-outside-window` now automatically enables isolated marker conversion in a child process, and the new `--isolate-conversion` worker flag can be used independently to contain native marker/PyTorch crashes. The parent worker owns the S3 lock/heartbeat, enforces child-process timeouts, requeues on schedule boundary, and records ordinary child failures without dying.
- **2026-07-08:** Added local worker run windows. `blobforge worker --run-window HH:MM-HH:MM` gates job acquisition by the worker machine's local time; the flag may be repeated or comma-separated and supports midnight-crossing windows. By default, active jobs finish after a window closes. `--abort-outside-window` interrupts active conversion at the schedule boundary, requeues the job with `recovered_from: schedule_window_closed`, and releases the processing lock. Documentation added in `docs/worker_schedule.md`, README, and DESIGN.
- **2026-06-25:** Requeued all current retry-candidate problem jobs. `blobforge janitor --verbose` restored stale processing lock `0237641f74fd...` at retry `3/3` and retried failed timeout job `792ac29bd6b6...` at retry `2/3`. `blobforge retry-all --dead --reset-retries --priority 3_normal` requeued the four dead-letter jobs (`0857d1183713...`, `3c7ccc748fb4...`, `a96530cb7011...`, `f829c114cc29...`). Verification showed failed `0`, dead-letter `0`, processing `0`, `3_normal` `9`, and `4_low` `431`.
- **2026-06-25:** Follow-up failed-job investigation after reruns found 1 failed job, 4 dead-letter jobs, and 1 stale processing lock. `atlantis` and `citadel` were confirmed retired and their worker registry entries should be ignored as stale. Failed job `792ac29bd6b6...` (`Changeling The Lost - Core Book.pdf`) exceeded the 86400s conversion timeout at retry 1. Stale processing lock `0237641f74fd...` (`Cthulhu_7_Grundregelwerk.pdf`) had a ~4h50m-old heartbeat and would be restored by janitor at retry 3/3. Dead-letter jobs are `0857d1183713...` (`7910 - Rigger 3.pdf`), `3c7ccc748fb4...` (`Trinity Continuum Aberrant (Rasterized).pdf`), `a96530cb7011...` (`Cthulhu-Edition-7-Grundregelwerk-2017.pdf`), and `f829c114cc29...` (`Geist - The Sin-Eaters.pdf`); no structured error logs were available for those markers.
- **2026-06-09:** Removed the two remaining PDFium data-format failure jobs from BlobForge: `3f094b24b162...` (`4th Edition/Shadowrun 4E - Mil Spec Tech.pdf`) and `5be2a0426593...` (`Scion 1st/Scion - Seeds of Tomorrow.pdf`). `blobforge remove` deleted each dead-letter marker, raw PDF object, manifest entry, and error log; verification showed dead-letter count `0` and both hashes absent from the manifest.
- **2026-06-09:** Selectively requeued 38 dead-letter jobs whose errors were not `PDFium: Data format error`. Requeued jobs were moved to `queue/todo/3_normal/` with retry counters reset to `0` and `recovered_from: manual_bulk_retry_dead_excluding_pdfium`; the two PDFium data-format failures (`3f094b24b162...`, `5be2a0426593...`) remain in dead-letter.
- **2026-06-09:** Investigated the current failed/dead-letter backlog. The failed queue was empty, while dead-letter had 40 jobs at retry count 4 (`max_retries: 3`). Error grouping: 34 `Worker restarted while job was processing`, 4 process-pool abrupt terminations, and 2 PDFium data-format errors. A local Marker probe of the smallest restart-failed PDF (`1f71f4699dbe...`, 19.8 MiB) loaded successfully and began layout recognition, indicating at least some dead-letter jobs are retry candidates, but local conversion caused high memory pressure. Avoid bulk retry; retry one job at a time under managed worker control.
- **2026-03-27:** Added `blobforge repair-metadata` to restore missing BlobForge raw-object metadata (`original-name`, `tags`, `size`) from manifest entries after S3 migrations. The repair uses same-key server-side copy with `MetadataDirective=REPLACE`, merges in unrelated existing metadata (for example `src_last_modified_millis`), and reconstructs `original-name` from the basename of the first manifest path to match historical ingest behavior.
- **2026-03-27:** Investigated `blobforge dashboard` showing `unknown.pdf` for live processing jobs. The dashboard prints the worker heartbeat's `progress.original_filename`, and `worker.py` falls back to `s3_meta.get("original-name", "unknown.pdf")`. Live Backblaze raw objects currently retain only `src_last_modified_millis` metadata for sampled/in-flight PDFs, while the manifest still contains correct `paths`, so this symptom indicates missing raw-object filename metadata rather than lost PDF/manifest data. The user confirmed the cause: migrating raw objects to a new S3 provider with `rclone sync` did not preserve the BlobForge metadata.
- **2026-02-03:** Added `S3_PREFIX` support to `config.py` to allow namespacing in the S3 bucket. All queue and storage paths now respect this prefix.
- **2026-02-03:** Standardized `janitor.py` and `status.py` to use the central `config.py` for path resolution.
- **2026-02-03:** Major refactor - consolidated all S3 operations into single `s3_client.py` module. All components now use this unified client.
- **2026-02-03:** Added dead-letter queue (`queue/dead/`) for jobs exceeding MAX_RETRIES. Jobs can be manually retried via CLI.
- **2026-02-03:** Worker now uses heartbeat mechanism (60s interval) with 15-minute stale timeout instead of 2-hour fixed timeout.
- **2026-02-03:** Fixed race condition: todo markers are now kept until job completion (not deleted on lock acquisition).
- **2026-02-03:** Improved sharding from 16 to 256 shards (2-char hex prefix) for better worker distribution.
- **2026-02-03:** Worker ID is now persistent based on machine fingerprint (hostname + /etc/machine-id) instead of random per session.
- **2026-02-03:** Restructured as Python package with `pyproject.toml` entry point. Install via `uv tool install .`
- **2026-02-03:** All env vars now use `BLOBFORGE_` prefix. S3 credentials use `BLOBFORGE_S3_ACCESS_KEY_ID`, etc.
- **2026-02-03:** Operational config (max_retries, timeouts) now stored in S3 at `{prefix}registry/config.json` with 1-hour TTL cache.
- **2026-02-03:** Worker registration: workers push metadata to `{prefix}registry/workers/{id}.json` on startup/shutdown.
- **2026-02-03:** New CLI commands: `blobforge config` (view/update remote config), `blobforge workers` (list registered workers).
- **2026-02-04:** Optimized worker job polling: replaced random shard scanning (5 priorities × 256 shards = 1280 potential requests) with broad priority scans (max 5 LIST requests). Added adaptive exponential backoff with jitter when queue is empty. Added priority cache to skip empty queues for 30s.
- **2026-02-04:** Added `blobforge test-s3` CLI command to test S3 endpoint capabilities (conditional writes, metadata, etc.).
- **2026-02-04:** Implemented timestamp-based soft locking for S3 providers without conditional write support (e.g., Hetzner Object Storage). Set `s3_supports_conditional_writes: false` in remote config to use this mode.
- **2026-02-04:** Enhanced heartbeat metadata: now tracks CPU/RAM usage (via psutil), elapsed time, file size, original filename. Install psutil for full metrics: `pip install psutil` or `pip install blobforge[metrics]`.
- **2026-02-04:** Added job throughput metrics: workers track jobs_completed, jobs_failed, bytes_processed, avg_processing_time, jobs_per_hour. Metrics stored in worker registry and displayed in `blobforge workers` command.
- **2026-02-04:** Improved status dashboard: shows detailed processing job info including filename, elapsed time, stage, CPU/RAM usage. Progress bar for overall completion.
- **2026-02-04:** Added job logging: errors now saved to `{prefix}registry/logs/{hash}/error.json` with full traceback and context. View with `blobforge logs <hash>`.
- **2026-02-04:** New CLI commands: `blobforge logs` (view job logs), `blobforge watch` (auto-refresh dashboard), `blobforge download` (get results), `blobforge preview` (peek at output), `blobforge retry-all` (bulk retry), `blobforge clear-dead` (purge dead-letter), `blobforge search-queue` (find by filename), `blobforge cancel` (cancel running job).
- **2026-02-05:** Added Telegram bot integration (`blobforge telegram`). Features: interactive dashboard, PDF upload for ingestion, queue stats, job status lookup, workers/config views, janitor trigger, retry/cancel/download actions. Uses inline keyboards for navigation. Requires `BLOBFORGE_TELEGRAM_TOKEN` and `BLOBFORGE_TELEGRAM_ALLOWED_USERS` (comma-separated user IDs). Install with `uv pip install -e '.[telegram]'`.
- **2026-02-05:** Fixed S3 metadata Unicode handling. S3 only supports ASCII in metadata values, but filenames may contain Unicode characters (e.g., curly apostrophes like ' U+2019). Added `blobforge/utils.py` with `sanitize_metadata_value()` / `decode_metadata_value()` functions that URL-encode non-ASCII characters. The S3 client now automatically encodes metadata on upload and decodes on retrieval. Also added `original_name` parameter to `ingestor.ingest()` for telegram bot compatibility.
- **2026-02-05:** Implemented file hash caching via extended attributes (xattrs) as specified in `docs/file_hashing_via_xattrs.md`. The ingestor now caches SHA256 hashes in `user.checksum.sha256` and validates using `user.checksum.mtime`. This significantly speeds up re-ingestion of unchanged files. The xattr package is optional - install with `uv pip install -e '.[xattr]'` or `uv pip install -e '.[all]'`. Works on Linux/macOS with ext4, btrfs, xfs, zfs filesystems.
- **2026-02-05:** Added `blobforge remove` CLI command to completely remove jobs from the system. Removes from all queues (todo/failed/dead), raw store, manifest, and logs. Throws error if job is currently processing or already completed (use `--force` to remove completed jobs). Supports `--dry-run` to preview changes.
- **2026-02-08:** Worker startup recovery now treats recovered processing locks as failed attempts. On startup, recovered jobs increment retry count from the processing lock; jobs beyond retry budget move directly to dead-letter queue, and only within-budget jobs are requeued to todo. This prevents crash/restart loops from repeatedly running the same job at `retry=0`.
- **2026-02-08:** Worker runtime now handles catchable shutdown signals (`SIGINT`, `SIGTERM`, and platform-available `SIGHUP`/`SIGQUIT`) and performs graceful shutdown by requeueing the active job and releasing its processing lock before deregistration. This avoids waiting for stale-lock janitor recovery during normal restarts.
- **2026-02-10:** Hardened graceful shutdown ordering in `worker.py`: signal handlers remain active until cleanup finishes, active job requeue happens before heartbeat join wait, and unexpected loop exceptions now still trigger graceful shutdown with requeue intent.
- **2026-02-10:** Startup recovery retry reconciliation now uses `max(lock_retries, todo_retries)` before incrementing to avoid undercounting retries when lock and todo metadata diverge.
- **2026-02-10:** Conversion timeout is now enforced in the worker conversion path via `SIGALRM`/`ITIMER_REAL` when available. Added fallback behavior/logging for platforms/contexts where signal timers are unavailable.
- **2026-02-10:** Updated `README.md` to document graceful worker shutdown semantics (signal handling, immediate active-job requeue, janitor fallback for ungraceful termination) and conversion-timeout behavior/caveats so user-facing docs match current runtime implementation.
- **2026-02-26:** Added `blobforge hydrate` command to walk local PDFs, hash with xattr-aware caching, and materialize completed conversion outputs as `<stem>.md` plus `<stem>.assets/`. Hydration deduplicates zip downloads by hash in a single run, rewrites markdown image links from `assets/` to `<stem>.assets/`, and supports `--dry-run` / `--force`.
- **2026-02-26:** Optimized `blobforge hydrate` remote checks with a two-phase preflight: local hash indexing + single manifest fetch for hash prefiltering, then done-availability checks per unique candidate hash. Added bulk done-hash listing support in `S3Client.list_done_hashes()` for large hydration runs.
- **2026-04-28:** Investigated and fixed `blobforge dashboard` slowness. Root causes were (1) sequential S3 `count_prefix` calls for 5 todo priorities + done/failed/dead queues, (2) `scan_processing_detailed` doing sequential `get_object_json` for every active job (N+1 query problem), and (3) `list_workers` sequentially fetching each worker JSON. Fixed by parallelizing all independent I/O with `concurrent.futures.ThreadPoolExecutor`: `status.py` now fetches all counts and processing details concurrently; `s3_client.py` `scan_processing_detailed` fetches lock contents in parallel (max 16 workers); `list_workers` fetches worker metadata in parallel (max 8 workers). Also added optional `limit` parameter to `count_prefix` for future capping of huge prefixes.
