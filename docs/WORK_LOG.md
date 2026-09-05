# Work Log

## 2026-09-05 (Production wiki-v5 rollout)

- Corrected Gandalf upstream supplied the active service contract. Connected
  through its managed SSH configuration and the operator's deploy certificate;
  host verification remained enabled and credentials remained private.
- Published tested revision `26ba2f5`. CI tests and server/hosted image builds
  passed; Gandalf `88c566b9` pins server `sha256:be88cc30c694...` and hosted
  `sha256:db3bf3a299d0...`. The established quiesced snapshot succeeded, scoped
  Ansible check/apply passed, and the deployed revision and image pins match.
  Identity, ingress, cache volumes, credentials, and quota limits were unchanged.
- Validated all 35 retained Mistral wiki-v3 parent MDAFs before scheduling.
  Previewed the exact cohort, upgraded one canary (328 unchanged assets), then
  queued the remaining 34 as artifact-input derivatives. All completed; all
  original artifacts remain. No provider requests were added by the migration.
- Final SQLite quick_check passed and all three services are active. Public
  health passed and public SCIM still returns 404. Six concurrent uploads account
  for the queue changing from 1368 done/441 todo to 1362 done/447 todo; they were
  not migration changes. The final queue matches the pre-rollout post-upload
  counts. Pending source assignments and incompatible Marker/Datalab parents
  were not retargeted by offline migration.
- Validated all 35 derivatives and parent selections, comparing every retained
  primary Markdown/native OCR member and 3,746 asset members byte-for-byte.
  Hierarchy strategies: numbered TOC 7, TOC/relative geometry 17, explicit TOC
  tiers 2, compact flat TOC 1, retained Markdown 8; 30 reports carry diagnostics.
  Provider reservation aggregates remained identical to the pre-migration
  baseline. Repeated full validation inside the coordinator hit its 128 MiB
  memory-high threshold; stopped only the identified verifier and completed
  in a disposable 512 MiB/one-CPU, network-disabled, read-only container.
  Coordinator memory recovered to approximately 56 MB and health passed.
- Gandalf's mandatory check passed with 767 tests, 1 skip, 4 subtests and 13
  Deno tests. Its admin wiki publication preview was attempted but requires
  OUTLINE_API_TOKEN; committed local runbook changes were not published remotely.
- Blobforge documentation validation: 367 tests and 5 subtests passed; source
  and wheel builds plus git diff --check passed. No additional MDAF SPEC change
  is required for deployment of the already specified recipe contract.

## 2026-09-05 (Approved default promotion; deployment pending)

- Operator approved wiki-v5 as the successor/default and offline replay of old
  runs. Changed the Mistral CLI default and added immutable routing revision 3;
  retained all applicability, rights, spending, and unassigned-queue gates.
- Added default/explicit-release CLI regressions and exercised default routing
  through the coordinator without a canary opt-in. Updated rollout guidance to
  use reviewed, deduplicated artifact-input cohorts and preserve originals.
- Read deployment history, fetched Blobforge/Gandalf refs, and inspected a
  separate Gandalf worktree. Its main lacks the documented Blobforge role.
  Noninteractive SSH to the plain citadel alias failed host-key verification;
  no verification bypass, secret inspection, push, deployment, or production
  queue mutation was performed. Current infrastructure and trusted access are
  needed to continue the production rollout.
- Validation: `UV_CACHE_DIR=/tmp/blobforge-uv-cache uv run --no-sync pytest -q`
  passed 367 tests plus 5 subtests; `uv build --offline` built source and wheel
  packages, and `git diff --check` passed. Promotion changes no MDAF contract,
  so no additional SPEC or Vulcan bundled-skill update is required beyond the
  preceding pipeline commits.

## 2026-09-05 (Expanded TOC-led hierarchy tuning)

- Evaluated all ten supplied filenames/nine distinct native responses without
  provider calls. Read retained contents, title blocks, geometry, Markdown,
  and source maps. Compared the frozen geometry-led baseline to structural TOC,
  numbered-series, explicit-tier, and relative-geometry candidates using uv.
- Added the offline JSONL sweep script and tested ratios 0.55/0.65/0.75.
  Rejected global wide-title rescue and height-only selection after they
  promoted scenarios/subsections. Retained 0.65 and review diagnostics for the
  fallback; exact TOC groups dominate typography. Repaired same-column wrapped
  titles, split authored titles, prefix geometry leakage, and topic depth.
- Added recipe wiki-v5/profile wiki-v4 with unchanged extraction identity,
  frozen earlier recipes, evaluator/worker selection, and offline reprocessing.
  Native response and primary Markdown bytes are unchanged. Offset hypotheses
  are hierarchy evidence only, never invented observed citation labels.
- Added synthetic regressions, including negative and localized cases. Full
  suite: 363 tests plus 5 subtests passed. New Python files pass ruff; uv builds
  source and wheel packages. The separate quota fixture commit removes the
  reproducible millisecond collision encountered during verification.
- Actual Vulcan imports use disposable, separate vaults at both chapter and
  topic depth, checking disjoint/full source spans, root remainders, asset byte
  identity, and generated navigation/citation targets. The expanded results
  are documented in `docs/mdaf_hierarchy_experiments.md` and Vulcan's pipeline
  investigation. No deployment, source archive replacement, or paid OCR ran.
- Reviewed the MDAF SPEC and bundled import skill: no new core fields, commands,
  or provider-aware consumer logic are required. Existing explicit authority,
  depth selection, and immutable-evidence guardrails remain applicable.

## 2026-09-05 (Deterministic quota concurrency fixture)

- Repeated full-suite hierarchy verification exposed the existing concurrency
  fixture's dependence on two calls landing in different milliseconds. The
  helper and test could create identical account/window keys before testing
  concurrency at all. Give the second policy a distinct two-day endpoint;
  overlapping active windows and production quota semantics remain unchanged.
- This is a separate test-only maintenance item from the hierarchy recipe.

## 2026-09-05 (Evidence-backed wiki pipeline repair)

- Added immutable recipe Mistral wiki-v4, profile wiki-v3, and generation-five
  adapter support. Existing recipe files, extraction/cache identity, provider
  requests, and hosted-worker defaults are unchanged. Added explicit evaluator
  and worker selection and offline source-name recovery.
- Added contents/typed-title geometry and numbered-chapter outline recovery,
  fence-aware heading projection for the new profile, observed page labels,
  conservative source-reference spans, and machine-readable hierarchy evidence.
  Reprocessing retains unknown extensions and earlier lineage for the new recipe.
- Updated Vulcan separately to honor original authored anchors under alternative
  outlines, reject unsupported bounded section layouts, preserve root titles,
  surface warnings/root remainders, and resolve prose references only when both
  placement and destination are unambiguous. Updated its MDAF v1 SPEC and docs.
- Replayed retained OCR locally for three distinct private books, without PDFs,
  provider requests, or spending. Imported all three at chapter and topic depth
  into a disposable vault. Verified full disjoint byte-span coverage, all 417
  assets byte-for-byte at each depth, and every generated navigation/citation
  target. Chapter counts are 7/17/9 including root/front matter; root source
  remainders are zero. Detailed results and limits are in the evaluation doc.
- Added synthetic regression tests for hierarchy, fences, citation ambiguity,
  UTF-8 offsets, recipe/cache identity, replay determinism, metadata, evaluator
  selection, and repeated-upgrade provenance. Used apply_patch for edits and uv
  for Python tests/build. Rust workspace tests pass in a private bwrap namespace
  with an empty login profile and usable /dev; the host profile injects Bash
  syntax into /bin/sh otherwise. No host profile changes or deployment occurred.
- Final Python verification: 347 tests and 5 subtests passed; `uv build`
  produced both wheel and source distribution. One preceding suite run exposed
  an unrelated millisecond-window collision in the quota concurrency fixture;
  the unchanged full suite passed on rerun. Build-backend resolution initially
  hit sandbox DNS restrictions and succeeded with approved network access.
  Replayed all three books again after final edits and confirmed identical
  Markdown, source maps, and outlines to the six verified imports.

## 2026-09-05 (MDAF wiki evaluation and source-name propagation)

- Inspected the four supplied private MDAFs using ZIP/JSON analysis and Vulcan
  inspect/import previews. Completed a London import in a disposable vault and
  checked asset bytes and navigation targets. No private corpus was committed.
- Read repository guidance and conversion, lifecycle, worker, coordinator,
  normalization, and test code using `rg`, `sed`, and file reads. The supplied
  checkout was stale at `a1e43f1`; fetched origin and created isolated branch
  `fix/mdaf-wiki-evidence` at upstream `ff6273a` in `/tmp/blobforge-mdaf-wiki`.
- Located the lost-name boundary between coordinator metadata, fixed worker
  staging paths, and shared packaging. Patched worker propagation and portable
  display-name handling using `apply_patch`; provider requests remain unchanged.
- Installed locked dev/server dependencies using `uv sync --frozen --extra dev
  --extra server`. Focused worker/packager tests: 22 passed. The full Python
  suite with coordinator environment variables empty passed 337 tests and 5
  subtests. `git diff --check` passed; `uv build` produced both the source
  distribution and wheel successfully.
- Documented hierarchy limitations and the separate recipe-based follow-up in
  `docs/mdaf_wiki_evaluation.md`; updated TODO and repository findings. No
  provider purchases, production writes, image publication, or deployment ran.

## 2026-09-01 (Hosted Worker Temporary-Storage Exhaustion)

- **Objective:** Diagnose and recover the failed Cortex Prime Mistral job
  without purchasing OCR twice, then prevent equivalent large-PDF failures.
- **Inspection/tools:** Used read-only `rg`/`sed`, Citadel `df`, container `df`,
  sanitized SQLite queries, and a bounded cached-response size calculation.
  Updated the incident plan before changing deployment state.
- **Finding:** Citadel has 611 GiB free, while each hosted worker receives only
  a 128 MiB `/tmp` tmpfs. Cortex Prime is 109,792,283 bytes; its durable Mistral
  response is 15,261,836 bytes and contains 333 images totaling approximately
  9,081,447 decoded bytes. The recipe worker, converter runner, rendered bundle,
  and final MDAF all stage beneath Python's temporary directory, so this valid
  job cannot fit even though host storage is healthy.
- **Purchase safety:** Reservation `qres_3693a7e878394f5f01e722b2` is committed
  for exactly 256 pages / EUR 0.93184 conservative exposure and checkpoint
  `sha256:cd5b2e69...`; the complete response JSON remains in the persistent
  provider cache. The failed job has no new Mistral artifact. Retry only after
  moving temporary work to disk; it must be a cache hit and must not create a
  second paid request.
- **Fix/deployment:** Gandalf commit `a9158793` sets `TMPDIR` to each worker's
  private `/var/lib/blobforge-provider` mount while retaining the 128 MiB tmpfs
  for uv and incidental files. Updated the role design and assertion test; all
  4 focused tests passed. Citadel check mode completed with zero failures. A
  quiesced backup completed successfully at 14:46 CEST before the worker
  restart, preserving the paid response. The 55-task deployment passed
  readiness, OIDC, SCIM, and backchannel canaries. Both containers report the
  disk-backed directory through `tempfile.gettempdir()`, with 611 GiB free;
  `/tmp` is empty.
- **Recovery:** Requeued Cortex Prime without resetting its retry history. The
  replay reservation was `cache_hit=1`, zero requests, zero pages, and zero
  monetary exposure. It completed at retry count 2, published Mistral artifact
  `blake3:77c77b0293459b75a378b848fe572a59ecfcb83197737f4ff3447e8126df69d7`
  (19,278,999 bytes), and retained the original committed 256-page / EUR
  0.93184 purchase exactly once. SQLite `quick_check` and ZIP integrity passed,
  all three services are active, and neither provider volume retained a
  `blobforge-*` temporary directory after completion.
- **Diagnostics:** Two read-only verification queries used obsolete guessed
  column names (`jobs.error` and `artifacts.object_key`) and failed without
  changing state. Schema inspection corrected them to `blocked_reason` and
  `storage_path`; all subsequent checks passed.

## 2026-09-01 (September Mistral Window Verification)

- **Objective:** Verify whether the September allowance window is active and
  distinguish schedule activation from the manual provider-usage gate.
- **Inspection/tools:** Queried Citadel's production SQLite database read-only
  through Ansible. The first direct query lacked permission to traverse the
  protected service directory; the corrected become-enabled query made no
  production changes. A preceding combined diagnostic produced truncated
  client output and was replaced with narrow, sanitized policy/snapshot
  queries.
- **Finding:** The active Mistral window runs from `2026-09-01 00:00:00 UTC`
  through `2026-10-01 00:00:00 UTC` with a EUR 12.75 limit. That is 02:00 CEST
  at both boundaries. The latest provider snapshot still covers the August
  window (`coverage_through=2026-08-31 00:17:17 UTC`, EUR 10.91), so September
  jobs are delayed for `snapshot_missing`, not because the window has not
  started. Record a fresh September snapshot only from confirmed provider
  console usage; do not infer a zero balance.
- **Verification:** Local time was `2026-09-01T13:58:20+02:00`, well after the
  boundary. `git diff --check` passed; only the required Findings, TODO, and
  work-log records from this inspection are uncommitted.

## 2026-09-01 (Exclusive-Consumer Quota Mode — Implementation Start)

- **Objective:** Remove recurring manual snapshot acknowledgements for a
  provider allowance used exclusively by BlobForge without weakening quota
  enforcement for shared accounts.
- **Inspection/tools:** Used `git status`, `rg`, and targeted `sed` reads over
  the provider-account schema/migrations, recurring policy materialization,
  snapshot validation, reservation authorization, admin API/UI, tests, and
  quota design. Updated the task plan before changing application logic.
- **Decision:** Add an explicit, audited account-level exclusive-consumer flag.
  For provider-snapshot accounts only, BlobForge creates one append-only zero
  baseline for each scheduled window at its reset boundary and treats that
  baseline as non-expiring while conservatively counting all later internal
  reservations. Manual provider observations remain append-only reconciliation
  evidence. Shared accounts retain the freshness gate, and disabling the flag
  restores it immediately.
- **Implementation:** Added the backward-compatible SQLite migration, preserved
  exclusivity when older API clients omit the new field, required an existing
  provider-snapshot basis before opt-in, and materialized one
  `automatic-exclusive-reset` snapshot per billed scheduled window. Enabling
  the mode clears matching quota delays; reservation authorization exempts only
  opted-in accounts from snapshot age while continuing to count all purchases
  after the reset or latest manual coverage cutoff. The admin API audit event,
  quota summary, and cache-busted management UI expose the assertion and its
  warning.
- **Tests:** The quota rollover test simulated August activation, a September
  `snapshot_missing` denial, automatic release, a reservation after reset,
  continued conservative exposure after the normal six-hour expiry, and
  immediate restoration of `snapshot_stale` after opt-out. The focused quota
  and server suites pass 35 tests; extracted management JavaScript passes
  `node --check`. The first full suite inherited production coordinator
  environment variables and produced 16 network-only failures after 316 tests
  passed. The clean rerun explicitly removed those variables and passed all
  332 tests plus 5 subtests.
- **Publication/deployment:** Committed BlobForge revision `78f3874`, pushed it,
  and observed the CI test/distribution, server-image, and hosted-image jobs
  pass. The server tag resolves to immutable manifest
  `sha256:80dd45bf1b...`. Gandalf commit `7814a69d` pins that coordinator image;
  its focused role tests passed 4/4 and Citadel check mode completed with zero
  failures while preserving two unrelated generated-wiki edits.
- **Recovery/rollout:** Read-only preflight found all three units active,
  SQLite `quick_check=ok`, zero processing jobs, and the unchanged 1,356 done /
  453 todo queue. The quiesced recovery profile completed successfully at
  14:16 CEST. The digest-pinned coordinator rollout completed 55 tasks with
  zero failures, including readiness, OIDC, SCIM reconciliation, and the
  backchannel SCIM canary. Public health and the cache-busted management-v8 GET
  both return HTTP 200; the unsupported HEAD probe returned the expected 405.
- **Production opt-in:** Before mutation, the new schema existed and the active
  Sep 1–Oct 1 window had zero Mistral reservations. Enabled exclusivity only on
  `mistral:monthly` with an explicit deployment audit event. BlobForge appended
  the zero `automatic-exclusive-reset` baseline covering exactly Sep 1 00:00
  UTC and released all 22 snapshot-delayed jobs. The next guard correctly
  deferred work for an expired USD/EUR rate.
- **FX and paid canary:** The latest available ECB reference (31 August 2026)
  was 1 EUR = 1.1596 USD. Recorded a 24-hour 0.91 EUR/USD quota rate, including
  about a 5.5% conservative buffer and full source/reason audit evidence; it
  released five jobs that had reached the FX gate. The worker completed and
  published one validated 495-page artifact under a committed EUR 1.8018
  conservative reservation (actual billed cash remains unknown), then reserved
  EUR 0.70616 for the next 194-page job. SQLite remained healthy, no jobs were
  quota-delayed, and the worker continued processing within the EUR 12.75
  September ceiling.
- **Cooldown/resumption:** Continued read-only monitoring found three committed
  September conversions totaling 891 pages and EUR 3.24324 conservative
  exposure. Mistral then rate-limited the account; BlobForge released two
  reservations, deferred four jobs under the shared cooldown without charging
  them, and resumed a new processing lease immediately after the 12:25:01 UTC
  boundary. This confirms the rollout did not merely clear the snapshot gate;
  normal provider backpressure still operates independently.

## 2026-09-01 (Coordinator, Marker, and Hydration Production Rollout)

- **Objective:** Publish and deploy the coordinator UI/worker-state changes,
  Marker legacy recipe, and MDAF/TextPack hydration repairs.
- **Publication:** Committed the prior deployment-status record as `2f9bd91`,
  pushed `badfa8e..2f9bd91`, and monitored GitHub Actions run `33486366108`.
  Its tests/distribution, multi-architecture server, and multi-architecture
  hosted-worker jobs passed. Skopeo resolved and verified revision labels for
  coordinator manifest `sha256:d0db743aa0ed0ba2f144228475b5e9f2e376bee6896b6224517fcc61c13eeda3`
  and hosted-worker manifest
  `sha256:b816f6b7e22aefe92488a2fdfef6fe62ba6501aa9debe197f3d8fb542120e1df`.
  The unrelated CPU/CUDA matrix jobs were still building when the deployed
  server/hosted artifacts and canaries had completed.
- **Gandalf:** Preserved two unrelated user-modified generated wiki files.
  Updated only the exact service image pins and restored the owner's requested
  enabled Mistral desired state. The preceding unpushed Gandalf commit already
  rotated the exposed worker credential; coordinator and worker receive that
  same replacement in one rollout. BlobForge role tests passed 4/4 (with only
  a read-only pytest-cache warning), diff checks passed, and Citadel check mode
  completed with zero failures. Committed/pushed canonical Gandalf revision
  `6be00d6c`.
- **Preflight/recovery:** Sanitized inspection found all three units active,
  SQLite `quick_check=ok`, no processing jobs, and 1,356 done / 453 todo. The
  configured application-consistent `restic-profile-blobforge` snapshot then
  completed with `Result=success` and restarted all enabled services.
- **Deployment:** Applied `playbooks/hosts/citadel.yml --limit citadel --tags
  blobforge`. All 55 tasks completed with zero failures; only the three
  secret-bearing Quadlets changed. Coordinator readiness, Authentik OIDC
  reconciliation, private SCIM reconciliation, and the backchannel SCIM canary
  passed without exposing credentials.
- **Production verification:** All three units are active. Podman reports the
  exact pinned manifests and full revision `2f9bd91742ac...`; the coordinator
  is healthy. SQLite remains `ok` and queue counts remain 1,356 done / 453 todo.
  Startup installed enabled Marker composite
  `blake3:8f299a9230dc20143695f0f67517e3ae1edd3f3a8b060f6aa6f418465662976c`
  and assigned exactly 431 eligible raw-only legacy jobs. Both hosted workers
  re-registered idle with rotated/current credentials; the authenticated API
  reports Datalab `idle` and Mistral `quota-exhausted` because its 22 assigned
  jobs await a current-window manual provider snapshot. The Sep 1–Oct 1
  Mistral EUR 12.75 window and UTC/day-1 schedules remain intact.
- **Public and functional canaries:** Public health returns HTTP 200 JSON,
  unauthenticated GET `/` redirects 307 to `/auth/login`, public SCIM returns
  404, and immutable `management-v7.js` contains recipe filtering, compact
  digests, availability state, and supersession logic. A dry-run production
  lookup found the retained rulebook artifact. A full isolated canary then
  downloaded and validated the real 436-page MDAF and directly emitted a valid
  TextPack with 2,195,933 Markdown bytes, 328 assets, 331 members, and exact
  wiki-v3 recipe provenance. Its temporary PDF, index, and TextPack were
  removed.
- **Diagnostics:** One Podman format probe was rejected by Ansible/Jinja before
  remote execution; a sanitized JSON query replaced it. One read-only SQL
  query had shell-quoting syntax failure after already proving `quick_check`
  and queue counts; the corrected query passed. A HEAD probe returned the
  documented 405, so the supported GET redirect canary was used and passed.

## 2026-09-01 (Latest-Fix Deployment Status Inspection)

- **Objective:** Determine whether the latest coordinator/Marker and hydration
  repairs are deployed.
- **Inspection/tools:** Used read-only `git status`, `rev-parse`, `log`, remote
  configuration, `rg`, and targeted `sed` inspection. The first sandboxed
  GitHub CLI request could not reach `api.github.com`; the approved network
  retry used `gh run list` read-only and succeeded. No repository code,
  production service, image pin, queue, or external state was changed.
- **Finding:** Local `main` at `58e23ca` is three commits ahead of
  `origin/main`. GitHub Actions has no build for those commits; its latest
  successful image workflow is run `33379151268` for `badfa8e`. Consequently
  revisions `582575b`, `092e8f9`, and `58e23ca` have not been deployed.
- **Next gate:** Push the commits, require a green image workflow, pin exact
  manifests through Gandalf, then perform the queue-preserving rollout and
  health/database/UI/worker canaries.

## 2026-08-31 (MDAF-Native Hydration and Direct TextPack Output)

- **Objective:** Repair hydration for current MDAFs, make retained-artifact
  selection deterministic, and allow hydration directly to `.textpack`.
- **Inspection/tools:** Used `git status`/`diff`, `rg`, and targeted `sed` reads
  over hydration, TextPack maintenance, coordinator status/download APIs,
  SQLite artifacts, MDAF validation/building, CLI commands, documentation, and
  tests. Used `apply_patch` for all edits and `uv run` with an isolated `/tmp`
  cache for compilation, CLI help, focused tests, and the full suite. The first
  compile attempt exposed the sandbox's read-only default uv cache and was
  repeated with `UV_CACHE_DIR=/tmp/uv-cache`; no dependency was changed.
- **Implementation:** Bulk job status now includes immutable retained-artifact
  summaries even when a job is queued or failed. Hydrate selects an exact
  requested recipe, the job's selected retained recipe, or a sole artifact and
  fails closed on ambiguity. Downloads are cached by source and recipe.
  `mdaf/v1` downloads use a `.mdaf` staging name, pass full validation, and
  read `text.md`; explicit legacy archives retain `content.md` compatibility.
  Download and preview use the same deterministic selection, and preview now
  displays MDAF Markdown. `hydrate --format textpack` atomically emits a
  TextBundle v2 without sibling intermediates and records artifact identity,
  recipe digest, and artifact type in its BlobForge metadata extension.
- **Documentation/tracking:** Updated the hydrate design, README examples,
  TODO state, and durable repository findings. TextPack is documented as a
  lossy convenience projection; MDAF remains the canonical provenance and
  source-mapping artifact.
- **Verification:** Focused hydration/TextPack/coordinator/server coverage
  passed (71 tests). The clean full suite passed with **330 tests and 5
  subtests**. `blobforge hydrate --help` exposes `--format` and
  `--recipe-digest`; Python compilation passed.
- **Status:** Complete. No user artifact or hydrated output was modified.

## 2026-08-31 (Hydrate/MDAF Compatibility Inspection)

- **Objective:** Determine why `blobforge hydrate` is broken and whether its
  materialization model can work with current MDAF artifacts.
- **Inspection/tools:** Used `rg` and targeted `sed` reads over the CLI,
  hydrator, coordinator client/server artifact endpoints, database selection,
  MDAF validator/builder, tests, and hydrate design. Used `find` to select one
  real migrated artifact and `uv run` for a read-only archive/member probe and
  direct hydrator reproduction. No user PDF, artifact, cache, or hydrated output
  was modified.
- **Finding:** The real MDAF contains `text.md`, no `content.md`, and eight
  assets. `_read_markdown_from_archive` reproducibly raises `Conversion archive
  is missing content.md`. Current hydration also mirrors only jobs whose mutable
  state is `done` and downloads without a recipe, which selects the newest
  artifact by creation time. `blobforge preview` likewise only displays
  `content.md`. This is unsafe once one source retains multiple immutable
  recipes or queues another conversion.
- **Decision:** Hydrating Markdown/assets remains compatible with MDAF as a
  projection, but the implementation must become format-aware and recipe-aware.
  The pending repair is tracked in `TODO.md`; no fix was made during this
  diagnostic turn.

## 2026-08-31 (Coordinator State UI and Marker Legacy Routing)

- **Objective:** Add exact-recipe job filtering, readable artifact provenance,
  semantic worker availability, conspicuous policy supersession, and a real
  Marker 1 recipe for remaining raw-only legacy work.
- **Inspection/tools:** Used `rg`, `find`, and targeted `sed`/`nl` reads to map
  admin endpoints, SQLite state, management assets, migration tags, adapters,
  and enrichment. Used `apply_patch` for repository edits; used `uv run` for
  digest/lifecycle checks, compilation and tests; checked emitted JavaScript
  syntax with Node.
- **Implementation:** Added exact recipe/unassigned API and UI filters; compact
  artifact cards; color-coded heartbeat and availability badges; derived
  quota/cooldown/disabled/deferred worker states; and clearly marked
  superseded policy rows with replacement metadata. Added raw Marker 1.10.2
  and lifecycle-v3 Marker-plus-enrichment recipes. The adapter retains raw
  Markdown/metadata and Poppler evidence/report data. Coordinator startup
  installs the recipe and transactionally assigns only unassigned raw-only
  legacy PDFs, auditing a nonzero idempotent migration.
- **Verification:** Composite identity is
  `blake3:8f299a9230dc20143695f0f67517e3ae1edd3f3a8b060f6aa6f418465662976c`;
  lifecycle parsing agrees. Targeted coordinator/worker/quota tests passed.
  The first full run inherited production coordinator variables and reproduced
  the documented 16 legacy-worker DNS failures; the CI-isolated rerun with
  `BLOBFORGE_COORDINATOR_URL` and `BLOBFORGE_COORDINATOR_TOKEN` unset passed
  **327 tests plus 5 subtests**. Compileall, generated-JavaScript syntax
  checking, and diff checking also passed.

## 2026-08-31 (Explicit Mistral Credential Risk Acceptance and Restart)

- **Decision:** The owner clarified that the exposed Mistral inference key and
  BlobForge worker token are acceptable temporarily and explicitly requested
  that the hosted Mistral worker remain running. Credential rotation stays on
  the roadmap but is no longer a restart prerequisite.
- **Action:** Restarted only `blobforge-hosted-mistral.service` using a narrow
  `systemctl start` command that returned no unit environment or credentials.
  No Gandalf commit, secret, recipe assignment, coordinator image, or quota
  policy changed.
- **Verification:** Sanitized read-only production audits found SQLite
  `quick_check=ok`, the Mistral worker active/working, Datalab idle, and all 431
  unassigned jobs unchanged. Mistral completed two more explicitly assigned
  jobs: 421 pages across two requests with EUR 1.684 conservative exposure.
  Accounting therefore uses EUR 10.91 provider baseline plus EUR 1.684 after
  its coverage cutoff, or EUR 12.594 of the EUR 12.75 ceiling. Twenty-three
  Mistral jobs remain todo; claims that cannot fit the remaining EUR 0.156 are
  deferred without retries. The worker remains running under the owner's
  explicit temporary risk acceptance.
- **Tools:** Narrow Ansible `command`/`systemctl`, followed by sanitized
  read-only Ansible/Podman/SQLite inspection. No verbose systemd module or
  status payload was used.

## 2026-08-31 (Provider Estimate Currency and FX Provenance)

- **Objective:** Continue safely while Mistral remains disabled and correct the
  discovered USD-list-price/EUR-account conflation without rerouting any of the
  25 explicitly assigned Mistral jobs to Datalab.
- **Production inspection:** A sanitized read-only Citadel audit found SQLite
  healthy, the coordinator and Datalab worker active, Mistral offline, no
  unsettled attempts, 25 Mistral-assigned todo jobs, 431 unassigned todo jobs,
  and no Datalab-assigned todo jobs. The first inspection used the nonexistent
  `last_heartbeat` column and failed read-only; the corrected query used
  `last_seen`. No production state changed.
- **Implementation:** Added append-only provider FX observations with integer
  ratios, exact source/account currencies, observation/expiry, evidence,
  reason, and actor. Provider probes may qualify their original estimate
  currency; cross-currency reservations require a current matching rate and
  retain the source amount, conservative ceiling-rounded account amount, and
  immutable rate ID. Attempt reports independently qualify list-price currency
  while billed cash and credits remain account-currency values. Existing rows
  migrate as same-currency history without rewriting amounts.
- **Failure behavior and administration:** Missing/expired FX evidence defers a
  job for five minutes without a reservation or retry. Recording a matching
  confirmed rate releases relevant delays. The admin API and quota UI can
  create and inspect immutable rates, display list currencies separately, and
  require operators to include any safety margin; BlobForge performs no
  implicit network lookup.
- **Verification:** Focused provider, adapter, recipe-worker, converter-runner,
  and local-server coverage passes 66 tests; the complete hermetic suite passes
  324 tests plus 5 subtests, and generated management JavaScript passes
  `node --check`. A dedicated regression proves
  missing-rate deferral, release, USD/EUR conversion, exact dual-amount
  retention, FX linkage, and distinct list/billing-currency settlement.
- **Production-shaped migration canary:** GitHub Actions passed its test job and
  published the server and hosted-worker images for `e5c35d0` at
  `sha256:f18b5b6519...` and `sha256:422e9860d7...`. The candidate server image
  migrated a consistent online backup of Citadel's live ledger in an isolated,
  networkless, read-only-root container. All 28 historical reservations gained
  four required columns, no qualifier remained null, no legacy amount changed,
  and `quick_check` remained `ok`; the temporary copy was then removed.
- **Deployment gate:** Production itself was not changed. The local Gandalf
  branch still contains the explicitly deferred credential-rotation commit, so
  pushing a new image pin would implicitly publish that change. Keep Mistral
  stopped and leave the published image deployment-ready until that history is
  resolved deliberately.
- **Tools:** `rg`, `sed`, `apply_patch`, uv/pytest, and sanitized read-only
  Ansible/Podman/SQLite inspection.

## 2026-08-31 (Manual Provider Usage Snapshot Accounting)

- **Implementation:** Added append-only, account-currency provider usage
  snapshots with administrator identity, evidence, observation time, exact
  quota window, and an explicit provider-data coverage cutoff. Activating the
  fallback atomically supersedes the current estimate-based policy, retains
  its billed/non-money limits, removes only the list-estimate ceiling, updates
  the recurring schedule, and releases quota-delayed jobs for recomputation.
- **Fencing:** Billed exposure is the reported baseline plus full reservation
  estimates for every purchase after the coverage cutoff. Snapshots reject
  unsettled covered attempts, future/inconsistent timestamps, decreasing
  coverage, and decreasing within-window usage. Missing or stale snapshots
  block new purchases while cache hits remain eligible. Freshness defaults to
  six hours and is bounded from 15 minutes through seven days.
- **Administration:** Added the confirmed admin endpoint and management-console
  dialog/table. The UI makes the coverage assertion and immutable policy
  transition explicit. Management assets advance to `v5` so immutable browser
  caches cannot retain the previous quota UI.
- **Verification:** Focused quota/server coverage passes 30 tests and
  JavaScript syntax validation passes. The first full run inherited this
  shell's production coordinator variables and caused 16 known legacy-worker
  network failures after 307 tests passed. The hermetic rerun with
  `BLOBFORGE_COORDINATOR_URL` and `BLOBFORGE_COORDINATOR_TOKEN` unset passed
  323 tests plus 5 subtests. Production rollout results will be appended after
  completion.
- **Production:** Commit `6dfc045` passed GitHub Actions and its server image
  was deployed on Citadel at manifest `sha256:65d348adbf...` after a successful
  quiesced recovery snapshot. The initial EUR 0.96 Mistral observation was
  recorded as `quse_04e5e6fc3ee34434a9df`, covering all purchases through the
  last settled reservation. It superseded the active estimate-based policy
  with `qpol_5d74656af3d60dff0a69`, retained the EUR 12.75 billed limit, removed
  the estimate ceiling from the active/future schedule, and released 25 quota
  delays. While the Mistral worker remained stopped, the console advanced to
  EUR 10.91 for the same 17 requests / 3,184 pages. A second immutable snapshot,
  `quse_bf6fb86c5ec0b7b28138`, supersedes the incomplete reading and leaves EUR
  1.84 available. No newer purchase was folded into its unchanged coverage
  cutoff. SQLite `quick_check` is `ok`, public health is green, and there are no
  processing jobs or unsettled Mistral reservations.
- **Credential incident:** The Ansible `systemd_service` stop result included
  systemd's complete `ExecStart` field, which exposed both the Mistral inference
  key and BlobForge worker token in tool output. The Mistral worker remains
  stopped. A replacement worker token is encrypted in local Gandalf commit
  `2a90a4d6` and the desired worker state is disabled, but pushing/deploying that
  emergency change requires explicit user approval. The provider key must be
  revoked and replaced in Mistral's console before re-enabling the worker.

## 2026-08-31 (Mistral Console/Quota Discrepancy Diagnosis)

- **Initial finding:** BlobForge blocked the next Mistral jobs because all 17 committed
  reservations have unknown provider billing. Its conservative fallback counts
  3,184 pages at the configured standard list price of EUR 0.004/page: EUR
  12.736 against the EUR 12.75 allowance. Mistral's admin console initially
  showed EUR 0.96. The inference that this reflected a discounted subscription
  rate was disproved when the console advanced to EUR 10.91 while the worker
  was stopped. This was reporting lag. The later total is also consistent with
  converting the USD 12.736 list exposure into EUR, revealing that the
  configured estimate had incorrectly treated USD and EUR as interchangeable.
- **Provider check:** Read-only requests from the hosted worker to Mistral's
  billing usage and spend-limit endpoints both returned HTTP 401. The deployed
  inference key is intentionally not an Admin API key, which Mistral requires
  for organization billing metrics. Mistral currently documents the dedicated
  Admin API and Backoffice-created keys as a Preview/Enterprise facility; it
  does not document a usage-only key scope. A standard Studio API key cannot be
  elevated into an Admin API key. No provider or production state changed.
- **Production evidence:** A read-only SQLite audit found zero known billed or
  credit amounts, EUR 12.736 estimated/list exposure, EUR 12.736 enforced
  fallback exposure, no unsettled reservation, and quota-delayed jobs. The
  active reset window remains the correct 2026-08-01 through 2026-09-01 UTC
  interval; the discrepancy is the usage basis, not the boundary.
- **Decision:** Keep immutable list-price estimates separate from subscription
  consumption. Do not rewrite settled reservations, pretend unknown per-job
  charges are zero, change the adapter to batch pricing, or inflate the policy
  ceiling. Add append-only provider usage snapshots; quota authorization can
  then use a fresh provider baseline plus conservative estimates for purchases
  made after that snapshot. Automating the baseline requires a separately
  stored Admin API key and an isolated read-only polling component; manual
  console observations must be explicitly audited when the account cannot
  create Admin API keys. Because the provider does not document endpoint-level
  read-only scoping, never expose this credential to conversion adapters.
- **Reconciliation:** Submitted the later EUR 10.91 console reading through the
  confirmed admin endpoint with the same last-settled coverage cutoff. The
  monotonic update created `quse_bf6fb86c5ec0b7b28138`; it did not replace the
  policy or release jobs again. A sanitized production check verified EUR
  10.91 effective usage, EUR 12.75 limit, no unsettled Mistral reservations,
  zero processing jobs, inactive Mistral worker, active coordinator/Datalab,
  and SQLite `quick_check=ok`.
- **Tools:** `rg`, `sed`, Ansible, Podman, read-only SQLite/Python probes, and
  Mistral's official pricing, OCR, subscription, and Admin API documentation.

## 2026-08-31 (Calendar-Month Provider Quota Correction)

- **Finding:** Both recurring production schedules were configured for local
  midnight on day 28, but the provider allowances reset at the beginning of
  each UTC calendar month. Mistral's next observed reset is 2026-09-01 00:00
  UTC, which is 02:00 CEST. The production schedules must therefore use day 1
  in `UTC`, not day 1 in `Europe/Berlin`, which would reset two hours early.
  The production ledger has already committed EUR 12.736 of the EUR 12.75
  Mistral ceiling across 17 requests/3,184 pages, leaving EUR 0.014 rather than
  another EUR 12 today. No Mistral reservation is unsettled. The recurring
  Datalab account has no usage; its earlier USD 0.07 canary belongs to the
  separate frozen canary account.
- **Design:** Schedule boundary changes preserve the old immutable policy and
  append explicit supersession metadata. A replacement may supersede the
  active policy only if its window covers every timestamp counted by the old
  policy through the change and each limit is equally strict or stricter.
  Consequently the Aug-1 replacement counts all Aug-28 usage and cannot reset
  today's allowance, while the Sep-1 window starts cleanly at the provider
  boundary. Unsafe boundary moves fail atomically.
- **Implementation:** Added policy supersession persistence, authorization and
  summary semantics, HTTP 409 conflict handling, calendar-month UI defaults,
  one-time release of stale quota delays, and focused tests for preserved
  usage, exact defer time, and rollback of an unsafe realignment. The complete
  hermetic suite passes 320 tests plus 5 subtests.
- **Production cutover:** Coordinator revision `f7968ef` is deployed at
  manifest
  `sha256:9b277ee84a8a19de92d3320a1d107e8899e2f504b4f1ab78ce1511d23234f390`.
  The audited API realignment changed both recurring accounts to day 1 in
  `UTC`, retained their original Aug-28 policies with supersession links, and
  created active Aug-1-to-Sep-1 replacements. Mistral carried forward exactly
  EUR 12.736 of EUR 12.75 estimated/list exposure, leaving EUR 0.014 and no
  unsettled attempt. Its stale Sep-28 quota delays were released and subsequent
  probes can defer to 2026-09-01 00:00 UTC without consuming retries. Datalab's
  recurring USD 20 window remains unused and has no assigned jobs; the separate
  USD 0.07 canary ledger remains unchanged. No job recipe was silently changed
  merely to consume expiring allowance.

## 2026-08-31 (Mistral Degenerate-Table Packaging Failure)

- **Incident:** The uploaded *Storypath Ultra Core Manual* reached Mistral
  normalization but failed packaging when a provider-typed table contained no
  body row. `markdown_table_to_html` correctly rejected the fragment, while
  the Mistral caller incorrectly promoted that local representation limitation
  into a whole-job failure.
- **Correction:** Mistral normalization now retains the original Markdown for
  provider-typed table blocks that are not valid rectangular pipe grids. Valid
  grids still receive deterministic semantic HTML. This implements the frozen
  recipe's existing `ambiguous_content=retain` contract and changes only an
  execution path that previously emitted no artifact; successful inputs and
  their statistics remain byte-identical, so the recipe digest is retained.
- **Settlement correction:** The adapter wrote its committed provider-attempt
  report before normalization, but `run_converter` raised inside the worker's
  temporary-directory context. Context cleanup deleted the report before the
  outer worker exception handler could read it, causing the reservation to be
  conservatively but incorrectly settled as ambiguous. Converter execution
  errors now validate and carry any provider-attempt report before cleanup;
  the worker also settles the on-disk report inside the temporary-directory
  context for every converter or packaging exception.
- **Production evidence:** The failed 257-page source has durable Mistral cache
  checkpoint `sha256:b949db6f...`, whose envelope names the same
  `qres_1b67e099ab9729032a87fdd1` reservation. The cached response is 6,416,026
  bytes and uses the expected v1 cache contract. Its EUR 1.028 reservation is
  currently ambiguous, and the exact source remains failed under wiki-v3.
- **Verification and recovery gate:** Forty-one focused converter, worker,
  Mistral, and table-normalization tests pass, including subprocess failure
  retention of a committed report. The complete hermetic suite passes 316
  tests plus 5 subtests. Deploy the repaired hosted image while its worker is
  idle, then retry through the checkpoint-resume path and verify the original
  reservation commits without a replacement provider request.
- **Production recovery:** GitHub Actions published hosted-worker manifest
  `sha256:4579d53d5f019132efab36475839f30cb1acae149c6648bcdddef6030f9e7d15`
  from revision `cad2fb3`. A queue-preserving drain let the unpaid preflight
  lease return before deployment; the corrected image and `cryptography 50.0.1`
  were verified before restoring scheduling. Retrying Storypath resumed
  `qres_1b67e099ab9729032a87fdd1`, committed its original one-request/257-page
  EUR 1.028 record, created no second reservation, and published validated MDAF
  `blake3:9b1d9598a5447d5f93e7de7bd820590fec74951d34f160a287c286cfceb1d4d3`.
  Its administrator-selected `2_high` priority and failure history were
  retained. Gandalf commit `5de71540` pins and documents the deployed image.

## 2026-08-31 (Encrypted-PDF Hosted Preflight)

- **Incident:** The newly ingested 436-page
  `Cthulhu_7_Grundregelwerk.pdf` failed during the network-free Mistral quota
  probe. Its PDF encryption uses AES, while the isolated evaluator environment
  installed `pypdf` without its `cryptography` runtime dependency. No provider
  request or quota reservation occurred.
- **Correction:** Added `cryptography>=48.0.1` to the Mistral evaluator's own
  dependency set and refreshed its frozen uv lock. The isolated evaluator
  environment now reads all 436 pages from the exact local production source.
  This is a runtime packaging prerequisite, not an extraction or normalization
  recipe change, so it does not alter recipe identity.
- **Recovery gate:** Publish and deploy the resulting hosted-worker image, then
  retry the failed source through the normal quota probe. Do not bypass the
  existing page and monthly spend ceilings.
- **Production recovery:** The corrected evaluator was deployed and its exact
  environment can read the local 436-page source. The production retry retained
  `4_low` priority and failure history, but a simultaneous signed-input network
  outage occurred before the repaired preflight could execute. That exposed a
  separate worker retry-accounting defect addressed below.

## 2026-08-31 (Hosted Signed-Transfer Network Failure)

- **Incident:** A brief Citadel network outage made the worker unable to reach
  public signed source URLs. Several jobs failed before provider preflight and
  incorrectly consumed conversion retries, including Cthulhu attempt 2. No
  provider reservation or purchase existed for those attempts.
- **Correction:** Worker claim and output capability URLs now retain the
  internal coordinator request origin; browser/admin transfers continue to use
  the configured public origin. `CoordinatorTransferUnavailable` distinguishes
  signed-input network failures from local I/O faults. Recipe workers release
  those leases, mark the outcome deferred, and wait before reclaiming instead
  of recording a conversion failure.
- **Verification:** Focused client, worker, and local-server tests cover network
  classification, retry-free lease release, deferred-loop backoff, public admin
  URLs, and internal worker source/artifact/output URLs.
- **Production deployment:** The complete hermetic suite passes 318 tests plus
  5 subtests. GitHub Actions published coordinator manifest
  `sha256:35e9f25a08ab7546c90b69ed15080dec4be5a296e23a06c52311d2aae05ec618`
  and hosted-worker manifest
  `sha256:18a8cea2a15851501755936931e9831ccaa2807ab71a021e28e6a8a886793bca`
  from revision `26d0728e3cbda0aa88899aa6524ade243f79cd5c`.
  A queue-preserving drain allowed an in-flight paid conversion to commit
  before replacement. Exact pre-maintenance scheduling was restored and the
  scratch table removed afterward.
- **Recovery audit:** No job whose history contains the observed
  network-unreachable transfer error remains failed. Jobs with one such attempt
  stayed queued with their retry history intact. The formerly exhausted
  436-page Cthulhu job completed through Mistral at `4_low`, committed exactly
  one request/436 pages, and retains retry count 2. Storypath remains done at
  `2_high` with its single original committed reservation and validated
  `blake3:9b1d9598a5447d5f93e7de7bd820590fec74951d34f160a287c286cfceb1d4d3`
  artifact. All three services are active, SQLite `quick_check` is `ok`, the
  backup timer is enabled, and the public health endpoint returns HTTP 200.

## 2026-08-31 (Administrative CLI Intake)

- **Objective:** Let administrators upload and prioritize local rulebook
  batches without using the browser.
- **Implementation:** Added `blobforge upload` with recursive directory PDF
  discovery, explicit active recipe resolution by digest/backend/display name,
  an explicit unassigned opt-in, shared priority/tags, dry-run and JSON modes,
  and a ten-minute default socket timeout. Source bytes stream directly to the
  existing authenticated management endpoint; they are never loaded wholly
  into CLI memory. Partial batches report each failure and return nonzero.
- **Safety:** The command requires a revocable admin token and either a recipe
  or `--unassigned`. Hosted workers remain unable to claim unassigned jobs.
  No rulebook was uploaded while implementing the client.
- **Verification:** The focused CLI/client/server suite passes 37 tests, CLI
  help exposes only the self-hosted priority vocabulary, and the complete
  hermetic suite passes 311 tests plus 5 subtests. The first full-suite run
  inherited legacy coordinator environment variables and attempted external
  worker identity calls; rerunning with those variables explicitly absent
  passed. No provider or production mutation occurred.

## 2026-08-31 (Recurring Hosted Quotas and Rulebook Intake)

- **Objective:** Replace one-off canary windows with honest recurring Mistral
  and Datalab budgets, close the remaining failure-injection coverage, and make
  rulebook intake usable without enabling automatic hosted assignment.
- **Implementation:** Added account and policy currency binding, local-time
  monthly schedules with immutable cycle materialization, API/audit endpoints,
  management controls, and currency-aware worker probes and settlement reports.
  Legacy `micro_usd` names remain compatible while representing the account's
  declared currency. The upload UI accepts multiple files with a shared
  priority, tags, and explicit source recipe.
- **Safety:** Added DST-boundary, schedule-idempotency, currency-mismatch,
  rate-limit/cooldown, and post-purchase upload-failure tests. Hosted workers
  remain explicit-assignment only. No rulebook was uploaded or converted.
- **Validation and release:** Python compilation, browser JavaScript syntax
  checking, `git diff --check`, 54 focused tests, and the complete 306-test
  plus 5-subtest suite passed. GitHub Actions published server manifest
  `sha256:c8ca359aef4e4988356d78eb5dd5a97c22f02549dd1bd59dd6704b4772f1c763`
  and hosted-worker manifest
  `sha256:18f1be8035d07f162c684f4db431c98b3d9e47c9265e34832092d23c977f02f8`
  from revision `f9ff3fe`.
- **Production:** Gandalf deployed the coordinator and both concurrency-one
  workers. Mistral has an EUR 12.75 monthly estimate and billed-exposure cap;
  Datalab has a USD 20 cap matching its current advertised free tier. Both
  reset at local midnight on day 28 in `Europe/Berlin`; the active cycle is
  2026-08-28 through 2026-09-28 and started at zero usage. Both exact-recipe
  workers registered idle with `claim_unassigned=false`, leaving the 1,377
  done / 431 todo split unchanged. The public health endpoint passed.
- **Recovery:** Reconciled the backup profile so its resume hook starts both
  newly enabled workers. A fresh quiesced snapshot completed successfully,
  all services resumed idle, and an isolated restore of that snapshot passed
  SQLite integrity verification. Final live assertions again found all three
  services active, both workers idle, the queue unchanged, and zero current
  monthly exposure.

## 2026-08-30 (First Citadel Hosted-Worker Canary)

- Deployed the security-patched coordinator and disabled Mistral/Datalab
  Quadlets through Gandalf after a successful quiesced backup. Production
  retained 1,808 sources/jobs, 1,377 artifacts, and 431 unassigned `todo` jobs;
  SQLite `quick_check` passed and both worker services were stopped.
- Configured concurrency-one provider accounts and immutable canary windows:
  three requests / 30 pages, with USD 0.15 Mistral and USD 0.30 Datalab
  aggregate estimated and billed-exposure ceilings. Seeded the exact already
  paid eight-page Storypath response checkpoints into the backed-up provider
  cache.
- Queued only Mistral wiki-v3 and enabled only its exact-recipe worker. The
  attempt failed before quota reservation or provider access because non-root
  `uv run` tried to create `/app/.cache/uv` on the read-only application tree.
  The retained legacy artifact was not overwritten, Datalab remained stopped,
  and the Mistral worker was stopped immediately.
- Set the hosted image's disposable uv cache to `/tmp/uv-cache`, documented the
  runtime boundary, and added a static container-contract regression. A local
  Podman build then ran both evaluator projects through `uv run` as UID 10001
  with a read-only root and a 128 MiB `/tmp` tmpfs; each created only
  `/tmp/uv-cache` and loaded pinned pypdf 6.14.2. The published digest must
  repeat this contract before the canary is retried.
- The first production stop also revealed that the idle worker, running as
  container PID 1, did not act on SIGTERM and was killed after the 60-second
  timeout. Added explicit supervisor signal handling, interruptible idle waits,
  cancellable isolated adapter process groups, safe lease release, and
  ambiguity-preserving settlement. Unit/integration coverage terminates and
  reaps a real adapter process and delivers SIGTERM to the supervisor. A final
  disposable Podman container exited zero, printed its deregistration marker,
  and needed no SIGKILL under a five-second stop deadline.

## 2026-08-30 (Hosted Worker Production Claim Fence)

- **Objective:** Close the queue-selection and recipe-selector gaps discovered
  in the production-readiness inspection before publishing or starting hosted
  workers.
- **Implementation:** Added a persisted `claim_unassigned` capability with an
  additive SQLite migration. Mistral and Datalab set it false, so their claims
  match only jobs already assigned to the exact recipe. Claim-time metadata is
  constrained to the worker's stored registration, preventing a later payload
  from broadening recipe, media/input kinds, provider account, or assignment
  mode. The no-capability legacy protocol remains compatible. CLI recipe
  arguments now accept canonical tagged BLAKE3 identities as well as historical
  raw hexadecimal identifiers. Quadlet examples use 20-page canary ceilings of
  USD 0.05 for Mistral and USD 0.10 for Datalab.
- **Tools and verification:** Inspected repository and Gandalf guidance with
  `rg` and `sed`, edited with `apply_patch`, ran `git diff --check`, and ran the
  focused CLI/provider/coordinator/worker suite, then the complete hermetic
  suite. All 35 focused tests and 298 full-suite tests plus 5 subtests pass,
  including explicit-only selection, stored-capability anti-broadening, tagged
  digest parsing, and legacy worker compatibility. Container publication
  verification follows in the release step.
- **Release correction:** The first GitHub release run failed closed before
  image publication because its runner lacked `pdftotext`; the newly added
  real enrichment tests exposed that undeclared system dependency. Added
  `poppler-utils` to the CI setup rather than skipping the tests. Ubuntu 24.04
  then supplied Poppler 24.02.0, which the frozen 25.03.0 recipe correctly
  rejected. The test job now runs in Python 3.12 on Debian Trixie and installs
  its matching Poppler line; a local clean-container probe reported exactly
  `pdftotext version 25.03.0`. The runtime check remains fail closed. The
  corrected release run and image digests are tracked in the deployment entry.
- **Container hardening:** The Gandalf translation found that the hosted image
  otherwise defaulted to container root. Set its runtime user to UID 10001;
  deployments must give only that UID access to each provider-specific cache.
  A local Podman rebuild succeeded and image inspection returned exactly
  `10001` for the configured runtime user.
- **Dependency gate:** Before deployment, queried open high-severity GitHub
  dependency alerts by manifest. The two hosted environments directly parse
  untrusted PDFs with pypdf 6.0.0, affected by two malformed-inline-image
  infinite-loop advisories; GitHub identifies 6.14.2 as the first version
  fixing both. Raised both isolated pins to 6.14.2 and regenerated their locks.
  Provider bundles now record that exact pypdf version as an additional tool;
  34 focused adapter/worker/quota tests and the complete 298-test plus
  5-subtest suite pass with the patched environments. A fresh hosted-image
  rebuild succeeded, and an in-container probe returned UID 10001 and pypdf
  6.14.2.
  Other high alerts belong to non-deployed Marker evaluation locks, the retired
  Bunny package, or APIs outside the server execution path; they remain normal
  repository maintenance rather than a hosted-worker deployment exception.

## 2026-08-30 (Hosted API Worker Quota Implementation)

- **Objective:** Implement provider workers that can safely share the Citadel
  coordinator, enforce local quota and promotional-credit budgets, recover
  across crashes without duplicate purchases, and permit explicit bounded job
  overages.
- **Adapter and worker protocol:** Added versioned network-free provider probes
  and purchase-boundary attempt reports to the converter ABI. Mistral and
  Datalab now probe durable cache/checkpoint state, persist reservation IDs,
  report list/billed/credit micro-USD separately, classify rate limits and
  ambiguous outcomes, and support checkpoint-based reservation resume. The
  recipe worker reserves before a cache-miss purchase, settles independently
  of artifact publication, and defers scheduling conditions without a failed
  conversion or retry increment.
- **Coordinator and persistence:** Added provider accounts, immutable quota
  policies, one-use overrides, fenced reservations, delayed jobs, shared
  provider cooldowns, reconciliation, per-job history, and summary accounting
  to SQLite and the FastAPI protocol. Budget checks and reservation creation
  run in one immediate transaction. Strict contracts, worker capability/account
  binding, reservation idempotency, and settlement identity/usage bounds fail
  closed. Unknown billed cash is represented as conservative billed exposure,
  not mislabeled as an actual charge.
- **Operations and UI:** Added the Quotas console for account and policy setup,
  usage/cooldown visibility, bounded overages, revocation, and ambiguous-attempt
  reconciliation. Added separate Mistral and Datalab rootless Quadlet examples,
  key/worker-token templates, persistent response-cache volumes, the combined
  hosted-worker image, README commands, and recovery documentation. Live
  Citadel/Gandalf integration and backup canaries remain intentionally pending.
- **Tools and verification:** Used `rg`, `sed`, `apply_patch`, focused and full
  `uv run pytest`, Node syntax checking, Python bytecode compilation, diff
  validation, and a local Podman hosted-worker container build. Focused provider,
  worker, adapter, and coordinator tests cover exhaustion, cache hits,
  concurrency, bounded override consumption, recovery, reconciliation,
  billing, and HTTP fencing. The focused suite passes 46 tests; the complete
  hermetic suite passes 296 tests and 5 subtests. Management JavaScript passes
  Node syntax validation, changed Python modules compile, and the image builds
  both frozen environments and runs the dual-provider CLI. The build exposed
  and corrected a stale `.dockerignore` rule that had omitted Datalab. The
  first full-suite invocation inherited a retired Bunny coordinator URL from
  the operator shell and made 16 legacy worker tests fail on DNS; the CI-style
  rerun with coordinator variables cleared passed completely.
- **Handoff:** Reviewed the final worktree with `git status`, `git diff
  --check`, and `git diff --stat`, then prepared the implementation, tests,
  architecture notes, roadmap updates, and deployment examples as one cohesive
  repository commit at the user's request.

## 2026-08-30 (Hosted API Workers and Quota Architecture)

- **Objective:** Decide how hosted-provider workers can run alongside the
  coordinator while preventing duplicate purchases, enforcing quota across
  workers, and permitting exceptional jobs to exceed a local budget safely.
- **Inspection/tools:** Used `rg` and `sed` to inspect the SQLite job/lease and
  recipe capability models, worker/client protocol, hosted Mistral/Datalab
  adapters, durable response caches, management API/UI, routing policy,
  deployment documentation, tests, and current roadmap.
- **Decision:** Keep provider workers out of the coordinator process and deploy
  separate provider-account Quadlets, even on the same VPS. Extend the fenced
  worker protocol with a network-free adapter probe, transactional quota
  reservation, immediate post-purchase checkpoint/report, and independent
  settlement. Cache hits do not consume paid allowance; ambiguous outcomes do
  not repurchase automatically.
- **Accounting and override:** Separate provider-side limits, non-overrideable
  applicability/safety rules, and BlobForge budgets. Record requests, pages,
  integer micro-USD estimates, reported list price, billed cash, and credits in
  an append-only reservation ledger. A job overage must be bounded, expiring,
  single-use, exact-recipe, reasoned, and audited; it cannot bypass privacy,
  rights, hard provider limits, or provider cooldowns.
- **Result:** Added `docs/api_workers_and_quotas.md` with persistence, protocol,
  failure-state, UI, deployment, and rollout decisions; decomposed the pending
  implementation and Citadel canaries in `TODO.md`; recorded the protocol in
  `AGENTS.md`. No runtime behavior or production state changed.

## 2026-08-30 (Coordinator-native Artifact Reprocessing)

- **Objective:** Complete the production-side lifecycle path so existing MDAFs
  can be upgraded through normal fenced workers without downloading the source
  PDF or repeating a paid provider request.
- **Coordinator/data model:** Added source/artifact job input modes, exact
  parent artifact and recipe bindings, input-kind-aware capabilities and
  claims, signed immutable parent downloads, and source-conversion reset
  semantics. Artifact claims require a worker registration that advertised
  artifact input. Normal source-only workers cannot consume them.
- **Planning and operations:** Added read-only and atomic-execute bulk planning
  with predecessor lifecycle validation, explicit source-key limits, counts
  for eligible/already-present/processing work, priority selection, retry
  reset, and audit events. Added `blobforge reprocess-plan`, a recipes-page
  bulk dialog, per-artifact upgrade controls, and job input/parent visibility.
- **Worker/publication:** The exact-recipe worker branches on the leased input:
  source runs the adapter while artifact runs the offline MDAF reprocessor.
  The coordinator validates every `mdaf/v1` result, reported logical identity,
  embedded lifecycle recipe, leased target, and exact `derived_from` parent
  before publication. It stores MDAF logical identity separately from the ZIP
  content checksum and leaves all predecessor artifacts intact.
- **Compatibility correction:** The recipe worker had reused legacy S3
  priorities (`1_critical` through `5_background`) even though the local
  coordinator contract is `1_urgent` through `4_low`; corrected the isolated
  worker claim priorities while leaving the legacy worker constants unchanged.
- **Verification/tools:** Used `rg`, `sed`, `apply_patch`, focused and full
  pytest, Node syntax checking through a no-file subprocess, `compileall`, ZIP
  and MDAF validation, temporary SQLite/local storage, and a credential-free
  real Storypath canary. Focused coordinator/worker tests pass 26 cases; the
  full hermetic suite passes 286 tests and 5 subtests. The real canary queued
  parent `blake3:aedfe704...` as artifact input and reproduced derivative
  `blake3:e984145a...` with zero provider calls. Its first attempt passed the
  extensionless internal object path to the public suffix-checking validator;
  the corrected lease-style `.mdaf` staging passed, matching worker behavior.
  The final wheel contains the database/API/UI, worker, reprocessor, lifecycle
  recipe, and current routing policy. As in the preceding build, the first
  sandboxed isolated build could not resolve Hatchling through restricted DNS;
  the approved `uv build` retry succeeded.

## 2026-08-30 (Immutable Recipe Lifecycle and Offline Reprocessing)

- **Objective:** Make evolving post-processing cheap and honest while using a
  major recipe version to mark changes that require expensive extraction.
- **Design and implementation:** Added lifecycle recipe schema v3 validation,
  exact predecessor allowlists, extraction-major/digest gates, embedded
  canonical recipes, and split extraction/post-processing provenance. Added a
  shared network-free Mistral native-response renderer and
  `blobforge reprocess`, which validates an existing MDAF, carries its native
  evidence unchanged, records complete parent lineage, creates a new
  self-contained derivative, validates it, and refuses overwrite. Existing v2
  recipes retain their original combined-provenance path so their frozen
  identities do not change.
- **Versioned policy:** Restored routing policy revision 1 to its committed
  wiki-v2 recipe. Added revision 2 for lifecycle-aware wiki-v3 and made the
  resolver point to the new file. Tests assert that both policy documents and
  identities remain distinct.
- **Real cache canaries:** With both provider keys explicitly unset, replayed
  the already-paid Storypath response. The v2 run from the original
  `source.pdf` reproduced MDAF `blake3:aedfe704...` byte-for-byte. Direct v3
  produced `blake3:1cb473b4...`; offline reprocessing produced lineage-linked
  `blake3:e984145a...`. Both v3 paths preserve the exact 169,550-byte native
  response and the evaluated Markdown. Repeating reprocessing produced
  byte-identical ZIP bytes. No provider call occurred.
- **Verification and tools:** Used `rg`, `sed`, `ls`, `cmp`, canonical JSON/ZIP
  inspection, the shared MDAF validator, `uv run`, pytest, `git diff`, and
  `apply_patch`. The focused lifecycle, reprocessing, runner, routing,
  adapter, worker, and server suite passes 42 tests. The complete hermetic
  suite passes 284 tests and 5 subtests. The built wheel contains the lifecycle
  and reprocessing modules, shared renderer, v3 recipe, and both immutable
  routing-policy revisions. The first sandboxed cache replay failed because the
  adapter's cache lock lives below the read-only home cache; an approved retry
  wrote only that lock and read the retained response. An initial inspection
  used two nonexistent `blobforge.mdaf` exports and was corrected to the
  supported validator plus direct ZIP inspection. The first isolated package
  build could not resolve Hatchling due restricted DNS; the approved `uv
  build` retry succeeded.
- **Commit preparation:** Reviewed the complete worktree scope, whitespace,
  generated-file exclusions, and credential patterns before creating the
  requested repository checkpoint. Local MDAFs, provider caches, temporary
  canaries, and build output remain outside Git.
- **Operational boundary:** Local/archive upgrades are implemented. A future
  coordinator job type must lease a parent artifact and schedule bulk
  derivatives; normal source-conversion jobs must not be described as offline
  upgrades or repeat paid work implicitly.

## 2026-08-30 (List-normalized Recipe, Routing, and MDAF Worker)

- **Checkpoint:** Committed the complete preceding hosted evaluation and wiki
  normalization state as `625bb45` after staged diff and credential-pattern
  checks. Generated corpora, caches, MDAFs, and private review material were
  not committed.
- **List recipe:** Added Mistral wiki-v2 at exact recipe
  `blake3:bdd3e060e88f64277834245a42528a54b6b077774123c3806bdd827cf8ea3026`.
  Unit fixtures cover existing Markdown lists, provider-typed text runs, lone
  ambiguous glyphs, inline mechanics, and headings. A keyless real Storypath
  cache replay produced validated MDAF `blake3:aedfe704...`, removed 20
  redundant decorations, recovered 10 items, and retained `At ♦` / `• TO ••`.
- **Routing:** Added a canonical advisory PDF-rulebook policy and read-only
  `route-plan` command. It records all evaluated feature inputs, revision,
  policy BLAKE3, exact recipe, estimate, status, and rationale; rights, spend,
  applicability, unknown overrides, equations, and canary promotion all fail
  closed. The eight-page canary resolved at $0.032 under a $0.04 ceiling. An
  optional coordinator apply endpoint recomputes the decision, requires an
  active exact-recipe worker, and stores the complete decision plus actor as a
  `job.route` audit event.
- **Workers/build:** Added an exact-recipe subprocess worker with capability
  arrays, media-specific staging, lease renewal, fenced upload/completion,
  failure context, and unknown-claim release. A fake-coordinator test dispatched
  audio then PDF through one process. Added a bounded Mistral canary runtime,
  cache-only mode, hosted-worker container, and GHCR build flavor.
- **Tools/verification so far:** Used `git`, `rg`, `sed`, `find`, `uv`, pytest,
  the shared MDAF validator, ZIP inspection, and `apply_patch`. Focused tests
  pass 28/28. The initial pytest invocation failed only because uv's default
  home cache was read-only; all subsequent runs used `/tmp/blobforge-uv-cache`.
  An attempted nonexistent `blobforge mdaf validate` CLI subcommand was
  replaced by the supported `validate_mdaf` library call.
- **Final verification:** The full hermetic suite passes 275 tests. `uv build`
  produced the sdist/wheel and the wheel contains the policy JSON, exact recipe,
  runtime, and worker. A second keyless Storypath replay is byte-for-byte equal
  to the first wiki-v2 MDAF. Rootless Podman built
  `Containerfile.hosted-worker` as local image
  `6e5d53d3d5bceab9e1b4b7aaa4a46358ba7aa3b6cbfbd87cf24a7d1f670cd7c0`;
  a container smoke test loaded the embedded recipe and reproduced its exact
  digest. Docker verification was unavailable because this account cannot use
  `/var/run/docker.sock`; Podman provided the equivalent build/run check.
- **Commit preparation:** Reviewed the final worktree scope, distribution
  contents, diff whitespace, and credential patterns before creating the
  requested follow-up commit. Local provider caches, generated MDAFs, review
  materials, build output, and the locally tagged Podman image remain outside
  the commit.

## 2026-08-30 (Evaluated-state Commit Checkpoint)

- **Scope:** Prepared the complete Datalab adapter, hosted wiki composites,
  table/cleanup normalization, review UI, real evaluation decisions, tests, and
  documentation as one requested checkpoint before beginning list/routing
  improvements. Ignored corpora, provider caches, credentials, generated MDAFs,
  and review keys/results remain outside Git.
- **Verification:** Reused the immediately preceding hermetic 262-test plus
  5-subtest pass, deterministic provider-cache replays, Vulcan imports, and
  `git diff --check`. A secret-pattern scan of the staged patch found no
  credentials. The first sandboxed `git add` could not create `.git/index.lock`;
  the approved repository-scoped retry succeeded. Removed three Markdown
  trailing-space status suffixes reported by the staged diff check.

## 2026-08-30 (Storypath Wiki-profile Regression)

- **Execution:** Replayed the already-paid eight-page Storypath Mistral and
  Datalab responses with both provider keys explicitly removed. No API request
  occurred. Produced Mistral-wiki
  `blake3:5b1074c707e16069c8ea0172cd90557f57c4eee32c77ff4c886c0d96bca35568`
  and Datalab-wiki
  `blake3:646bb02b391704d6f27af4e52eb0bc8ba01efc3c7c9d78a879bbd2d821ba36ea`.
- **Mistral regression:** Removed 16 provider-typed footer blocks and nothing
  else. The exact line diff accounts for all 166 removed UTF-8 bytes. Heading
  sequences, outline title/level pairs, image links, asset members, lists,
  emphasis, and body prose remain unchanged; 8 mappings and 19 outline nodes
  remain.
- **Datalab regression:** Removed zero descriptions, footer images, or tables.
  Final Markdown is byte-identical to raw Datalab because no defect met the
  conservative evidence thresholds. This preserves honesty but leaves its
  reviewed prose defects intact.
- **Determinism/consumer:** Second keyless replays were byte-identical. Both
  pass Vulcan validation and imported into a temporary vault as 20 notes with
  2/4 assets. The imports preserve the expected hierarchy and assets.
- **Decision:** Did not create another human campaign. It would compare an
  unchanged Datalab candidate and a Mistral candidate differing only by running
  footers already criticized in the blinded review. Mistral-wiki strictly
  supersedes raw Mistral on this canary and is the hosted quality-tier leader;
  Marker 1 remains the local/privacy fallback and retains its list-syntax edge.
- **Tooling:** Used keyless guarded evaluator replays, `cmp`, Vulcan validation
  and import, backend-neutral comparison, exact `difflib` inspection, ZIP/JSON
  invariant auditing, `apply_patch`, `rg`, and `sed`. The one-off image-link
  audit regex emitted a harmless nested-set `FutureWarning`; its equality
  result was still valid and all other audit invariants passed.

## 2026-08-30 (Wiki-normalized Table Review Decision)

- **Ingestion:** Preserved the supplied review-v2 export exactly and ran it
  through strict `review-summarize` validation against campaign
  `blake3:efd4e84ff559de4e497fb51ae406b288f7de91224bb223288bc84fb0af8853ce`.
  Validation accepted 2/8 pages, 26 numeric ratings, 14 N/A values, and 40/160
  slots. The remaining pages were intentionally not assigned copied scores.
- **Unblinding:** A is Mistral OCR 4.1 plus wiki normalizer; B is Datalab
  accurate plus wiki normalizer. Both score 5.0 for text, reading order, source
  mapping, and the one applicable asset rating. Mistral leads tables 5.0 to 3.0
  and wiki utility 5.0 to 4.0; Datalab leads hierarchy 5.0 to 3.0.
- **Decision:** Selected Mistral-wiki as the complex-table recipe. It applies
  semantic HTML consistently across the reviewed table set. Datalab converts
  only parseable rectangular grids; its other inconsistent pipe tables remain
  unchanged by design, so it remains a hierarchy-strong challenger rather than
  the table fallback.
- **Caption evidence:** Traced the reported `阴森` token to Datalab's native
  generated image alt text. The normalizer removed the exact duplicated body
  paragraph but retained the alt text; it did not introduce the token. Provider
  captions remain unverified evidence.
- **Files/tooling:** Added the ignored result and unblinded summary beside the
  campaign, then updated the roadmap, table strategy, normalization design,
  review guide, benchmark results, work log, and repository findings. Used
  `apply_patch`, the strict summarizer, `sed`, `unzip`, and `rg`; no provider or
  production request was made.

## 2026-08-30 (Review Table Contrast Repair)

- **Reported defect:** The semantic-table preview inherited BlobForge's light
  foreground color while table cells used a light browser/background color,
  producing white-on-white content in much of the table.
- **Repair:** Scoped the preview to an explicit light palette: white body cells
  with `#17202a` text, `#e4edf5` headers with `#111820` text, and `#8a98a8`
  borders. This does not affect candidate Markdown, ratings, campaign identity,
  or the rest of the dark review UI.
- **Existing campaign:** Patched the generated, still-current
  `london-falling-tables-wiki-v2/index.html` in place so localStorage ratings
  keyed by campaign
  `blake3:efd4e84ff559de4e497fb51ae406b288f7de91224bb223288bc84fb0af8853ce`
  remain usable. The generator carries the same fix for future campaigns.
- **Verification:** Review tests pass 7/7, the generated JavaScript passes
  `node --check`, and the delivered bundle contains the explicit foreground and
  background declarations.

## 2026-08-30 (Wiki-normalized Hosted Table Recipes)

- **Architecture:** Extended the converter bundle ABI with multiple exact tools
  and explicit Markdown variant/features. The shared builder remains the sole
  MDAF packager. Composite recipes carry their own artifact recipe digest while
  using the frozen raw-provider recipe digest as the paid-response cache key.
- **Normalization:** Added a strict semantic table grid/serializer and
  evidence-specific wiki profiles. Mistral rebuilds pages from typed blocks,
  suppresses typed header/footer blocks and tightly bounded bottom images, and
  converts typed tables. Datalab removes only exact duplicated caption
  paragraphs and sufficiently recurring, small, dimensionally consistent final
  images with shared semantic alt tokens. Both retain ambiguous content and
  package only final referenced assets.
- **Real cache replay:** With `MISTRAL_API_KEY` and `DATALAB_API_KEY` explicitly
  removed, replayed the already-paid eight-page London Falling responses. No
  provider request occurred. Mistral removed 8 headers, 13 footers, and 5 footer
  images and converted 34 tables; Datalab isolated 8 exact descriptions,
  removed 7 footer images, and converted 18 tables.
- **Artifacts:** Produced ignored evaluation MDAFs
  `blake3:1a6b3dad11b78eb1c2912bab9f87b6c23aeb77dde383a33c011bc183f8866534`
  (Mistral) and
  `blake3:bbed7f449c82e4c53f7aa552f1431ab434fe840f5c8b9d5c5159b6effc4b3fab`
  (Datalab). Both retain 8/8 mappings and one relevant asset. A second keyless
  replay was byte-identical for each.
- **Consumer gate:** Independent Vulcan validation accepts both artifacts.
  Actual imports into an initialized temporary vault created 51/48 notes and
  one asset each, preserving `<table>` and `colspan`. Updated comparison metrics
  to count semantic HTML rows without counting tag names as words.
- **Review:** Added a review UI semantic-table preview that reconstructs only a
  fixed element/attribute allowlist through DOM methods, never provider
  `innerHTML`. Generated blinded campaign
  `blake3:efd4e84ff559de4e497fb51ae406b288f7de91224bb223288bc84fb0af8853ce`
  for the two normalized artifacts; the private key remains mode `0600` and
  should stay unopened until review export. The earlier ignored v1 bundle was
  superseded before review because its table preview did not yet allow the
  serializer's safe inline tags; no scores were collected against it.
- **Verification:** Focused normalization, adapter, runner, evaluation, and
  review tests pass. The first full run inherited production coordinator
  variables and produced 16 legacy worker-test network failures after 246 tests
  and 5 subtests passed; the affected 35 tests pass with coordinator variables
  removed. The initial uv run also needed its read-only home cache redirected
  to `/tmp`. A first `blobforge compare` typo was corrected to `compare-mdaf`.
  Node syntax validation passes for the generated review script. A headless
  Firefox screenshot attempt stalled on the embedded local PDF viewer and was
  terminated without an image; browser rendering remains the human review
  gate. Direct `node`/jsdom execution was unavailable because jsdom is not
  installed in this checkout. The final hermetic full suite passes 262 tests
  plus 5 subtests, and `git diff --check` passes.

## 2026-08-30 (Hosted Table Review Decision)

- **Input/coverage:** Transcribed the three rated pages from the supplied
  review-v2 export and preserved its page-specific qualitative defects in the
  benchmark documentation. Strict `review-summarize` validation verified the
  campaign/seed assignment and accepted 3/8 reviewed pages, 80 numeric ratings,
  36 N/A values, and 116/320 slots. Page-one assets were intentionally blank,
  not N/A. The reviewer reported that pages 4-8 repeated the result; no numeric
  values were copied or reconstructed after unblinding.
- **Unblinding/result:** A is Marker 1.10.2, B is Mistral OCR 4.1, C is Datalab
  Convert accurate, and D is Docling 2.122.0. Mistral/Datalab table means are
  5.0/4.0 and wiki utility 4.0/4.0. Marker/Docling both score 1.0 for tables and
  wiki utility. Datalab leads hierarchy at 5.0; Mistral leads the hosted asset
  candidates, while Marker alone extracts only the relevant image on the two
  rated asset pages.
- **Defects:** Recorded Mistral repeated page headers; Datalab image-description
  bleed; irrelevant footer logos from Mistral, Datalab, and Docling; and
  Docling's redundant table screenshots. These corroborate the earlier
  Storypath defects and become structural cleanup fixtures, never global
  replacements.
- **Architecture decision:** Added `docs/table_output_strategy.md`. Pipe-table
  Markdown remains the default for rectangular grids. Tables needing merged
  cells should use a tightly allowlisted, escaped semantic HTML subset with
  `colspan`/`rowspan`, conditional on a Vulcan/import/renderer preservation and
  safety fixture. Table normalization precedes final outline and UTF-8 source
  spans; invalid grids retain evidence/assets but are not reported as
  successful structured extraction.
- **Tooling/state:** Used the strict review summarizer, `jq`, read-only MDAF
  specification and Vulcan consumer inspection, `rg`, `sed`, and `apply_patch`.
  No provider request, artifact, source, campaign, or production service was
  changed.

## 2026-08-30 (Four-way London Falling Table Challenger)

- **Fixture:** Extracted original London Falling pages 12, 23, 31, 38, 64, 78,
  90, and 92 into an eight-page, 1,992,639-byte PDF using Poppler 25.03.0.
  Recorded source/fixture digests, tool versions, and page map in an ignored
  provenance JSON. Rendered every source/fixture page pair at 72 DPI and proved
  all eight PNG pairs byte-identical.
- **Hosted guards/execution:** Both credential-presence plans passed without a
  request. With the user's explicit API-rights confirmation, submitted the
  exact fixture under Mistral's 8-page/$0.04 and Datalab's 8-page/$0.10 guards.
  Mistral completed in 7.5s at a $0.032 list estimate; Datalab completed end to
  end in 25.2s, reported 19.14s provider runtime, and billed $0.06. Responses
  were cached before packaging and credentials were never printed or persisted.
- **Determinism repair:** Mistral replayed byte-identically. Datalab's first
  replay had identical parsed response JSON but a different native member
  digest because live provider key order differed from the sorted cache. Added
  canonical `sort_keys=True` native serialization and a live/replay native-byte
  regression assertion. Preserved the unreviewed live-order artifact under
  `/tmp`, rebuilt from cache with both keys removed, and proved byte identity at
  canonical MDAF
  `blake3:3a4551a34a4ba805287e16ac9a1a4b4794d48bcb720dec05ca28b7046076dafa`.
  No provider request was repeated.
- **Local controls:** Staged the same fixture on Enterprise. The first SSH
  attempts lacked remote `uv` in `PATH`; reran with the explicit user-local
  path. Docling completed in 192.8s. Marker completed in 736.1s and reached
  roughly 10.5 GiB RSS, reinforcing one-large-Marker-job scheduling on 32-GiB
  hosts. Retrieved both MDAFs without changing their bytes.
- **Validation/metrics:** Independent Vulcan validation accepts every artifact
  with 8/8 mappings. Marker/Docling/Mistral/Datalab respectively produced
  133,915/391,233/45,750/96,977 Markdown bytes, 6,096/21,404/5,867/6,554 words,
  303/235/299/299 apparent table rows, and 4/18/6/8 assets. These are review
  inputs, not quality scores.
- **Review:** Generated fresh random-seed review-v2 campaign
  `blake3:9a366ab22d1557b1f665b7c76f08ab90db14b670ad0c4d823ed043a8a6b0d3a1`.
  Its public bundle passed an engine-name scan, contains page-map/rubric notes,
  and its private key is mode `0600` and remains uninspected.
- **Verification/tooling:** Focused Datalab tests pass 5/5; the hermetic full
  suite passes 249 tests plus 5 subtests. Used Poppler, BLAKE3/SHA-256, `cmp`,
  guarded `uv` evaluators, SSH/rsync, Vulcan, JSON semantic comparison,
  `apply_patch`, and the review generator. An initial keyless replay inside the
  filesystem sandbox could not open the existing home-cache lock; the approved
  cache-access retry succeeded with both provider keys removed. No production
  service, source PDF, or prior artifact was modified.

## 2026-08-30 (Hosted Review Result and Table Gate)

- **Input/validation:** Transcribed the complete numeric ratings from the
  supplied Storypath review-v2 export, retaining the reviewer's qualitative
  defects in the benchmark record. An initial normalization command failed on
  an integer/string page-key typo and was corrected without changing any
  artifact or campaign. `blobforge review-summarize` then verified the private
  seed assignment, campaign digest, pages, dimensions, candidates, and all
  values: 8 reviewed pages, 252 ratings, 68 N/A values, and 320/320 slots.
- **Unblinding:** A is Marker 1.10.2, B is Mistral OCR 4.1, C is Datalab Convert
  accurate, and D is Docling 2.122.0. Mistral leads wiki utility (5.0) and
  assets (4.857), with perfect text, formatting, reading-order, and mapping
  scores. Marker and Docling lead list syntax; Docling's formatting is 3.0 and
  Datalab's wiki utility is 3.0.
- **Failure evidence:** Recorded Mistral footer noise and intermittent
  non-Markdown list symbols; Marker's residual `Y` dingbats, rogue marker, and
  inconsistent crops; Docling's formatting loss/lower-resolution assets; and
  Datalab's body-text description bleed, nonstandard bullets, and whole-page
  raster extraction.
- **Large-book table gate:** The reviewer found both blinded London Falling
  candidates unusable for tables and therefore did not manufacture numeric
  distinctions. Validated the campaign key after that verdict: A is Marker and
  B is Docling. Docling's table images retain limited human readability but no
  structured cell semantics; Marker also fails. Added a hosted table-focused
  challenger and modular cleanup stages to the roadmap.
- **Tooling/state:** Used `rg`, `sed`, `uv`, the strict review summarizer, `jq`,
  and `apply_patch`. Read only the two mode-`0600` campaign keys after the
  corresponding blinded judgments. No provider request, source, artifact, or
  production service was changed.

## 2026-08-30 (First Hosted Canaries and Large-Book Review Inputs)

- **Credentials/plans:** Confirmed the two variable names in the git-ignored
  `.env` without printing values. Both guarded plans reported ready. Keys were
  sourced only inside provider commands and never entered arguments, logs,
  recipes, caches, artifacts, or tracked files.
- **Mistral:** Submitted the authorized eight-page Storypath PDF under the
  $0.04 ceiling. OCR completed in 6.9s, reported 8 pages / 9,909,035 bytes, and
  has a $0.032 list-price estimate. The successful response was cached before
  packaging. A keyless replay exposed native JSON insertion-order drift; added
  sorted native serialization, preserved the initial unscored artifact and
  campaign under `/tmp`, and rebuilt without a second request. Repeated keyless
  packages are byte-identical at MDAF identity
  `blake3:cb906843d778f3328175fa869251e39520015d1aec8bddef59b9cab5915112e8`.
- **Datalab:** Submitted the same PDF to Convert accurate with an eight-page
  bound and $0.10 returned-cost ceiling. Provider runtime was 13.12s and exact
  billed cost was $0.06. The live response omitted the documented
  `list_cost_cents`, parse-quality score, and model versions. The cached result
  enabled compatibility fixes for optional list cost and MDAF's
  `mutable-alias` vocabulary without repurchase. The failed partial MDAF was
  moved to `/tmp`; canonical keyless replays are byte-identical at identity
  `blake3:2071347f7728035763d51c2de451dd6fde7c0542fb9e30891f3abc5e4982522f`.
- **Validation/metrics:** BlobForge and Vulcan accept both hosted MDAFs with
  8/8 exact page mappings and 19 outline nodes. Mistral has 26,262 Markdown
  bytes, 4,266 words, and 2 assets; Datalab has 27,484 bytes, 4,465 words, and 4
  assets. Counts remain non-quality evidence.
- **Enterprise completion:** Collected the 98-page Marker artifact after
  3,260.3s. Vulcan accepts its 98 mappings, 487 outline nodes, 1,584 table rows,
  and 89 assets. It has 819,143 Markdown bytes / 65,546 words versus Docling's
  1,723,253 / 115,960, requiring human adjudication rather than a count-based
  conclusion.
- **Review campaigns:** Generated fresh random-seed review-v2 campaigns without
  inspecting their private assignments. Storypath hosted campaign
  `blake3:4f10cea83474b0a728199b05707d5eb3188bb0854bc798759c9aeb2cf5a900cc`
  compares four structured engines on pages 1-8. London Falling campaign
  `blake3:f31eabad8aacc5f4b10ebb96976d5a5491048252a6813df593d74458cab26d67`
  compares Marker/Docling on pages 12, 23, 31, 38, 64, 78, 90, and 92. Public
  files passed an engine-name blinding scan.
- **Backfill acceptance:** The independently running frozen enrichment backfill
  reached 1,377 converted with no other current status. A separate read-only
  `migrate enrich-verify` pass reread every derivative and catalog lineage:
  1,377 checked, 1,377 valid, zero invalid. Phase 1's complete enrichment
  backfill gate is closed.
- **Tooling:** Used guarded plans/execution, private cache locks, `uv`, pytest,
  `vulcan`, `cmp`, ZIP member hashing, SSH/rsync, JSON schema inspection,
  `apply_patch`, and generated review checks. The hermetic suite passes 249
  tests plus 5 subtests; compilation and `git diff --check` pass. No production
  coordinator or source PDF was mutated; no provider request was repeated.

## 2026-08-29 (Hosted Evaluator Readiness and Enterprise Stress Test)

- **Credential safety:** Checked only credential presence, never values. Neither
  `MISTRAL_API_KEY` nor `DATALAB_API_KEY` is exported, present in the repository
  `.env`, standard shell configuration, or the enterprise SSH environment.
  Both guarded plans therefore report `Ready: no`; no provider request was
  made and no credits were consumed. The operator was asked to place the two
  assignments in the git-ignored/build-excluded `.env`.
- **Datalab adapter:** Verified Datalab's current official Convert v1 contract,
  response fields, modes, limits, polling, one-hour retention, bbox add-on
  billing, and post-response cost breakdown. Added a locked isolated evaluator
  and canonical accurate-mode recipe at
  `blake3:c1dc8c06bf29a7a5f1639a4a0bdfc8be1250745d5f6e13438c68b1e38df9bc6f`.
  It bounds pages and file size before submission, requires rights and a
  returned-cost ceiling, stores the same-origin polling URL immediately,
  captures success before packaging, replays offline, validates exact page
  delimiters, rewrites magic-checked raster assets, preserves native billing,
  and reports list/billed/derived discount separately. Accurate replaced the
  initially drafted balanced mode before any API call because quality, not
  throughput, is the first rulebook gate.
- **Review contract:** Introduced review v2 with `inline-formatting` while
  retaining exact v1 validation. A real one-page, three-candidate generated
  smoke campaign contains ten dimensions. Existing result files and campaigns
  were not changed.
- **Enterprise setup:** Read-only inspection found Debian 13, 16 logical CPUs,
  31 GiB RAM/swap, 524 GiB free disk, Podman, Poppler, and the relevant corpus.
  Staged a secret-free checkout, installed uv 0.12.7 in the user account, and
  synchronized the pinned core, Docling, and Marker 1 CPU environments.
- **Enterprise canaries:** Storypath Docling completed in 115.0 seconds and
  Marker 1 in 100.8 seconds. Their converter payloads are byte-identical to the
  earlier outputs; only current outline provenance changes logical MDAF
  identity. The 98-page London Falling Docling run completed in 1,015.9 seconds
  and independently passed Vulcan with 98 mappings, 467 outline nodes, 1,311
  table rows, and 163 assets. It was retained in the ignored local evaluation
  workspace with identity `blake3:64df8d1435e52c090e651caecbe814b631392525aab20bfb2ec56ea0de0cd747`.
  It used roughly 3.0-3.5 GiB RSS. The overlapping
  Marker stress run remained healthy beyond 45 minutes, reached roughly 13.8
  GiB RSS, and was still running at this log point. Use one large Marker job at
  a time on 32-GiB hosts.
- **Backfill:** The frozen enrichment recipe advanced from 1,132 to 1,148
  converted, with 227 pending and 2 processing at the last check. Historical
  rejected-recipe rows were excluded from these figures.
- **Verification/tooling:** Focused adapter/review tests, compilation,
  `git diff --check`, dry-run plans, a generated review-v2 smoke, BlobForge
  validation, independent Vulcan validation, and the hermetic full suite pass;
  the latter reports 249 tests plus 5 subtests. Used `rg`, `find`, `sed`,
  `sqlite3`, `uv`, `pytest`, SSH, rsync, process/memory inspection, official
  Datalab documentation, and `apply_patch`. No source, production coordinator,
  provider account, or existing artifact was mutated.

## 2026-08-29 (Supplemental Blinded Inline-Formatting Evidence)

- **Observation:** The reviewer clarified that they noticed the emphasis
  difference during blinded scoring but forgot to include it in the exported
  notes. The observation is therefore blinded evidence recorded late, not a
  post-unblinding quality judgment.
- **Verification:** Read the two retained MDAFs directly. Marker 1 contains 39
  bold and 17 italic spans used for meaningful list labels, ability names,
  headings, and quotations. Docling contains zero bold and zero italic spans.
  Unblinding maps the observed stronger candidate to Marker and the counts
  support the observation. The exact browser exports remain untouched; a
  separate structured supplement records timing, attribution, and counts, and
  no numeric score is reconstructed after the fact.
- **Decision:** Recorded inline formatting as a distinct review-v2 dimension.
  Existing review-v1 keys/results must remain independently valid rather than
  changing their dimension contract in place.
- **Operational status:** The enrichment catalog advanced to 1,089 converted,
  286 pending, and 2 processing with no current failed row during this check.
- **Tooling:** Used `unzip`, `rg`, `wc`, `sqlite3`, `find`, `pdfinfo`,
  `pdftotext`, `sed`, and `apply_patch`. No artifact, source, provider,
  coordinator, or production state was modified.

## 2026-08-29 (Complete First Rulebook Review and Rating Baselines)

- **Input:** Retained the exact reblinded pages 2-8 export without reviewer
  identity and validated it against private campaign
  `blake3:f8183298733ee442bd2b3f52c7554e3dcbc5110052d349392121dbbf2a22c694`.
  Strict summarization accepted all pages/candidates/dimensions/scores and
  verified the seed-derived label assignment.
- **Unblinding:** A is Docling 2.122.0, B is Marker 1.10.2, and C is Poppler
  25.03.0. All 7 pages have notes; pages 2-4 contain 54 numeric scores plus 27
  N/A values, giving 81/189 completed slots. Pages 5-8 deliberately describe
  differences relative to the stable earlier ratings.
- **Combined result:** Across independently blinded numeric pages 1-4, Marker
  leads text 4.50 and wiki utility 4.75; Docling leads reading order 4.75 and
  hierarchy 4.75. Both score lists 4.00 and page mapping 5.00. Poppler scores
  1.00 for every content/structure/wiki dimension and 5.00 for mapping. Marker
  is the provisional quality leader for this document, while Docling remains a
  faster credible challenger; no production default was selected.
- **Defects:** Recorded the shared `Y` dingbat issue, Marker's repeated H1
  promotion and cross-page list-continuation error, Docling's inaccurate middle
  dots and flattened heading relationship, both engines' loss of a boxed
  callout container, and Marker's better page-eight structure/image resolution.
  Tables and references remain untested.
- **Reviewer improvement:** Added **Copy previous ratings**. It deep-copies only
  the preceding selected page's ratings, never notes; it is disabled without a
  rated predecessor, performs no mutation on page view, and confirms before
  replacing existing ratings. A two-page jsdom exercise copied 4/5 text scores,
  retained an empty destination note, and persisted successfully. The first
  smoke command used an incorrect relative `/tmp` path and was rerun with the
  explicit absolute path.
- **Verification:** Six focused reviewer tests pass. The hermetic full suite
  passes 243 tests plus 5 subtests; bytecode compilation, `git diff --check`,
  strict summarization of the real export, generated-JavaScript execution, and
  a fresh sdist/wheel build also pass.
- **Tooling:** Used `apply_patch`, `jq`, `uv`, pytest, and Node/jsdom. No source
  PDF, MDAF, provider account, API, coordinator, or production state was
  modified.

## 2026-08-29 (Review Evidence and Resumable Scoring)

- **Objective:** Correct the review limitations exposed by the first human
  score before asking for the remaining pages.
- **Review UI:** Embedded the dimension guidance and common 1-5 anchors, added
  explicit N/A, and added partial-result import. An exact-campaign export now
  resumes in a fresh bundle/browser session; a mismatched campaign is refused.
- **Asset evidence:** The builder finds archive assets linked from selected-page
  Markdown, rewrites converter-specific targets to neutral candidate paths, and
  displays a per-candidate gallery. Only declared PNG/JPEG/GIF/WebP members
  whose magic bytes match are copied; unsupported or mismatched members remain
  described but are not browser-loaded. A hostile SVG payload declared as PNG
  is covered by a regression test and neither copied nor named publicly.
- **Result ingestion:** Added `blobforge review-summarize`. It bounds/parses both
  JSON inputs, recomputes the campaign digest and seed hash, derives the label
  assignment from the retained private seed, rejects unknown pages/dimensions/
  candidates/scores and tampered assignments, then reports reviewed-page and
  slot coverage plus per-converter dimension counts, N/A counts, and means.
  Output uses exclusive creation and cannot overwrite prior evidence.
- **Real canary:** Generated
  `.blobforge-migration/evaluations/reviews/storypath-ultra-tasty-bit-03-local-v6/`
  with the unchanged campaign identity
  `blake3:77957f19a06b1ddf8288840aa59f2992482eeeab004314134496c9f90e33a468`.
  The private key is mode `0600`. Public leakage scanning passed. Page 1 exposes
  a neutral 611x470 Docling PNG and 1632x1275 Marker JPEG; both were visually
  inspected. The submitted page-one export imports into the browser with text
  scores 1/4/3 and summarizes as 1/8 pages, 21/216 rating slots.
- **Reblinding:** The page-one mapping had already been disclosed, so continuing
  under it would bias the remaining review. Added `--random-seed`, which creates
  a cryptographic label seed known only to the private key, and generated
  `storypath-ultra-tasty-bit-03-remaining-v1` for pages 2-8. Its campaign is
  `blake3:f8183298733ee442bd2b3f52c7554e3dcbc5110052d349392121dbbf2a22c694`;
  a private comparison confirmed the assignment changed without printing it,
  and the public leakage scan passed.
- **Verification:** Six focused reviewer tests pass, including MIME mismatch,
  N/A accounting, stable blinding, and key-tampering rejection. The hermetic
  full suite passes 243 tests plus 5 subtests. The generated JavaScript executed
  under jsdom with 8 pages, 3 candidates, 27 selectors/N/A choices, 2 page-one
  images, and successful prior-score persistence. Bytecode compilation, CLI
  help, `git diff --check`, public leakage checks, and a fresh wheel inspection
  pass. The sandboxed build could not resolve Hatchling due blocked DNS; the
  approved networked retry built both sdist and wheel successfully.
- **Tooling:** Used `apply_patch`, `rg`, `sed`, `jq`, `find`, `file`, `stat`,
  `unzip`, `uv`, pytest, Node/jsdom, and local image inspection. No source PDF,
  MDAF, provider account, API, coordinator, or production state was modified.

## 2026-08-29 (First Blinded Human Score)

- **Input:** Received and retained the exact page-one score export for campaign
  `blake3:77957f19a06b1ddf8288840aa59f2992482eeeab004314134496c9f90e33a468`.
  The result intentionally contains no reviewer identity.
- **Unblinding:** Verified the campaign digest against the private key. Candidate
  A is Poppler, B is Docling 2.122.0 standard, and C is Marker 1.10.2.
- **Finding:** Docling leads Marker on this page for text and reading order
  (4 versus 3) and ties it at 4 for hierarchy, lists, displayed asset links, and
  wiki utility. Poppler is 1 on content/structure/wiki dimensions and 5 only on
  page mapping. All three page mappings scored 5. Tables and references were
  not rated, and image fidelity was not observable, so no aggregate recipe
  score was calculated.
- **Defect evidence:** Candidate text confirms the shared `Y` list marker.
  `pdffonts` reports an embedded `FantasyRPGDings` TrueType font, supporting the
  reviewer's dingbat-decoding hypothesis without yet proving the exact glyph
  mapping. Recorded a font/layout-aware normalization task and explicitly
  rejected global `Y` replacement.
- **Review-system gap:** Recorded inline rubric anchors, explicit N/A, blinded
  archive-asset inspection, and result import/coverage reporting as the next UI
  improvements. Updated the benchmark report while preserving the one-page
  limitation; no converter was selected or promoted.
- **Tooling:** Used `jq`, `pdffonts`, `rg`, `sed`, and `apply_patch`. No source
  PDF, MDAF, private key, conversion output, provider account, or production
  state was modified.

## 2026-08-28 (Runnable Blinded Rulebook Review)

- **Objective:** Continue from converter infrastructure to an artifact a human
  can test without waiting for hosted credentials or the complete legacy
  backfill.
- **Implementation:** Added `blobforge review-bundle`. It independently
  validates every MDAF, requires one common supplied source digest, extracts
  page text from final UTF-8 source-map spans without duplicating overlapping
  regions, rejects ambiguous multi-page intervals, parses explicit one-based
  page/range selections, and creates a new atomic review directory. Candidate
  order is a deterministic hash shuffle and cannot change with CLI argument
  order.
- **Review UI:** The browser-local bundle shows the original PDF beside raw
  candidate Markdown and provides 1-5 controls for text, reading order,
  hierarchy, lists, tables, assets, references, source mapping, and wiki
  utility, plus per-page notes and blinded JSON export. Markdown enters the DOM
  only through `textContent`; embedded JSON escapes `<`. Local-storage denial
  degrades to export-only operation. Public files contain no engine/path/
  artifact/tool/model identifiers; the separate key is written mode `0600`.
- **Real test:** Generated the complete eight-page Poppler/Marker 1/Docling
  review at
  `.blobforge-migration/evaluations/reviews/storypath-ultra-tasty-bit-03-local-v3/`
  with campaign identity
  `blake3:77957f19a06b1ddf8288840aa59f2992482eeeab004314134496c9f90e33a468`.
  Its private key is the sibling `.key.json`. A jsdom runtime check found 8
  pages, 3 candidate columns, 27 score selectors, and `source.pdf#page=1`.
- **UI debugging:** The first headless Firefox image showed an empty interface.
  Investigation found both an unescaped JavaScript newline and one missing
  closing brace; both were corrected. Firefox's headless PDF plugin then hung
  while taking a second screenshot, so that process was interrupted and the
  generated JavaScript was instead executed directly under the repository's
  jsdom dependency. Earlier local-v1/v2 directories remain ignored diagnostic
  output; local-v3 is the usable bundle.
- **Hosted trial:** Added a no-request `blobforge evaluate mistral --plan` mode
  and mandatory `--confirm-api-rights` for a cache miss. The selected whole-book
  test is exactly 8 pages, $0.032 list price, with page ceiling 8 and cost
  ceiling $0.04. The plan reports the frozen recipe, cache, credential, rights,
  and readiness. It reports not ready solely because `MISTRAL_API_KEY` is
  unset; no provider call or upload occurred.
- **Verification:** Offline review tests cover stable blinding, source binding,
  one-based bounds, Unicode byte spans, hostile Markdown, private-key mode,
  mismatched sources, and overwrite refusal. Mistral tests additionally prove
  that rights rejection precedes provider access. Focused review/Mistral/corpus
  checks pass 15 tests. The final hermetic suite passes 241 tests plus 5
  subtests. All three real input artifacts pass independent Vulcan validation;
  generated public files pass an engine/path leakage scan; executing the
  generated JavaScript produces 8 page choices, 3 candidates, and 27 score
  controls. `git diff --check` passes.
- **Packaging:** The first sandboxed wheel build could not resolve Hatchling
  because network access was restricted; its following archive listing was
  therefore of the prior wheel and was not accepted as evidence. The approved
  rerun built fresh sdist/wheel artifacts and confirmed both `blobforge/review.py`
  and the frozen Mistral recipe are included.
- **Tooling:** Used `apply_patch`, `rg`, `find`, `sed`, `stat`, `sqlite3`,
  `systemctl`, `uv`, pytest, bytecode compilation, the BlobForge and Vulcan
  CLIs, Node/jsdom, headless Firefox, local image inspection, and process
  interruption. No rulebook source, existing MDAF, API cache, credential,
  provider account, or production coordinator state was modified.

## 2026-08-28 (Backfill Launch and Quota-Safe Mistral Evaluation)

- **Backfill:** Launched the frozen all-artifact enrichment as collected user
  service `blobforge-enrichment-backfill.service` with two size-aware worker
  processes. Its one-time inventory cached all 1,365 available source page
  counts and classified 100 large / 1,262 ordinary pending documents. The
  first-pass audit reached 62 recipe artifacts (50 instrumented in this run),
  4 control-glyph failures, 2 interrupted rows, and a 1.58-GiB service memory
  peak. The transient service was then stopped cleanly so replacement process
  workers can load the proven parser repair; completed artifacts were retained.
- **Corpus failure repair:** The running pass exposed Poppler bbox XHTML with
  illegal raw C0 glyphs: one inspected failing line contained `0x18`. Added a
  narrow XML 1.0 pre-parser that removes only forbidden C0 bytes and records a
  nonzero count in native evidence. A real 227-page failed source then
  extracted 1,757 blocks after removing 9 bytes. Valid XHTML is byte-identical,
  so the frozen recipe identity is retained; failed attempts stay append-only
  and will be retried after restarting the process workers on the repaired
  checkout.
- **Restart integrity:** Added a nonblocking workspace `flock` held for the
  whole CLI run and startup recovery that closes abandoned `processing`
  attempts as `interrupted` before scheduling. During the controlled upgrade,
  attempt IDs 55 and 56 were the only rows that predated the replacement
  service; they were relabeled `interrupted` with an explicit restart reason.
  No output, successful attempt, job identity, or failure evidence was deleted.
  Final acceptance checks current `legacy_enrichments` state; historical failed
  and interrupted attempt rows intentionally remain nonzero.
- **Mistral adapter:** Added durable response capture keyed by exact source
  SHA-256, frozen recipe digest, model, and API flags. A per-request `flock`
  spans lookup and the paid call; successful native JSON is fsynced and
  atomically renamed at mode `0600` before validation or packaging. Cache hits
  work without credentials, while malformed entries and incomplete page/usage
  coverage fail closed without silently buying a replacement response.
- **Output correctness:** Fixed confidence extraction to use Mistral's actual
  `average_page_confidence_score`. Added deterministic page-prefixed asset
  names, traversal/collision resistance, data-URL media types, rewritten links
  before UTF-8 mapping offsets, exact page-index coverage, usage-page checks,
  and consistent diagnostic severity. Native blocks, rectangles, dimensions,
  tables, model, confidence, and usage remain intact in the rendition; region
  precision is not claimed without validated block-to-byte correspondence.
- **Recipe and operation:** Packaged Mistral evaluation recipe
  `blake3:982a97ca1d45f5a0ac30dd8c7507efb594688d1b949f406ef4620f3352e723c7`
  and added `--response-cache`, defaulting to the operator cache directory.
  Documented sensitive-cache backup, replay, explicit repurchase, list-price
  versus billed/credited spend, and remaining production gates in
  `docs/mistral_api_adapter.md`.
- **Verification:** Added seven offline adapter tests (nine cases) covering capture/replay,
  secret exclusion, file modes, UTF-8 offsets, assets/media, page confidence,
  preflight ceilings, malformed cache handling, and response completeness.
  The focused converter/MDAF suite passes 16 tests; the final isolated full
  suite passes 236 tests plus 5 subtests; bytecode compilation and CLI help smoke
  checks pass. An initial full-suite invocation inherited the operator's real
  coordinator URL/token and made 16 old worker-unit tests attempt a blocked
  Bunny identity lookup (213 passed); explicitly removing those two variables
  restored the correct hermetic result. Ruff is not installed in the root
  development environment, so that optional command could not run. No Mistral
  API request, credential read, provider upload, or paid conversion occurred.
- **Packaging:** Refreshed the isolated Mistral lock after raising the adapter
  package to 0.2.0. The first offline lock attempt correctly failed because its
  registry metadata was absent; the approved networked lock resolved the same
  21 packages and changed only the local project version. An isolated wheel
  build initially hit the same restricted build-backend lookup, then succeeded
  with approved network access. The final wheel contains both frozen recipe
  JSON files.
- **Tooling:** Used `systemd-run`/`systemctl`/`journalctl`, `sqlite3`, `ps`,
  `find`, `rg`, `sed`, `apply_patch`, `uv`, pytest, bytecode compilation,
  package/SDK schema inspection, canonical BLAKE3 calculation, and an official
  documentation search. The search returned no usable page content, so the
  pinned SDK's generated response models were used as the exact schema source.

## 2026-08-28 (Enrichment Telemetry, Scheduler, and Recipe Freeze)

- **Implementation:** Added append-only `legacy_enrichment_attempts` telemetry
  with status, timestamps, elapsed seconds, process-tree peak RSS method/value,
  source pages, output bytes, and errors. Added immutable cached page counts in
  `legacy_pdf_metadata`. Status aggregates measured documents/pages,
  process-seconds, peak RSS, outputs, and process-hour throughput; verification
  cross-checks output bytes and retained Poppler page evidence.
- **Scheduling:** Replaced shared-thread enrichment concurrency with isolated
  worker processes so memory belongs to one document. PDFs at 300+ pages or
  64+ MiB are large; at most one large input runs concurrently, while an
  ordinary input may use the second slot. Thresholds are explicit
  performance-only CLI controls. The approved 32-GiB-host setting is two jobs.
- **Canary:** Reran the existing 8-page Shadows and Mirrors, 70-page Curseborne
  omnibus, and 256-page/104.7-MiB Cortex book with two processes. They completed
  in 6.2/45.9/138.5 seconds at 51.8/138.0/354.2 MiB peak RSS and produced
  0.4/3.5/34.8 MiB artifacts. The scheduler kept the large book from
  overlapping another large input. All three logical MDAF identities were
  unchanged; the complete 15-artifact verifier remained 15 valid / 0 invalid.
- **Freeze:** Moved the reviewed recipe into packaged canonical JSON at
  `blobforge/recipes/pdf-enrichment-v1.json`. It retains digest
  `blake3:0e7e6c1ba4bb6a8920a58cd08fe3c957bd48b729cbccc5733ffec3d47876a569`
  and fails closed on any Poppler version other than 25.03.0. The built wheel
  contains the recipe. The born-digital publication policy, deterministic
  canary, restart behavior, validation, and operational telemetry gates now
  pass, so `pdf-enrichment/v1` is frozen and the append-only backfill is
  authorized.
- **Verification:** The focused suite passes 13 tests. The full suite passes
  225 tests plus 5 subtests. `git diff --check` passes. An initial isolated
  wheel build could not resolve Hatchling under restricted networking; the
  approved rerun built both wheel and sdist and confirmed the recipe member.
- **Tooling:** Used `apply_patch`, `rg`, `sed`, `sqlite3`, `uv`, `pytest`,
  `pdfinfo`, package inspection, the migration CLI, and read-only enrichment
  verification. No source PDF, base MDAF, production coordinator row, or prior
  recipe artifact identity changed.

## 2026-08-28 (Born-Digital Recipe Scope and API-Credit Policy)

- **Decision:** Declared the current legacy enrichment, Poppler, Marker 1,
  Marker 2 no-OCR, and Docling standard profiles to target born-digital
  illustrated pen-and-paper rulebooks with usable embedded text. Scan-heavy and
  image-only PDF support is not an acceptance gate. If BlobForge later needs
  it, OCR behavior must use a separate recipe identity with explicit models,
  provenance, cost, and evaluation rather than a hidden fallback.
- **Hosted evaluation:** Recorded that the operator has a low-cost Mistral
  subscription with promotional API credits. Mistral and future Datalab runs
  may proceed as resumable, quota-aware batches over successive months. Credits
  affect cash timing but not normalized comparison: retain list-price estimate,
  billed amount, credits applied, provider usage, page count, and run budget as
  separate data. Production adapters must persist successful responses by
  `(source_digest, recipe_digest)` before packaging to prevent duplicate
  billable calls; that checkpoint remains implementation work.
- **Security and limits:** API keys, private balance/expiry data, and account
  details remain worker secrets. Rights checks and hard page/spend ceilings
  remain mandatory even for fully credited runs.
- **Documentation:** Updated the enrichment decision/canary, conversion roadmap,
  adapter architecture, evaluation/cost model, evaluator operator READMEs,
  canonical TODO, and repository findings. This is applicability metadata, not
  an output-affecting algorithm change, so the validated enrichment recipe
  digest and existing canary artifacts remain unchanged.
- **Tooling:** Used `rg`, `sed`, `git`, and `apply_patch`. No API call,
  credential access, artifact conversion, production mutation, or dependency
  change occurred.

## 2026-08-28 (Corrected PDF Enrichment Candidate and Expanded Canary)

- **Objective:** Repair the mapping-accuracy failures that rejected the first
  PDF-enrichment recipe, verify the replacement against the same corpus, and
  expand the canary before any bulk backfill.
- **Implementation:** Replaced one-sided block alignment with nearest-anchor
  bounded, page-monotonic alignment. Poppler evidence now retains stable line
  and word IDs with point geometry. Candidate refinement consumes disjoint word
  ranges; exact whole blocks can retain clipped block geometry; fuzzy prose
  regions are limited to one source block and separately gated by similarity,
  Markdown-token coverage, and normalized-length ratio. Strong page matches
  that cannot defend a rectangle publish the namespaced page-only method. Added
  an independent publication audit for monotonicity, method/selector contracts,
  duplicate rectangles, and report counts. All output-affecting policies are in
  generation-2 recipe identity
  `blake3:0e7e6c1ba4bb6a8920a58cd08fe3c957bd48b729cbccc5733ffec3d47876a569`.
- **Regression coverage:** Added fixtures for following-anchor bounds, disjoint
  split paragraphs, page-only fallback, multi-block precision downgrade,
  repeated evidence, page regression, geometry reuse, and retained word/line
  evidence. Extended the resumability test to recover a `processing` row and
  atomically replace an interrupted partial destination.
- **Initial canary:** Reprocessed the original ten documents twice with two
  jobs. All MDAF identities were stable. Coverage improved to 1,666/2,355
  blocks (70.7%) and 381,074/492,744 semantic bytes (77.3%): 1,078 mappings
  have regions and 588 are page-only. Previously observed wrong-page,
  repeated-label, coarse-column, and split-block failures are fixed or safely
  omitted/downgraded.
- **Expanded canary:** Added five complete difficult books (German Cthulhu,
  *Paths of Storytelling*, *Cortex Prime*, Shadowrun 5 core, and *Curseborne*)
  for 15 artifacts / 1,957 pages total. The combined output maps 20,047/31,997
  blocks (62.7%) and 4,678,882/10,282,814 bytes (45.5%), with 13,044 region and
  7,003 page-only mappings. The five-book/1,804-page run took about 23 minutes
  at concurrency two; 400–500-page books took roughly 15–18 minutes.
- **Inspection:** Reconstructed every mapping against retained Poppler
  evidence. There are zero page regressions and zero duplicate published
  rectangles. Rendered and manually reviewed 51 unique records spanning each
  document's lowest-confidence region, lowest-confidence page-only mapping,
  highest-confidence mapping, and every retained known v1 failure span. The
  sample covered rotated layouts, tables, columns, forms, sidebars, unusual
  fonts, German text, dark backgrounds, and image-heavy pages; all were correct
  at their advertised precision. Review sheets stayed in `/tmp` because they
  contain copyrighted source pages.
- **Verification:** The focused enrichment suite passed 11 tests; the repository
  suite passed 223 tests plus 5 subtests. All 15 outputs pass the read-only
  enrichment verifier and independent Vulcan validation. `git diff --check`
  passed after the documentation updates. A conversion-extra attempt was unable
  to download a CUDA transitive dependency in the network-restricted sandbox;
  the visualization was rerun successfully with the host's existing Pillow,
  without changing project dependencies.
- **Decision:** The candidate passes the native-text canary, but remains gated.
  Deliberately add scan/OCR and equation-heavy inputs, record duration and peak
  memory, define size-aware bulk concurrency, and formally freeze the
  publication policy before invoking the explicit `--all` backfill.
- **Tooling:** Used `apply_patch`, `rg`, `sed`, `git`, `sqlite3`, `jq`, `uv`,
  `pytest`, `pdftotext`, `pdffonts`, `pdftoppm`, temporary local audit/render
  scripts, image inspection, and the independent `vulcan artifact validate`
  command. No production object, coordinator row, base MDAF, or source PDF was
  modified.

## 2026-08-27 (Roadmap Status Reconciliation)

- **Objective:** Reconcile the current conversion-program plan with repository
  state after the first enrichment canary inspection.
- **State:** The self-hosted coordinator, administration console, OIDC/SCIM,
  recipe-aware routing, worker/admin credentials, Citadel import, and recovery
  deployment are complete as recorded. All 1,377 historical conversions exist
  as conservative MDAFs alongside 431 raw-only queued sources. The enrichment
  implementation is present in the working tree but not yet committed or
  deployed. Its first 10-document derived-artifact canary was rejected for
  mapping accuracy, leaving 1,367 enrichment rows pending. Converter evaluation
  infrastructure and the frozen 43-rulebook/9,465-page corpus exist, while
  production-ready Marker 1/2, Docling, Datalab, and Mistral recipes and the
  blinded benchmark remain pending.
- **Critical path:** Correct anchor-bounded alignment and geometry publication,
  rerun and approve the enrichment canary, complete the append-only backfill,
  then run the multi-engine benchmark and promote selected MDAF-producing
  recipes into the multipurpose worker dispatcher.
- **Tooling:** Used `git status`, `git diff --stat`, `git log`, and the canonical
  `TODO.md`. Removed the work-log/finding entries created for the user's
  explicitly misdirected renderer question; no implementation was reverted.

## 2026-08-27 (PDF Enrichment Canary Accuracy Inspection)

- **Objective:** Decide whether the first structurally valid enrichment recipe
  is accurate enough to freeze and run across the legacy corpus.
- **Inspection:** Restricted the catalog query to the active recipe so obsolete
  development rows could not contaminate results. Reconstructed all 1,411
  aligner-created rectangles from retained Poppler evidence; checked normalized
  equality, confidence bands, selector bounds, page order, and geometry reuse.
  Rendered the source PDFs at 144 DPI and overlaid published rectangles for 35
  unique mappings: both confidence extremes in every document plus every page
  regression and reused rectangle. Compared exact UTF-8 Markdown spans against
  both visual source regions and native evidence.
- **Findings:** Structural validation is necessary but insufficient. The audit
  found two page regressions and six reused-rectangle groups involving 13
  mappings. Exact/high-confidence unique matches were generally tight, while
  mappings below 0.90 frequently selected excess block content; 68/79 had
  reconstructed sequence similarity below 0.90 and 60/79 had normalized length
  ratio below 0.80. Exact repeated labels can still score 1.0 at the wrong
  occurrence. Detailed evidence and corrective requirements are recorded in
  `docs/pdf_enrichment_canary_review.md`.
- **Decision:** Rejected recipe
  `blake3:cf33db6438b2a2fbe1e44538bf05cb64a40bf9d88e3f211b1276933c580e1598`
  for bulk backfill. Keep its ten outputs as immutable local experiments and
  keep the remaining 1,367 pending a new recipe and repeated review. No
  production state, source PDF, or conservative MDAF changed.
- **Tooling:** Used `git status`, `rg`, `sed`, `sqlite3`, `jq`, `unzip`,
  `pdftotext`, `pdftoppm`, temporary Python audit/render scripts, ImageMagick
  capability checks, and local image inspection. The temporary review sheets
  were not added to the repository because they contain rendered copyrighted
  source pages.
- **Verification:** `git diff --check` passes. Read-only CLI status still shows
  10 converted, 1,367 pending, and zero failed for the reviewed recipe; the
  enrichment verifier rechecked all ten as valid with zero invalid artifacts.

## 2026-08-27 (PDF Enrichment Vertical Slice and Canary)

- **Objective:** Implement the dependency-critical legacy PDF enrichment slice
  before bulk backfill or production converter promotion.
- **Implementation:** Added the `blobforge.enrichment` contract, Markdown
  segmenter, Poppler bbox-layout extractor, conservative token-indexed
  monotonic aligner, native evidence/report members, derived legacy MDAF
  builder, recipe-keyed resumable catalog, bounded/all-gated CLI, aggregate
  status, and read-only verifier. Hardened shared MDAF provenance validation.
- **Canary findings:** The smoke artifact showed Vulcan rejects a bare parent
  digest as an activity input; lineage now uses `derived_from` while activities
  reference retained concrete evidence. Real rulebooks exposed legitimate
  negative Poppler coordinates, so raw coordinates are retained and normalized
  rectangles are clipped with diagnostics. Exhaustive string matching was too
  slow; rare-token indexing now bounds candidates and sequence refinement.
  Every output-affecting change produced a new recipe identity.
- **Result:** Final recipe
  `blake3:cf33db6438b2a2fbe1e44538bf05cb64a40bf9d88e3f211b1276933c580e1598`
  processed 10 rulebooks / 153 pages with zero failures. All pass BlobForge,
  catalog/lineage, and Vulcan validation. Aggregate coverage is 1,411/2,355
  Markdown blocks (59.9%) and 319,013/492,744 semantic bytes (64.7%);
  per-document byte coverage ranges from 9.4% to 98.0%, so manual accuracy
  review remains required. No production object, coordinator row, or base MDAF
  changed.
- **Verification:** Focused suites reached 14 passing tests; the full suite
  passed 217 tests plus 5 subtests. The CLI verifier and independent Vulcan
  validation of all ten canary artifacts pass. Tools included `rg`, `sed`,
  `sqlite3`, `jq`, `pdftotext`, `pdfinfo`, `uv`, `pytest`, and `vulcan`.

## 2026-08-27 (Conversion Program Roadmap and PDF Enrichment Design)

- **Objective:** Turn the agreed legacy-enrichment, converter-comparison, and
  production-routing sequence into persistent project planning and design
  records.
- **Findings:** The repository already had a large `TODO.md`, a frozen
  43-document/9,465-page corpus, converter ABI and MDAF design documents, and
  1,377 conservative legacy MDAFs. Those artifacts preserve available page
  anchors and exact TOC-heading matches; they are not a completed
  PDF-to-Markdown alignment backfill.
- **Actions:** Added a canonical phased roadmap to `TODO.md`; documented
  deliverables, dependencies, exit gates, parallel work, candidate recipes,
  evaluation, and routing in `docs/conversion_program_roadmap.md`; specified the
  reusable evidence extraction, segmentation, monotonic alignment, structure,
  confidence, validation, canary, and resumable-backfill contract in
  `docs/pdf_enrichment_pipeline.md`; and recorded the architectural decision in
  `AGENTS.md`.
- **Tooling:** Used `rg`, `sed`, `git status`, and `apply_patch` to inventory,
  inspect, and update the repository. No sources, artifacts, coordinator state,
  or external services were changed.
- **Verification:** The documentation-only patch passed `git diff --check`; no
  application tests were required.

## 2026-08-27 (Self-hosted Administration Console)
- **Objective:** Replace the diagnostic root page with an admin interface that
  can operate the self-hosted application without hand-written API calls.
- **Design:** Organized the console around Overview, Jobs, Workers, Recipes,
  and Access. Kept canonical recipe content immutable while allowing names,
  notes, and retirement. Added per-identity dynamic worker credentials and
  revocable automation-admin tokens; environment workers remain controlled by
  Gandalf. Job deletion uses a recoverable trash tree and active requeue clears
  the lease so stale workers remain fenced.
- **Implementation:** Added paginated/filterable job queries; streamed
  SHA-256+BLAKE3 upload; source/artifact downloads; priority, requeue, retry,
  conversion, and delete actions; failure/artifact detail; worker create,
  rotate, and revoke; admin-token create/list/revoke; recipe metadata controls;
  and an administrative audit feed. Replaced the raw Snapshot JSON and recipe
  registry links with the task-oriented responsive console. The versioned
  same-origin JS/CSS bundle uses a no-inline-script CSP. Added SQLite migrations
  for recipe metadata, credential ownership, admin tokens, and audit events.
- **Security and correctness:** Worker credentials cannot access role-gated
  administration. OIDC session mutations require the configured exact origin.
  Tokens are shown once and only hashes are stored. Source download capability
  tests caught and fixed missing digest query/signature context before release.
  Dynamic workers survive restarts; environment-managed credentials cannot be
  silently rotated in the database. Job deletion refuses active work and moves
  files rather than unlinking them.
- **Tooling and verification:** Audited application/database/storage/UI and
  existing authorization docs with `rg`, `sed`, and SQLite; edited with
  `apply_patch`; checked the browser bundle with Node; compiled Python; ran
  focused and full pytest; built both Python distributions; and checked the
  diff. The first isolated `uv build` could not resolve Hatchling because the
  sandbox has no DNS; the approved network retry built the sdist and wheel.
  Firefox is installed, but sandbox socket binding prevented a local
  screenshot server, so no screenshot claim is made. Focused server coverage
  passes 12 tests and the complete suite passes 211 tests plus 5 subtests.
  A copied 1,808-source migration database upgraded additively with
  `quick_check=ok`, retained every source, and gained the expected worker,
  recipe, token, and audit schema. Coverage includes the complete job/source
  lifecycle, tagged legacy recipe identifiers, credential restart/rotation/
  revocation, revoked-worker recipe availability, recipe retirement, and OIDC
  same-origin enforcement.
- **Publication and deployment:** Committed and pushed BlobForge revision
  `81f0584`; GitHub Actions run `33099437656` passed its test/distribution and
  multi-architecture server-image jobs and published immutable manifest
  `sha256:22ce040caa1c3f4c5ab82a4275145fe5eb6e0f45681118f339c158c9b1aeb289`.
  Updated Gandalf's canonical service contract, compiled inventory and external
  dependency views, updated/rendered the private runbook through Vulcan, passed
  740 tests plus 4 subtests and 13 Bunny tests, and committed the isolated
  deployment as `9dc8837d` while preserving concurrent calendar changes. The
  private Outline dry-run was approval-denied because it would transmit runbook
  content externally, so no remote documentation mutation occurred. The first
  Citadel apply reached no remote task because the SSH agent refused the key;
  the second used the wrong host-key form and also changed nothing. The third
  used Gandalf's canonical SSH mapping plus its dedicated key and completed
  with 53 tasks, 2 expected changes, and no failures. Production runs the exact
  manifest, has the new token/audit tables, returns healthy SQLite/filesystem
  API status, redirects unauthenticated root requests to OIDC, and serves the
  immutable `management-v1.js` bundle containing job, worker, token, and recipe
  controls. HEAD is not implemented for root/static FastAPI routes (405); GET
  canaries pass and are the supported browser path. The CPU/CUDA worker-image
  matrix remained in progress after the independently successful server image
  was deployed; those images are not part of the coordinator rollout.

## 2026-08-21 (done_seq migration ordering fix)
- **Objective:** Fix `Internal error` (500) on every coordinator API request
  after upgrading a pre-0.4.0 database.
- **Root cause:** `ensureSchema()` runs the static SCHEMA batch first, and that
  batch included `CREATE INDEX IF NOT EXISTS jobs_done_since_idx ON
  jobs(status,done_seq)`. On an existing database whose `jobs` table predates
  the `done_seq` column, the index DDL referenced a missing column, the whole
  batch threw `no such column: done_seq`, and every route (even
  unauthenticated `/api/v1/health`) returned `{"error":"Internal error"}`.
- **Fix:** Removed `jobs_done_since_idx` from the static SCHEMA batch. The index
  is created only inside the migration block, after the `done_seq` column is
  guaranteed to exist (fresh DBs get the column from `CREATE TABLE`, upgraded
  DBs from `ALTER TABLE ... ADD COLUMN`). Added a coordinator spec that creates
  a pre-`done_seq` `jobs` table, runs `ensureSchema()`, and asserts the column
  and index exist and health passes.
- **Validation:** `npm test` (22 passed) and `npm run check` clean.

## 2026-08-18 (Cross-stack review fixes)
- **Objective:** Close the full-app review findings across the Python client and
  the Bunny coordinator.
- **Coordination hardening:** `GET /api/v1/jobs/done-since` now pages over
  `(completed_at, done_seq)` with a `file_hash` prefix filter on `completed_at`
  only for backwards compatibility; the schema gained a `done_seq` column
  (backfilled) and an index. `fail()`/`release()` fence on the lease token
  (`lease_token`/`worker_id` match, 409 on mismatch); expired leases are
  recovered only by `recoverExpiredLeases()` which now returns a `count`.
  `snapshot()` is pure and never mutates lease state. `ensureSchema()` tolerates
  ALTER races on replica SQLite. `app.ts` `fetch()` uses `return await` on every
  handler so rejected promises become 4xx ClientErrors instead of escaping the
  try/catch; expected 4xx errors no longer spam `console.error`. The duplicate
  `timestamp` in `fail()` was removed.
- **Client hardening:** `hash_index.py` watermark is now versioned
  (`{version: 2, since, cursor}`); a pre-`done_seq` watermark forces a full
  resync. Worker catches transient `CoordinatorError` at acquire/suspend/resume/
  complete/fail and loop boundaries (401/403 re-raised fatal) and only clears
  local state after a successful release. `utc_now_iso()` in `utils.py` replaces
  local-wall-clock + `"Z"` timestamps. Conversion children are killed as a
  process group (`start_new_session` + `os.killpg`, `proc.kill()` fallback).
  `release_lock` now checks ownership when a `worker_id` is supplied. The mock
  S3 became a deterministic in-memory store (removed the random 10% failure).
  CLI management commands that need admin mutations (`reprioritize`, `retry`,
  `janitor`, `retry-all`, `clear-dead`, `cancel`) are thin stubs
  (`--management-ui` required) because admin endpoints use IndieAuth session
  auth; `main()` reports clean errors with `BLOBFORGE_DEBUG` for tracebacks;
  dead `worker --dry-run` and `config --show` flags were removed.
  `rewrite_asset_paths()` in `utils.py` centralizes the markdown asset-link
  rewrite (worker.py, conversion_child.py, cli.py) and only rewrites markdown
  link targets naming a known extracted image. Removed the legacy
  `MAX_RETRIES`/`HEARTBEAT_INTERVAL_SECONDS`/`STALE_TIMEOUT_MINUTES`/
  `CONVERSION_TIMEOUT_SECONDS` constants from `config.py` (getters remain);
  tests now assert through `get_*()`.
- **Console:** Version constants (`APP_JS_VERSION`, `APP_CSS_VERSION`,
  `LOGIN_JS_VERSION`, `MARKDOWN_JS_VERSION`, `BRAND_SVG_VERSION`) are centralized
  in `ui.ts` and drive the routes/ETags in `app.ts` (`DOCS_VERSION` drives the
  docs route). JS bumped to `app-v9.js` and CSS to `app-v8.css` (bytes changed).
  Sign-out POSTs `/auth/logout`. The viewer CSS no longer conflicts with the
  console layout. The `%PDF-` sniff scans the first 1024 bytes. The vestigial
  `Vary: Cookie` on private HTML responses was removed.
- **Validation:** Full Python suite `126 passed`; bunny `21 passed`; `npm run
  check` (tsc + generate-markdown) clean.

## 2026-08-18 (Coordinator done-since watermark reconciliation)
- **Objective:** Replace the 6h status TTL with an efficient watermark sync.
  The user rejected the TTL approach; the coordinator's done-set can be
  encapsulated into a single SQL query so each hydrate run pulls only hashes
  completed since the last sync and answers membership locally.
- **Server:** Added `jobs_done_since_idx` on `jobs(status, completed_at,
  file_hash)` and `listDoneSince(since, cursor, limit)` in
  `bunny/src/database.ts` — keyset pagination `WHERE status='done' AND
  (completed_at > ? OR (completed_at = ? AND file_hash > ?)) ORDER BY
  completed_at ASC, file_hash ASC LIMIT ?` — plus `GET /api/v1/jobs/done-since`
  (`since`, `cursor`, `limit` default 5,000, max 20,000) in
  `bunny/src/app.ts`, authorized for client tokens and admin tokens.
- **Client:** Rewrote `blobforge/hash_index.py` to a done-set mirror
  (`done_hashes`) + `(since_ms, cursor)` watermark in a `meta` table, replacing
  the TTL `hash_status` table. `CoordinatorClient.sync_done_hashes` pages
  `done-since` until `complete` and returns `(hashes, next_since, next_cursor)`.
  Hydrator reconciles via watermark, answers membership with `is_done`, resets
  on `--refresh-status`, and drops a hash from the mirror when a signed
  download fails. Removed `--status-ttl` and
  `BLOBFORGE_HYDRATE_STATUS_TTL_SECONDS`.
- **Validation:** Rewrote `tests/test_hash_index.py` for the new API; updated
  hydrate tests (watermark sync, mirror reuse, refresh resets, hash reuse
  without read) with a `sync_done_hashes`-aware FakeCoordinator; added
  `sync_done_hashes` pagination tests to `tests/test_coordinator_client.py`;
  added a bunny spec covering done-since pagination, watermark resume, and auth.
  Full suite: Python `121 passed`, bunny `16 passed`; `npm run check` clean.

## 2026-08-18 (Persistent hydration index and incremental reconciliation)
- **Objective:** Speed up `blobforge hydrate` on large libraries (30k+ PDFs).
  Repeated runs were slow because the xattr-only hash cache silently misses on
  filesystems without `user_xattr`, forcing every file to be re-read, and the
  whole unique-hash set was re-sent to the coordinator every run.
- **Design:** Added `blobforge/hash_index.py`, a WAL-mode SQLite index at
  `~/.cache/blobforge/hash_index.sqlite3` (overridable via `BLOBFORGE_CACHE_DIR`
  or `BLOBFORGE_HASH_INDEX_PATH`). Two tables: file hashes keyed by
  `(path, size, mtime_ns)` and done-status answers keyed by content hash with a
  timestamp. Hydration now reuses indexed hashes instead of re-reading files,
  and reconciles the done-set incrementally: known-done hashes (immutable
  content-addressed outputs) are never re-queried, missing hashes are re-queried
  only after a TTL (default 6h). Added `--refresh-status` and `--status-ttl`
  hydrate CLI flags. A full range-based set-reconciliation protocol (IBLT-style)
  was considered and rejected — the ~2 MB candidate payload makes the client-side
  delta snapshot the fitting optimization.
- **Validation:** New `tests/test_hash_index.py` (round-trip, invalidation,
  sticky-done, TTL expiry, batch) and hydrate-level delta tests (known-done skip,
  TTL re-query, `--refresh-status`, hash-reuse-without-read). Full suite: `119
  passed` (plus one pre-existing datetime warning).

## 2026-08-18 (Hydration progress and bulk-status chunking)
- **Objective:** `blobforge hydrate` gave no feedback while hashing tens of
  thousands of local PDFs, and the bulk status check silently dropped hashes.
- **Root cause:** The local hash preflight loop printed nothing until it
  finished, and `CoordinatorClient.check_statuses` sent every unique hash in a
  single `POST /api/v1/jobs/status` request even though the coordinator answers
  at most 5,000 hashes per request (`slice(0, 5000)`), so larger candidate sets
  were truncated server-side.
- **Fix:** `check_statuses` now dedupes, chunks into 5,000-hash batches matching
  the server limit, merges per-chunk results, and accepts an optional
  `progress(checked, total)` callback. Hydration prints `[hash] n/total files`
  every 100 files during local preflight and `[status] n/total hashes` after
  each status chunk. Added tests for chunking/progress and updated the
  coordinator fake.
- **Validation:** Full Python suite passes (`108 passed`, one pre-existing
  datetime warning).

## 2026-08-18 (Admin token bundle version and usage instructions)
- **Objective:** Fix the management console's **Create admin token** button,
  which did nothing, and document how to use an admin token from the CLI.
- **Root cause:** The `management_ui.ts` bundle gained the admin-token handlers,
  but the versioned asset path `/static/app-v7.js` was not bumped. Because
  versioned assets are immutable for one year, browsers kept executing the old
  cached bundle, which had no `#new-admin-token` handler.
- **Fix:** Bumped the JavaScript bundle to `/static/app-v8.js` with a matching
  ETag name in `app.ts`, updated the inline `<script>` reference in `ui.ts`, and
  updated the coordinator static-asset tests. Only the JS version changed; the
  stylesheet stayed at `app-v7.css`.
- **Instructions:** The admin-token credential panel now spells out the exact
  `export BLOBFORGE_COORDINATOR_URL=...` and `export BLOBFORGE_COORDINATOR_TOKEN=...`
  lines plus example `blobforge ingest`/`hydrate`/`download` invocations, and
  the copy button copies the whole instruction block.
- **Validation:** `npm run check` and all 15 coordinator tests pass.

## 2026-08-18 (Revocable admin tokens and coordinator-driven hydration)
- **Objective:** Let operators create per-person admin tokens for `ingest` and
  `hydrate`, replace the S3 done-hash index with an optimized bulk status API,
  and remove direct S3 access from all client-side commands.
- **Design:** Admins mint `bfa_...` tokens in the management console. They are
  stored as SHA-256 hashes, shown once, bound to one token ID, revocable, and
  accepted by `workerApi` for job enqueue/read, bulk status, and signed
  raw-upload/output-download URLs. `POST /api/v1/jobs/status` resolves up to
  5,000 hashes per request via a jobs/files join. `blobforge hydrate` uses the
  bulk call as its single remote preflight and streams archives through signed
  GET URLs; `blobforge ingest` uploads raw PDFs through signed PUT URLs;
  `download`/`preview` stream through signed GET URLs. S3 done-index/per-hash
  checks remain only as no-coordinator fallbacks.
- **Implementation:** Added `admin_credentials` to the coordinator schema with
  create/authenticate/list/revoke methods, admin-token identity in `workerApi`,
  the `jobs/status`, `raw-upload-url`, and `download-url` endpoints, admin token
  management UI and routes, `getJobStatuses` batch join (400-hash chunks), and
  backup table inclusion. Python: added
  `check_statuses`/`output_download_url`/`raw_upload_url`/`upload_raw`/
  `download_output` to `CoordinatorClient`; reworked `hydrator.py`, `ingestor.py`,
  and `cmd_download`/`cmd_preview`/`cmd_ingest`/`cmd_hydrate` to coordinator
  transfers; added `--coordinator-url`/`--token` flags to ingest/hydrate/
  download/preview. Updated CLI docs, README, coordination backend design, and
  hydrate design.
- **Validation:** Bunny `npm run check` and all `15` coordinator tests pass
  (new admin-token lifecycle test covers create/list/revoke, bulk status, signed
  URLs, and revocation denial). Python suite passes (`107 passed`, one
  pre-existing datetime warning), including new hydrator coordinator-path and
  coordinator-client tests and the reworked ingest logging test.

## 2026-07-21 (Public documentation landing page)
- **Objective:** Use the Edge Script root as a useful BlobForge landing/help
  page while ensuring scraper and bot traffic is served from the Bunny pull-zone
  cache instead of repeatedly executing database-backed application paths.
- **Design:** Serve a static technical handbook at `/`, move administrator login
  to `/login`, and keep the authenticated application at `/console`. Route the
  landing page, robots policy, IndieAuth metadata, and content-addressed static
  assets before database initialization. Give versioned CSS, JavaScript, and
  brand assets one-year immutable browser/CDN/surrogate caching; cache the HTML,
  robots policy, and origin-dependent metadata for one day at the CDN with short
  browser freshness and stale revalidation. Preserve private no-store headers
  for login, console, auth, and API responses.
- **Implementation:** Added a responsive public handbook with product overview,
  architecture flow, no-clone worker setup, schedules, service operations,
  coordinator settings, security/recovery notes, and common questions. Added a
  versioned brand asset and stylesheet, moved sign-in to `/login`, retained the
  application at `/console`, redirected expired sessions to the new login route,
  and published a robots policy that excludes private/API paths.
- **Caching:** Static routes run before database initialization. Versioned assets
  use one-year immutable browser/CDN/surrogate caching; public documents use a
  five-minute browser TTL and one-day edge TTL with stale revalidation. ETags,
  conditional 304 responses, HEAD handling, and database-free unknown-route
  rejection are covered by tests. Private surfaces remain `no-store`.
- **Validation:** Bunny TypeScript checking, all `14` tests, and the production
  Edge build passed (328.4 KiB). The complete Python suite passed (`102 passed`,
  one pre-existing datetime warning). Workflow YAML parsing and
  `git diff --check` passed. No local Chromium/Playwright browser was available
  for screenshot-based layout verification; responsive behavior is constrained
  in CSS and structural/static-response behavior is covered in jsdom tests.
- **Status:** Implementation, documentation, cache policy, and regression
  verification are complete. Prepared as one focused repository commit at the
  user's request.

## 2026-07-21 (Coordinator cost and worker distribution optimization)
- **Objective:** Reduce idle Edge Script traffic, represent run-window suspension explicitly, make heartbeat policy live-configurable, separate revoked workers from the active fleet, and provide a practical no-clone Linux worker setup.
- **Design:**
    1. Model run eligibility through reusable run-condition results carrying a reason and optional resume timestamp; run windows become the first condition implementation.
    2. Publish suspension and resume only on state transitions, pause heartbeat publishing while suspended, and keep lease renewal independent from optional idle/progress heartbeats.
    3. Piggyback runtime configuration on worker register, claim, and heartbeat responses so interval changes apply after the next existing coordinator request without adding config polling.
    4. Let an active job heartbeat renew the lease and update worker state in one request; avoid a second worker-heartbeat request.
    5. Exclude revoked credentials/runtime records from ordinary snapshots and load revoked enrollments only through an explicit admin view.
    6. Use the existing GHCR container publication as the Linux distribution boundary, backed by a systemd installer that stores credentials in a private environment file and persists the model cache.
- **Implementation:** Added modular run-condition decisions and one-shot
  suspended/idle transitions, paused all suspended heartbeat traffic, folded
  active worker updates into lease renewal, added live response-piggybacked
  heartbeat policy, and exposed a boolean lease-only mode. Bunny Database now
  stores suspension detail and excludes revoked credentials from ordinary fleet
  snapshots; the console loads revoked workers only in its explicit dialog.
  Added a systemd user-service installer, persistent model cache, CPU-default
  multi-architecture image, and separate amd64 CUDA image selected by `--gpu`.
- **Compatibility decision:** All workers are stopped for this rollout, so the
  coordinator and workers use one claim contract with no protocol negotiation:
  `{ job, config }`, including `job: null` for an empty queue. The Python client
  rejects obsolete HTTP 204 and direct-job claim responses.
- **Validation:** Complete Python suite passed (`102 passed`, one pre-existing
  datetime warning); Ruff passed on changed Python runtime/tests. Bunny type
  checking, all `14` tests, and production build passed. Installer syntax/help
  checks passed. Local Podman builds and CLI probes passed; the final amd64 CPU
  image imports `torch 2.10.0+cpu`, retains `filelock 3.30.2`, reports no CUDA,
  and is 1.88 GB uncompressed versus 16.1 GB for the CUDA image.
- **Status:** Implementation, documentation, distribution setup, and regression
  verification are complete. Prepared as one focused repository commit at the
  user's request.

## 2026-07-16 (Least-privilege worker enrollment and transfers)
- **Objective:** Remove bucket credentials from conversion workers, move terminal worker views to the coordinator, and add UI-managed per-worker enrollment.
- **Design:**
    1. Split the former shared worker credential into a trusted ingestor/CLI `CLIENT_API_TOKEN` and individually enrolled worker tokens.
    2. Store only SHA-256 worker-token hashes, bind each token to a server-generated worker ID, show plaintext only in the creation response, and support immediate UI revocation.
    3. Issue the raw PDF GET URL at claim time but issue the output PUT URL only immediately before upload because conversions may outlive a claim-time upload URL.
    4. Bind output URL issuance to the authenticated worker plus its active fenced lease, and verify object existence before database completion.
    5. Keep trusted ingestion on S3 for raw uploads; defer optional browser PDF upload as a tracked follow-up.
- **Implementation:** Added WebCrypto SigV4 presigning, S3 path/virtual-host addressing, database worker credentials and audits, worker identity enforcement, create/revoke admin APIs, enrollment UI, streamed Python signed-URL transfers, coordinator-only worker startup, CLI coordinator overrides, and coordinator-backed worker listing.
- **Validation:** Bunny TypeScript checks pass; Bunny/libSQL/SigV4/UI tests pass (`10 passed`); the complete Python suite passes (`94 passed, 5 subtests passed`). The Edge Script production build succeeds at 210.2 KiB, and the shipped browser module is syntax-checked by the UI test.
- **Status:** Implementation, rollout guidance, architecture documentation, focused security boundaries, and full regression verification are complete. Changes are ready to review and commit.

## 2026-07-16 (Cookie-independent Bunny session commit)
- **Objective:** Commit the validated cookie-independent IndieAuth session transport at the user's request.
- **Actions:** Confirmed the scope contains the fragment bootstrap, browser-stored signed session, authenticated admin API header, rollout cache bust, focused tests, and required documentation/protocol updates.
- **Status:** Prepared for one focused publication commit.

## 2026-07-16 (Cookie-independent Bunny admin sessions)
- **Objective:** Fix IndieAuth returning to an unauthenticated login screen after the prior cookie transport hardening.
- **Evidence:** The deployed `/auth/status` response reported `authenticated: false` and `cookie_present: false` while both the public request URL and forwarded protocol were HTTPS. This isolated the remaining failure to cookie transport rather than signature validation, database replication, or callback identity validation.
- **Implementation:**
    1. Changed the successful callback to redirect to `/console` with the HMAC-signed session in a URL fragment instead of a response cookie.
    2. Added a public, data-free console shell. Its same-origin application module copies the fragment token into browser storage and immediately replaces the history entry without the fragment.
    3. Added `BlobForge-Session` Authorization-header validation to the existing signed-session verifier, retaining cookie parsing only as backward-compatible fallback.
    4. Updated all management UI API calls to send the session header, clear expired sessions, and return to login on `401`; sign-out now removes the local session.
    5. Included the authenticated identity in the protected admin snapshot and expanded `/auth/status` to distinguish header and cookie transport.
    6. Preserved strict CSP, CDN/browser no-store controls, signed expiry, admin allowlisting, and same-origin validation for writes.
    7. Versioned the authentication script URLs so browsers cannot combine the new callback with the previously cacheable cookie-only application bundle during rollout.
- **Validation:** TypeScript checking passed; all Bunny/libSQL tests pass (`8 passed`), including a full mocked IndieAuth callback, fragment handoff, authenticated snapshot, and header-based status diagnostic. The production Edge Script bundle built successfully at 195.2 KiB, and `git diff --check` passed.
- **Status:** Cookie-independent session transport is implemented, documented, and ready to commit and deploy.

## 2026-07-16 (Bunny session transport fix commit)
- **Objective:** Commit the validated session-cookie and CDN-cache hardening so Bunny can publish the fix.
- **Actions:** Confirmed the scope contains the scheme-independent host cookie, explicit CDN no-store controls, `/auth/status`, focused tests, and required documentation updates.
- **Status:** Prepared for publication commit.

## 2026-07-16 (Bunny session-cookie transport hardening)
- **Objective:** Fix a successful IndieAuth callback still returning to the unauthenticated login view.
- **Investigation:**
    1. Confirmed the remaining failure boundary was callback `Set-Cookie` delivery or validation on the next root request; authorization, signed state, and identity validation had completed.
    2. Reviewed Bunny's current standalone scripting, request/response, pull-zone, environment, and caching documentation.
    3. Probed the live deployment headers with `curl -fsSI`. Bunny returned `cache-control: no-cache`, `cdn-cache: MISS`, and did not retain the application's intended `Vary: Cookie`, showing that explicit CDN-facing cache directives were warranted.
- **Implementation:**
    1. Removed request-protocol-dependent cookie naming. Production now always uses `__Host-blobforge_session` with `Secure`, `HttpOnly`, `SameSite=Lax`, and `Path=/`, preventing callback/root mismatches if Bunny edge instances expose different internal schemes.
    2. Added `private, no-store, no-cache, max-age=0, must-revalidate`, `CDN-Cache-Control: no-store`, `Surrogate-Control: no-store`, `Pragma: no-cache`, and `Vary: Cookie` where appropriate.
    3. Applied the same no-cache controls to the callback and logout redirects.
    4. Added `GET /auth/status`, which safely reports cookie presence, signed-session validity, identity, request protocol, and forwarded protocol without exposing the token.
    5. Extended the full callback test to assert the exact host cookie, Secure attribute, no-store response, and authenticated `/auth/status` result.
- **Validation:** `npm run check` passed; all Bunny/libSQL tests passed (`8 passed`).
- **Final validation:** Production bundle built successfully at 194.2 KiB; `git diff --check` is clean.
- **Status:** Cookie naming/cache hardening and diagnostics are implemented and validated. Deployment remains.

## 2026-07-16 (IndieAuth CSP fix commit)
- **Objective:** Commit the validated script-driven IndieAuth navigation fix at the user's request.
- **Actions:**
    1. Confirmed the scope contains only the CSP navigation fix, focused test, and required documentation/protocol updates.
    2. Prepared the changes for one focused Git commit.
- **Status:** Ready to commit.

## 2026-07-16 (IndieAuth form CSP redirect fix)
- **Objective:** Fix the profile login form being blocked before reaching the IndieAuth authorization endpoint.
- **Diagnosis:** Browser console evidence showed `form-action 'self'` blocking the request. The form submitted to same-origin `/auth/login`, but that response redirects externally; browsers apply the form-action policy to the redirected destination as well.
- **Implementation:**
    1. Kept `form-action 'self'` instead of weakening CSP to permit arbitrary HTTPS form destinations.
    2. Added a local `/login.js` module that prevents form submission, preserves standard form validation/Enter-key behavior, constructs the same-origin login URL with `URLSearchParams`, and starts a top-level navigation with `window.location.assign`.
    3. Added the script only to the unauthenticated page and exposed it with the same local-script CSP and static cache policy as the application assets.
    4. Extended the login UI test to verify the module is included, served as JavaScript, and uses top-level navigation.
- **Validation:** `npm run check` passed; all Bunny/libSQL tests passed (`8 passed`).
- **Status:** CSP redirect issue fixed without broadening the form-action policy. Redeployment is required.

## 2026-07-16 (Bunny IndieAuth fix commit)
- **Objective:** Commit the validated cross-edge IndieAuth session and multi-admin login fix at the user's request.
- **Actions:**
    1. Reviewed the final working-tree scope with `git status --short`.
    2. Confirmed the commit contains signed PKCE/session tokens, the dedicated signing secret, normalized profile input, multi-admin allowlisting, focused tests, and documentation updates.
- **Status:** Prepared for a single focused Git commit.

## 2026-07-16 (Bunny IndieAuth session and multi-admin fix)
- **Objective:** Fix IndieAuth redirecting back to the login page and support multiple administrators selected through a profile URL field.
- **Diagnosis:**
    1. Inspected Bunny auth routing, database session methods, UI rendering, runtime variables, and existing tests.
    2. Identified that the callback wrote a database session and immediately redirected to a new request that could run at another Bunny edge and read before replica visibility. PKCE attempts had the same cross-request dependency.
- **Implementation:**
    1. Replaced database-backed PKCE attempts and sessions with HMAC-signed self-contained tokens using a new, independently scoped `SESSION_SIGNING_SECRET`.
    2. Embedded the requested identity, verifier, token endpoint, nonce, and expiry in signed OAuth state; callback validation requires a valid signature, freshness, current allowlist membership, and an exact returned identity.
    3. Added a signed session cookie containing only identity and expiry. It remains `Secure`, `HttpOnly`, and `SameSite=Lax`, and is immediately verifiable by any edge without a database read.
    4. Removed unused authentication/session tables and database methods from the new schema. Existing deployed tables can remain harmlessly until a future explicit cleanup migration.
    5. Added a login form for IndieAuth profile URLs. Bare domains gain `https://`, non-HTTPS URLs are rejected, and disallowed identities are rejected before network discovery.
    6. Added comma-separated `ADMIN_MES` support with backward-compatible `ADMIN_ME` fallback. The default allowlist still contains `https://eric.wendland.dev/`.
    7. Updated the environment template and deployment/architecture documentation with the new signing secret, multi-admin configuration, and rotation behavior.
- **Validation so far:**
    1. `npm run check` passed.
    2. Bunny/libSQL tests pass (`8 passed`), including login-field rendering, bare-domain normalization, non-HTTPS rejection, pre-discovery allowlist rejection, full mocked IndieAuth callback, and immediate signed-session dashboard access.
- **Final validation:**
    1. Final Edge Script build passed; bundle size is 192.9 KiB.
    2. Complete Python suite passed: `88 passed, 5 subtests passed`.
- **Status:** IndieAuth session and multi-admin fix is implemented, documented, and fully validated. Deployment requires adding `SESSION_SIGNING_SECRET`, changing the admin variable to `ADMIN_MES` as needed, and publishing the new bundle.

## 2026-07-16 (Bunny backend commit)
- **Objective:** Commit the completed migration from Cloudflare coordination to Bunny Edge Scripting and Bunny Database at the user's request.
- **Actions:**
    1. Reviewed the final working-tree scope with `git status --short`.
    2. Confirmed the commit replaces the complete Cloudflare project and documentation with the tested Bunny service, database state machine, UI, tests, Python terminology updates, and deployment guide.
- **Status:** Prepared for a single focused Git commit.

## 2026-07-16 (Rebuild coordination backend on Bunny)
- **Objective:** Remove the Cloudflare implementation and rebuild the coordination service so it runs efficiently on bunny.net.
- **Platform research:**
    1. Reviewed current official Bunny Database, SQL API, Edge Scripting, limits, secrets, pricing, GitHub integration, deployment, and database-to-script connection documentation.
    2. Confirmed Bunny Database launched in February 2026 as a managed libSQL/SQLite service, integrates directly with Edge Scripts, scales down while idle, and remains in public preview.
    3. Confirmed standalone Edge Scripts support TypeScript/JavaScript, Web APIs, secrets, external HTTP calls, and dynamic UI/API responses within 30 seconds CPU, 128 MB active memory, and 50 subrequests per invocation.
    4. Determined that the workload does not need an always-on Magic Container: PDF conversion stays external and lease recovery can run atomically on the next worker poll or UI request.
- **Implementation:**
    1. Removed the `cloudflare/` Wrangler, Durable Object, test-runtime, generated dependency, and deployment artifacts.
    2. Added `bunny/` with the Bunny Edge Script SDK, web libSQL client, esbuild single-file output, TypeScript checks, Vitest, environment template, and gitignored build/secrets.
    3. Split the service into a Bunny runtime entry point, HTTP/IndieAuth application, database layer, and reusable management UI.
    4. Recreated file metadata, job states, priority ordering, retry/dead-letter handling, workers, progress, config, logs, and audit tables in Bunny Database. (Authentication state was subsequently moved to signed tokens; see the later IndieAuth fix entry.)
    5. Implemented claims as atomic SQLite `UPDATE ... RETURNING` operations with opaque lease tokens and a same-worker exclusion. Repeated claims return the existing lease, preserving request-loss safety.
    6. Replaced Durable Object alarms with atomic lazy lease recovery before claims and snapshots plus explicit UI recovery. Recovery increments retry state, clears stale workers, and permits immediate reclaim.
    7. Preserved authenticated enqueue/read/claim/heartbeat/complete/fail/release APIs, exponential failure backoff, migration import, worker/admin token separation, IndieAuth + PKCE admin restriction for `https://eric.wendland.dev/`, same-origin checks, secure cookies, and CSP.
    8. Tightened stale-request fencing so an old heartbeat/completion/failure/release cannot mark a worker idle after it has acquired a different job.
    9. Kept the generic Python coordinator URL/token contract and replaced Cloudflare-specific operator messages with Bunny terminology.
    10. Replaced the architecture/deployment guide with `docs/bunny_coordination_backend.md`, including dashboard setup, secrets, build entry file, safe backlog migration, public-preview caveat, and efficiency analysis; updated README, TODO, and findings.
- **Tool executions and validation so far:**
    1. Used official web documentation/search plus `rg`, `sed`, `git show`, and Git status/diff inspection to map the existing implementation and platform constraints.
    2. Installed 79 Bunny/libSQL/build/test packages with `npm install`; npm reported no vulnerabilities.
    3. Ran `npm run check` successfully.
    4. Built a 191.3 KiB single-file Edge Script with `npm run build`, well below Bunny's documented script-size limit.
    5. Ran local libSQL API tests: health/database initialization, worker-token rejection, enqueue/claim/repeated-claim/lease fencing/heartbeat/completion, separately authenticated terminal-state import, lazy expired-lease recovery, and IndieAuth client metadata all pass (`5 passed`).
- **Final validation:**
    1. Re-ran Python compilation through uv; all changed Python modules compiled.
    2. Ran the complete Python suite: `88 passed, 5 subtests passed`.
    3. Rebuilt the final Edge Script at 192.0 KiB and verified the bundle contains no Node built-in imports; the Bunny SDK remains the only runtime external.
    4. Re-ran the production dependency audit with registry access: `0 vulnerabilities`.
    5. Ran `git diff --check`: clean.
- **Status:** Bunny-native implementation, tests, security audit, deployment/cutover documentation, and repository protocol updates are complete. Live Bunny resource creation, secrets, hostname setup, deployment, and backlog migration remain explicit operator actions.

## 2026-07-16 (Cloudflare backend commit)
- **Objective:** Commit the completed Cloudflare coordination backend implementation at the user's request.
- **Actions:**
    1. Reviewed the final working-tree scope with `git status --short`.
    2. Confirmed the commit contains the Worker/Durable Object service, management UI, Python integration, migration command, tests, and documentation from the completed implementation.
- **Status:** Prepared for a single focused Git commit.

## 2026-07-15 (Cloudflare coordination backend and management UI)
- **Objective:** Replace the long-lived S3 queue coordination mechanism with Cloudflare Workers while retaining Bunny/S3 for blobs, and add an IndieAuth-protected administration interface.
- **Research and inspection:**
    1. Inspected the repository, current queue/manifest paths, worker shutdown/retry behavior, CLI commands, tests, and the clean Git baseline after removal of the prior PostgreSQL prototype.
    2. Reviewed current Cloudflare Durable Object SQLite, transaction, alarm, and local-test behavior using official documentation.
    3. Reviewed the IndieAuth living standard and discovered `https://eric.wendland.dev/` metadata. Its current authorization server metadata is at `https://indieauth.tionis.dev/.well-known/oauth-authorization-server`.
    4. Compared the coordination workload with platform constraints and selected one strongly ordered Durable Object because the backlog is hundreds of records but throughput is only a few completions per day.
- **Implementation:**
    1. Added the `cloudflare/` TypeScript service, pinned Node-20-compatible Wrangler/test dependencies, Wrangler Durable Object migration, TypeScript configuration, and gitignored local state/secrets.
    2. Added the SQLite schema for files, paths, tags, jobs, workers, logs, runtime config, IndieAuth attempts/sessions, and audit records.
    3. Added authenticated enqueue/read/claim/heartbeat/complete/fail/release APIs with priority ordering, lease fencing, retry-safe calls, exponential retry availability, dead-letter transitions, and Durable Object alarm recovery.
    4. Added a responsive dependency-free management UI for queue totals, recent jobs, workers, progress, priorities, retry/cancel, expired-lease recovery, and runtime settings.
    5. Added IndieAuth metadata discovery, Authorization Code + PKCE, state expiry, exact canonical admin identity enforcement for `https://eric.wendland.dev/`, hashed sessions, secure cookies, origin checks, CSP, and escaped server-rendered identity data.
    6. Added `blobforge/coordinator_client.py` and integrated coordinator selection into configuration, ingestion, workers, dashboard/list/status, heartbeats, metrics, completion, failure, release, and shutdown. S3 remains the raw/output store and remains a compatibility coordination fallback when coordinator variables are absent.
    7. Added output-upload ambiguity recovery: a leased coordinator job with an existing output ZIP is finalized without repeating conversion.
    8. Added a separately scoped `MIGRATION_API_TOKEN`, a transactional batch import endpoint, and `blobforge coordinator-migrate`. The importer scans legacy manifest/todo/processing/failed/dead/done state, applies deterministic state precedence, preserves retries and metadata, and converts unsafe old processing locks to todo.
    9. Guarded legacy S3 queue mutation/search commands when the Cloudflare backend is active, directing operators to the authenticated UI instead of silently editing stale queue markers.
    10. Added architecture, security, deployment, client configuration, and explicit stop-the-world cutover documentation in `docs/cloudflare_coordination_backend.md`; updated README and task/findings records.
- **Tool executions and validation:**
    1. Used `rg`, `sed`, `git status`, and targeted source reads throughout inspection and verification.
    2. Installed the pinned Cloudflare development dependency tree with `npm install`; adjusted versions after the latest toolchain required Node 22 and addressed isolated SQLite test storage behavior.
    3. Ran `npm run check` repeatedly after implementation; final TypeScript check passed.
    4. Ran Cloudflare Vitest through the local Workers runtime (host interface permission required): `4 passed` covering auth rejection, enqueue/claim/fenced heartbeat/completion, IndieAuth client metadata, and separately authenticated legacy import.
    5. Ran Python compilation through uv with a writable `/tmp` cache; all changed modules compiled.
    6. Added coordinator HTTP-client tests; `2 passed`.
    7. Ran the complete Python suite through uv: `88 passed, 5 subtests passed`.
    8. Ran `npm audit --omit=dev`: no production dependency vulnerabilities.
    9. Verified the installed CLI help exposes `coordinator-migrate`, ran the final Workers test pass (`4 passed`), and confirmed `git diff --check` is clean.
- **Status:** Implementation, migration path, management UI, tests, and operator documentation are complete. Deployment and live migration remain explicit operator actions because they require the user's Cloudflare account, final custom domain, secrets, and a coordinated pause of legacy workers.

## 2026-07-09 (Worker Conversion Isolation)
- **Objective:** Fix scheduled-abort worker mode after marker crashed natively with `corrupted double-linked list` and left an active processing lock behind.
- **Actions:**
    1. Investigated live queue state:
       - Job `f829c114cc2982b16472c22110b21f662cf32001d3a76f77582eb212c6fa7b98` remained in `PROCESSING` under worker `f8a500d06011`.
       - The active PDF was `Geist - The Sin-Eaters.pdf`.
    2. Updated worker runtime:
       - Added isolated marker conversion supervision via a child Python process.
       - Made `--abort-outside-window` automatically enable isolated conversion.
       - Added `--isolate-conversion` for crash containment without a run schedule.
       - Parent worker now keeps S3 lock ownership, enforces child timeout, and requeues on schedule boundary.
    3. Added `blobforge/conversion_child.py`:
       - Runs marker conversion in the child process.
       - Writes `content.md`, assets, marker metadata, and a small handoff result.
    4. Updated tests:
       - Covered successful isolated conversion handoff.
       - Covered schedule-boundary timeout killing the child and raising the requeue path.
    5. Updated documentation and tracking:
       - Updated `docs/worker_schedule.md`, `README.md`, `DESIGN.md`, `TODO.md`, and `AGENTS.md`.
- **Validation:**
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --no-project --with pytest python -m pytest tests/test_worker_runtime.py -q` -> `14 passed`.
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --no-project --with pytest --with xattr python -m pytest tests -q` -> `86 passed, 5 subtests passed`.
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --no-project --with ruff ruff check blobforge/worker.py blobforge/conversion_child.py tests/test_worker_runtime.py` -> passed.
    - Targeted Ruff including `blobforge/cli.py` is still blocked by pre-existing lint issues in `cli.py`.
- **Recovery:**
    - Network-approved `blobforge janitor --verbose` recovered stale processing lock `f829c114cc29...` at retry `1/3`.
    - Final queue check: `PROCESSING: 0`, `FAILED: 0`, `DEAD-LETTER: 0`, `TODO: 439`, `DONE: 1371`.
- **Status:** Implementation validated and stale failed-run lock recovered.

## 2026-03-27 (Raw Metadata Repair Command)
- **Objective:** Add an operator command to restore stripped raw-object metadata after S3 provider migration.
- **Actions:**
    1. Added metadata rewrite support to `blobforge/s3_client.py`:
       - Implemented same-key server-side copy with `MetadataDirective='REPLACE'`.
       - Preserved unrelated existing metadata keys such as `src_last_modified_millis`.
       - Preserved common object headers such as `ContentType` during metadata rewrite.
    2. Added CLI command `blobforge repair-metadata` in `blobforge/cli.py`:
       - Uses manifest entries as the source of truth.
       - Restores `original-name`, `tags`, and `size` onto `store/raw/<hash>.pdf`.
       - Defaults to filling only missing keys.
       - Supports `--force` to overwrite mismatched BlobForge metadata.
       - Supports `--dry-run` and optional per-hash targeting.
    3. Added test coverage in `tests/test_blobforge.py`:
       - Verifies metadata merge/preservation in the S3 client.
       - Verifies CLI repair behavior for missing metadata, forced overwrite, and dry-run.
    4. Updated documentation:
       - Added `docs/raw_metadata_repair.md`.
       - Updated `README.md` and `DESIGN.md` command examples.
- **Status:** Repair command implemented and documented.

## 2026-03-27 (Dashboard Filename Investigation)
- **Objective:** Determine why `blobforge dashboard` shows `unknown.pdf` for active jobs and verify whether data was lost.
- **Actions:**
    1. Inspected dashboard and worker code paths:
       - Confirmed `blobforge/status.py` renders `progress.original_filename`.
       - Confirmed `blobforge/worker.py` sets that field from raw-object metadata via `s3_meta.get("original-name", "unknown.pdf")`.
    2. Queried live S3 state for the two active jobs shown in the dashboard:
       - Verified both raw PDFs still exist in `store/raw/`.
       - Verified neither job is in `done`, `failed`, or `dead`.
       - Verified manifest entries still contain the original paths/filenames.
       - Verified raw object metadata for the live jobs contains only `src_last_modified_millis`, not `original-name`, `tags`, or `size`.
    3. Sampled additional raw objects and observed the same metadata shape (`src_last_modified_millis` only) in the sample.
    4. Ran `blobforge test-s3` against the configured Backblaze endpoint:
       - Confirmed custom metadata support works for new test writes.
- **Conclusion:** `unknown.pdf` in the dashboard is a display fallback caused by missing raw-object filename metadata, not by loss of the underlying PDF or manifest entry. The user later confirmed the operational cause: `rclone sync` was used during S3-provider migration and did not preserve BlobForge's custom metadata on the raw objects.

## 2026-02-03
- **Objective:** Add S3 namespacing support and establish agent protocols.
- **Actions:**
    - Modified `config.py` to include `S3_PREFIX`.
    - Updated `janitor.py`, `status.py`, and `ingestor.py` to use standardized config paths.
    - Verified prefixing logic with a dry-run test.
    - Created `AGENTS.md`, `TODO.md`, and `docs/WORK_LOG.md` to define repository standards for AI agents.
- **Status:** Namespacing complete. Agent environment initialized.

## 2026-02-03 (Design Review & Refactor)
- **Objective:** Address design document issues and implement architectural improvements.
- **Issues Identified:**
    1. Race condition in job acquisition (TOCTOU vulnerability)
    2. Duplicate S3Client implementations across 4 files
    3. Weak sharding strategy (16 shards with single char)
    4. No heartbeat mechanism (2-hour stale timeout too long)
    5. No retry limit or dead-letter queue
    6. Copy-paste errors in DESIGN.md
    7. Duplicate `Body=` parameter bug in worker.py
    8. Ingestor didn't check all queue states
    9. No CLI command to retry failed jobs
    10. Random worker IDs broke cross-restart cleanup
- **Actions:**
    - Created consolidated `s3_client.py` with unified S3Client class
    - Updated `config.py` with new settings: `S3_PREFIX_DEAD`, `MAX_RETRIES`, `HEARTBEAT_INTERVAL_SECONDS`, `STALE_TIMEOUT_MINUTES`, persistent worker ID generation
    - Rewrote `worker.py`:
        - Fixed race condition: todo marker kept until completion
        - Improved sharding: 2-char prefix (256 shards)
        - Added heartbeat thread (60s interval)
        - Added retry tracking and dead-letter queue support
    - Rewrote `ingestor.py`:
        - Now checks all queue states (done, processing, failed, dead, todo) before queueing
        - Added summary statistics
    - Rewrote `janitor.py`:
        - Uses heartbeat-based stale detection (15 min default)
        - Processes both stale processing jobs and failed queue
        - Respects MAX_RETRIES, moves to dead-letter when exceeded
    - Rewrote `status.py` to use consolidated S3Client
    - Rewrote `cli.py`:
        - Added `retry` command for failed/dead jobs
        - Added `dashboard` command
        - All commands use consolidated S3Client
    - Completely rewrote `DESIGN.md`:
        - Fixed copy-paste errors
        - Documented all new features
        - Added S3 provider compatibility section
        - Added state transition diagram
        - Added configuration reference
- **Status:** All changes complete. Code compiles successfully.

## 2026-02-03 (Quality Improvements)
- **Objective:** Address remaining issues from code review.
- **Actions:**
    - Added comprehensive unit test suite (`tests/test_blobforge.py`)
        - 35 tests covering S3 operations, locking, heartbeat, retry logic
        - Tests skip gracefully when botocore not available
    - Added logging framework:
        - Replaced `print()` with `logging` module in worker.py
        - Added `LOG_LEVEL` environment variable
        - Structured log format with timestamps
    - Made conversion timeout configurable:
        - Added `CONVERSION_TIMEOUT_SECONDS` to config.py
        - Updated worker to use configurable timeout
    - Fixed graceful shutdown:
        - `worker.shutdown()` now waits for heartbeat thread to finish
    - Updated README.md with complete documentation:
        - All CLI commands with examples
        - Full configuration reference
        - S3 provider compatibility matrix
        - State transition diagram
    - Fixed `datetime.utcnow()` deprecation warnings
- **Status:** All improvements complete. 35 tests pass.

## 2026-02-04 (Quality of Life Improvements)
- **Objective:** Implement comprehensive QoL improvements for monitoring and management.
- **Actions:**
    1. **Enhanced Heartbeat Metadata:**
       - Added CPU/RAM/disk usage tracking via psutil (optional dependency)
       - Track elapsed time, file size, original filename per job
       - System metrics included in heartbeat updates
    
    2. **Richer Status Dashboard:**
       - Redesigned `blobforge dashboard` with visual progress bars
       - Shows filename, elapsed time, stage, CPU/RAM for processing jobs
       - Added worker summary section with aggregate metrics
       - Visual indicators (emojis) for job status
    
    3. **Job Throughput Metrics:**
       - Workers now track: jobs_completed, jobs_failed, bytes_processed
       - Calculate avg_processing_time and jobs_per_hour
       - Metrics persisted in worker registry JSON
       - `blobforge workers` command shows aggregate metrics
    
    4. **Job Logs / Error Details:**
       - Errors saved to `registry/logs/{hash}/error.json`
       - Includes full traceback and context (stage, filename, etc.)
       - New `blobforge logs <hash>` command to view logs
    
    5. **New CLI Commands:**
       - `blobforge logs <hash>` - View error details and logs
       - `blobforge watch` - Auto-refreshing dashboard
       - `blobforge download <hash>` - Download completed results
       - `blobforge preview <hash>` - Preview markdown output
       - `blobforge retry-all` - Bulk retry failed/dead jobs
       - `blobforge clear-dead` - Purge dead-letter queue
       - `blobforge search-queue <query>` - Find jobs by filename
       - `blobforge cancel <hash>` - Cancel running job
    
    6. **Worker Management:**
       - Enhanced `blobforge workers` output with metrics display
       - Shows CPU/RAM, jobs completed, throughput rate
       - Aggregate statistics across all workers
    
    7. **Dependencies:**
       - Added psutil as optional dependency: `pip install blobforge[metrics]`
       - Added `[all]` extra for full install
    
- **Status:** All QoL improvements complete. CLI tested and working.
## 2026-02-05 (Progress Tracking & ETA Display)
- **Objective:** Add rich progress tracking for marker PDF conversion stages and ETA display.
- **Actions:**
    1. **tqdm Progress Interception:**
       - Implemented monkey-patch for `tqdm.tqdm` class to intercept progress bars
       - Captures stage name, current/total items, rate, elapsed time, and ETA
       - Rate-limited callbacks (2s interval) to avoid overhead
       - Progress stored in HeartbeatThread memory, included in scheduled heartbeats
       - Heartbeat frequency unchanged (follows `get_heartbeat_interval()` config)
    
    2. **Dashboard ETA Column:**
       - Added ETA column to processing jobs display
       - Shows estimated time remaining for current stage (e.g., "~45s", "~2m30s")
       - Stage column shows marker progress: "Recognizing Text: 5/12"
    
    3. **Containerfile Update:**
       - Changed to install `.[all]` to include psutil for system metrics
    
- **Status:** Progress tracking and ETA display complete. Tests passing.
    

    
## 2026-02-05 (File Hash Caching via Xattrs)
    
- **Objective:** Implement persistent file hash caching using filesystem extended attributes.
    
- **Actions:**
    
    1. **Specification Alignment:**
    
       - Reviewed `docs/file_hashing_via_xattrs.md` and Go reference implementation.
    
       - Implemented logic in `blobforge/utils.py` following the standard.
    
    2. **Xattr Cache Implementation:**
    
       - Added `get_cached_hash` and `set_cached_hash` with mtime validation.
    
       - Updated `compute_sha256_with_cache` to use atomicity checks (pre/post stat comparison).
    
       - Stored mtime as integer seconds string for cross-language compatibility.
    
    3. **Ingestor Optimization:**
    
       - Updated `blobforge/ingestor.py` to check xattr cache before computing hashes.
    
       - Optimized UI logging: "Computing hash..." is now only shown on cache miss.
    
       - Verified that redundant cache checks are safe and maintain correct logging.
    
    4. **Verification:**
    
       - Created `tests/test_xattr_hashing.py` to verify caching logic, mtime validation, and modification detection.
    
       - Created `tests/test_ingest_logging.py` to verify UI logging behavior.
    
       - Both tests passing with `uv run`.
    
- **Status:** Xattr caching implemented and verified. Ingestor performance significantly improved for large directories.

## 2026-02-08 (Worker Startup Recovery Retry Semantics)
- **Objective:** Prevent infinite crash/restart loops from reprocessing the same job with `retry=0`.
- **Actions:**
    1. Updated `blobforge/worker.py` startup recovery logic (`cleanup_previous_session`):
       - Recovered processing locks now increment retry count from lock metadata.
       - Recovered jobs are requeued with structured todo marker metadata (`retries`, `queued_at`, `recovered_from`).
       - Jobs exceeding retry budget during recovery are moved directly to dead-letter queue.
       - Added explicit cleanup of todo markers for dead-lettered recovered jobs.
    2. Added unit tests in `tests/test_worker_recovery.py`:
       - Verifies recovered jobs are requeued with incremented retries.
       - Verifies over-budget recovered jobs are dead-lettered and not requeued.
       - Verifies locks owned by other workers are ignored.
    3. Updated architecture documentation:
       - `DESIGN.md` section `4.2.1`
       - `docs/worker_startup_recovery.md` design note for startup recovery retry semantics
    4. Updated `TODO.md` completed items and added a repository finding in `AGENTS.md`.
- **Status:** Recovery logic now treats startup-recovered locks as failed attempts, closing the observed retry reset loop.

## 2026-02-08 (Signal-Aware Graceful Worker Shutdown)
- **Objective:** Ensure worker shutdown catches normal termination signals and requeues in-flight jobs immediately.
- **Actions:**
    1. Updated `blobforge/worker.py`:
       - Added catchable signal registration/restoration helpers for worker loops.
       - Added `run_worker_loop(...)` shared runtime with signal-aware graceful exit.
       - Added `_requeue_active_job(...)` used by shutdown to move active job back to todo and release lock.
       - Extended `shutdown(...)` with `requeue_current_job` behavior.
    2. Updated `blobforge/cli.py`:
       - `cmd_worker` now delegates to `worker.run_worker_loop(...)` so CLI worker path uses the same graceful signal handling.
    3. Added tests in `tests/test_worker_shutdown.py`:
       - Verifies shutdown requeues active job with preserved retries and recovery metadata.
       - Verifies no-op requeue when no current job is active.
       - Verifies loop interruption triggers shutdown with requeue intent.
    4. Added/updated documentation:
       - `docs/worker_graceful_shutdown.md`
       - `DESIGN.md` worker section (`4.2.7`) for signal-aware graceful shutdown semantics.
       - Updated `TODO.md` and `AGENTS.md` findings.
- **Status:** Worker shutdown now handles catchable signals and requeues active jobs without waiting for stale-lock recovery.

## 2026-02-10 (Worker Runtime Hardening Follow-Up)
- **Objective:** Address remaining worker robustness gaps after initial shutdown/recovery changes.
- **Actions:**
    1. `blobforge/worker.py` runtime/shutdown hardening:
       - Added `_safe_int()` utility for resilient numeric parsing from lock/marker metadata.
       - Updated startup recovery to reconcile retries using `max(lock_retries, todo_retries)` before incrementing.
       - Updated heartbeat thread stop behavior to use an event (`wait`) for prompt shutdown wake-up.
       - Reordered shutdown flow: stop heartbeat -> requeue active job -> join heartbeat -> deregister.
       - Kept custom signal handlers active until shutdown cleanup completes.
       - Added explicit handling for unexpected run loop exceptions (`exit_code=1`) while still forcing graceful cleanup/requeue.
    2. Conversion timeout enforcement:
       - Added `_run_conversion_with_timeout()` wrapper.
       - Wired `process()` conversion path to enforce `conversion_timeout` using `SIGALRM`/`ITIMER_REAL` when available.
       - Added fallback warnings for unsupported timer/signal environments.
    3. Test coverage updates:
       - Extended `tests/test_worker_shutdown.py` with shutdown ordering and handler-restore ordering checks.
       - Extended `tests/test_worker_recovery.py` with lock/todo retry reconciliation coverage.
       - Added `tests/test_worker_runtime.py`:
         - Timeout wrapper behavior tests.
         - Subprocess integration test sending real `SIGTERM` to verify graceful shutdown path invocation.
    4. Documentation updates:
       - Updated `DESIGN.md` worker sections (`4.2.1`, `4.2.4`, `4.2.7`).
       - Updated `docs/worker_startup_recovery.md` and `docs/worker_graceful_shutdown.md`.
       - Added `docs/worker_conversion_timeout.md`.
       - Updated `TODO.md` and `AGENTS.md` findings.
    5. Validation:
       - `uv run python -m pytest tests/test_worker_shutdown.py tests/test_worker_recovery.py tests/test_worker_runtime.py -q` -> `15 passed`.
       - `uv run python -m pytest tests/test_blobforge.py -q` -> `49 passed, 5 subtests passed`.
- **Status:** All identified follow-up issues from worker robustness review have been addressed and validated.

## 2026-07-08 (Worker Run Windows)
- **Objective:** Add a worker runtime option that limits CPU-intensive conversion work to configured local-time windows and can optionally abort/requeue active jobs when a window closes.
- **Actions:**
    1. Inspected worker polling, graceful shutdown, active-job requeue, and conversion-timeout paths.
    2. Implemented local run-window parsing and gating in `blobforge/worker.py`.
       - Supports repeated or comma-separated `HH:MM-HH:MM` windows.
       - Supports windows crossing midnight.
       - Prevents job acquisition outside configured windows.
    3. Added optional active conversion abort/requeue.
       - `--abort-outside-window` uses the existing signal timer path.
       - Window-boundary aborts raise a schedule-specific exception and requeue the active job with `recovered_from: schedule_window_closed`.
       - Default behavior still lets active jobs finish after a window closes.
    4. Wired CLI flags in both `blobforge worker` and standalone `blobforge.worker` parser.
    5. Added focused unit coverage for schedule parsing, run-loop acquisition gating, process schedule propagation, and schedule-derived conversion timeout.
    6. Added documentation:
       - `docs/worker_schedule.md`
       - README worker examples and behavior notes.
       - DESIGN worker lifecycle notes.
    7. Validation:
       - `uv run --no-project --with pytest python -m pytest tests/test_worker_runtime.py tests/test_worker_shutdown.py tests/test_worker_recovery.py -q` -> `23 passed`.
       - `uv run --no-project --with pytest --with xattr python -m pytest tests -q` -> `84 passed, 5 subtests passed`.
       - `uv run --no-project --with ruff ruff check blobforge/worker.py tests/test_worker_runtime.py` -> passed.
       - `uv run --no-project --with ruff ruff check .` -> failed on pre-existing unrelated lint issues across the repo; touched worker files pass targeted Ruff.
       - `uv run blobforge worker --help` -> confirmed `--run-window` and `--abort-outside-window` are registered.
    8. Refined outside-window idle behavior:
       - Worker now sleeps until the next configured opening window instead of waking on `idle_sleep`/short polling intervals.
       - Existing signal handling still interrupts the sleep for graceful shutdown.
       - Added regression coverage for a 10-hour outside-window sleep despite `idle_sleep=10`.
- **Status:** Worker run-window scheduling implemented, documented, and validated.

## 2026-06-25 (Failed Jobs Follow-Up Investigation)
- **Objective:** Inspect jobs that failed after the previous selective dead-letter retry, identify current failure causes, and recommend next handling.
- **Actions:**
    1. Started follow-up investigation and reviewed prior failed/dead-letter workflow notes.
    2. Queried live queue, worker registry, remote config, and object-level queue records.
       - Todo: 436 total (`3_normal`: 5, `4_low`: 431).
       - Processing locks: 1 actual lock (`0237641f74fd...`, worker `f8a500d06011`).
       - Failed queue: 1 job.
       - Dead-letter queue: 4 jobs.
       - Remote config: `max_retries: 3`, `conversion_timeout: 86400`.
    3. Noted that `atlantis` and `citadel` are retired hosts per user confirmation; their worker registry records are stale and should not be treated as live workers.
    4. Triaged current problem records:
       - Failed: `792ac29bd6b6...` (`Changeling The Lost - Core Book.pdf`, 126.7 MiB), `Conversion exceeded timeout (86400s)`, retries `1`.
       - Dead: `0857d1183713...` (`7910 - Rigger 3.pdf`, 104.4 MiB), `Worker restarted while job was processing`, retries `4`.
       - Dead: `3c7ccc748fb4...` (`Trinity Continuum Aberrant (Rasterized).pdf`, 92.9 MiB), `Exceeded max retries (4)`, retries `4`.
       - Dead: `a96530cb7011...` (`Cthulhu-Edition-7-Grundregelwerk-2017.pdf`, 48.0 MiB), `Worker restarted while job was processing`, retries `4`.
       - Dead: `f829c114cc29...` (`Geist - The Sin-Eaters.pdf`, 57.4 MiB), `Worker restarted while job was processing`, retries `4`.
       - Processing/stale: `0237641f74fd...` (`Cthulhu_7_Grundregelwerk.pdf`, 46.0 MiB), last heartbeat about 4h50m old, retries `2`.
    5. Ran `blobforge janitor --dry-run --verbose`.
       - Would restore stale `0237641f74fd...` to `3_normal` with retry `3/3`.
       - Would retry failed `792ac29bd6b6...` to `3_normal` with retry `2/3`.
       - Would not move any additional jobs to dead-letter.
    6. Checked available job logs.
       - Dead-letter jobs and stale processing lock had no structured error logs available.
       - Timeout job had structured error detail.
- **Status:** Investigation complete. Current failed/stale jobs are mostly resource/runtime failures, not PDFium data-format corruption. Suggested next operational step is to run janitor for the stale/failed queue, then decide separately whether to manually reset/requeue the four dead-letter jobs.

## 2026-06-25 (Failed/Dead/Stale Requeue)
- **Objective:** Requeue all current failed, dead-letter, and stale processing jobs after determining they were retry candidates.
- **Actions:**
    1. Ran `blobforge janitor --verbose`.
       - Restored stale processing lock `0237641f74fd...` to `3_normal` at retry `3/3`.
       - Retried failed timeout job `792ac29bd6b6...` to `3_normal` at retry `2/3`.
       - Moved no jobs to dead-letter.
    2. Ran `blobforge retry-all --dead --reset-retries --priority 3_normal`.
       - Requeued `0857d1183713...`.
       - Requeued `3c7ccc748fb4...`.
       - Requeued `a96530cb7011...`.
       - Requeued `f829c114cc29...`.
    3. Verified post-requeue state:
       - Failed queue: `0`.
       - Dead-letter queue: `0`.
       - Processing locks: `0`.
       - `3_normal`: `9` jobs.
       - `4_low`: `431` jobs.
- **Status:** All current failed/dead/stale jobs were requeued successfully.

## 2026-02-10 (README Documentation Sync)
- **Objective:** Update user-facing docs to reflect finalized worker shutdown and timeout semantics.
- **Actions:**
    1. Updated `README.md`:
       - Added key-feature bullets for graceful shutdown and conversion-timeout behavior.
       - Expanded worker section with explicit signal-handling/shutdown behavior.
       - Clarified janitor role as recovery path for crash/ungraceful-stop scenarios.
       - Updated `conversion_timeout` config description with platform caveats.
       - Added conversion-timeout notes describing hard-timeout support and fallback behavior.
    2. Updated repository tracking files:
       - Added completed documentation item in `TODO.md`.
       - Added findings entry in `AGENTS.md`.
- **Status:** README now matches current worker runtime behavior and operational expectations.

## 2026-02-26 (Hydrate Converted Outputs Feature)
- **Objective:** Implement a local hydration workflow that materializes completed conversion outputs next to source PDFs.
- **Actions:**
    1. Implemented new hydration component in `blobforge/hydrator.py`:
       - Added recursive PDF discovery for files/directories.
       - Reused xattr-aware SHA256 path (`compute_sha256_with_cache`) for hash resolution.
       - Added done-zip existence checks at `{prefix}store/done/<hash>.zip`.
       - Added local materialization to `<stem>.md` and `<stem>.assets/`.
       - Added per-run archive download deduplication for duplicate hashes.
       - Added markdown asset path rewriting (`assets/` -> `<stem>.assets/`) to prevent folder collisions.
       - Added staging/atomic write behavior for markdown and staged asset directory replacement.
       - Added `--dry-run` and `--force` support via function parameters.
    2. Wired CLI in `blobforge/cli.py`:
       - Added `cmd_hydrate(...)`.
       - Added `hydrate` subcommand with positional `paths` and flags `--force`, `--dry-run`.
    3. Added automated tests in `tests/test_hydrator.py`:
       - Hydrates markdown/assets from a mocked conversion archive.
       - Skips when local markdown exists and `--force` is not set.
       - Verifies one archive download is reused for multiple PDFs with identical hash.
    4. Added documentation:
       - New design note: `docs/hydrate_command.md`.
       - Updated `README.md` usage section with `blobforge hydrate` examples.
    5. Updated repository tracking/protocol files:
       - `TODO.md` completed item added.
       - `AGENTS.md` findings updated.
- **Tooling / Verification Commands:**
    - `rg`, `sed` used to inspect command surfaces and existing hash/download logic.
    - `uv run python -m pytest tests/test_hydrator.py -q` -> `3 passed`.
    - `uv run python -m pytest tests/test_blobforge.py -q` -> `49 passed, 5 subtests passed`.
    - `uv run python -m pytest tests -q` -> `69 passed, 5 subtests passed`.
    - `uv run blobforge --help` -> confirmed `hydrate` command registered.
    - `uv run blobforge hydrate --help` -> confirmed `paths`, `--force`, `--dry-run`.
- **Status:** Feature implemented, documented, and validated.

## 2026-06-09 (Failed Queue Investigation)
- **Objective:** Inspect the current failed PDF queue, identify likely failure causes, and determine whether to retry jobs on this machine.
- **Actions:**
    1. Reviewed CLI/S3 queue support for failed jobs, retries, janitor recovery, and job logs.
    2. Confirmed `search-queue` does not currently include failed/dead queues, so failed-job triage will use direct failed-queue listing and `blobforge logs`.
    3. Queried live queue state with `uv run blobforge list --verbose`:
       - Todo: 411 jobs.
       - Processing: 2 jobs.
       - Failed queue: 0 jobs.
       - Dead-letter queue: 40 jobs.
    4. Queried remote config and worker state:
       - `max_retries: 3`; dead-letter entries are at retry count 4.
       - Active workers: `atlantis` and `citadel`; `atlantis` was already memory-saturated while processing a large rasterized PDF.
    5. Triaged dead-letter records plus structured error logs:
       - 34 jobs: `Worker restarted while job was processing`.
       - 4 jobs: `A process in the process pool was terminated abruptly while the future was running or pending.`
       - 2 jobs: `Failed to load document (PDFium: Data format error).`
    6. Installed optional local conversion dependencies with `uv pip install -e ".[convert,metrics]"`.
    7. Downloaded the smallest restart-failed PDF (`1f71f4699dbe...`, 19.8 MiB) to `/tmp` and ran a local offline conversion probe.
       - Marker model downloads completed successfully.
       - The PDF loaded and began layout recognition, reaching `8/184` batches before the probe was stopped.
       - Local memory pressure rose substantially during the probe, then returned to normal after termination.
- **Status:** The live failed queue is empty; the real backlog is 40 dead-letter jobs. Most look like worker restart/resource casualties rather than corrupt PDFs. Do not bulk retry. Prefer one managed retry at a time, starting with a small restart-failed job, using `blobforge retry <hash> --priority 1_critical --reset-retries` followed by `blobforge worker --run-once` on a machine with enough RAM.

## 2026-06-09 (Selective Dead-Letter Requeue)
- **Objective:** Requeue all dead-letter jobs except PDFs that failed with `PDFium: Data format error`.
- **Actions:**
    1. Ran a dry-run S3 scan over `queue/dead/` and structured error logs.
       - Selected for requeue: 38 jobs.
       - Skipped: 2 jobs with `Failed to load document (PDFium: Data format error).`
    2. Requeued the selected 38 jobs to `queue/todo/3_normal/` with retry counters reset to `0`.
       - Todo marker `recovered_from`: `manual_bulk_retry_dead_excluding_pdfium`.
       - Preserved each prior error in marker metadata as `previous_error`.
    3. Removed the corresponding 38 dead-letter markers.
    4. Verified queue state after the update:
       - `3_normal`: 39 jobs.
       - `dead-letter`: 2 jobs.
       - Remaining dead-letter hashes are `3f094b24b162...` and `5be2a0426593...`, both PDFium data-format failures.
- **Status:** Selective requeue completed successfully.

## 2026-06-09 (Broken PDF Removal)
- **Objective:** Remove the two PDFs that failed with `PDFium: Data format error`.
- **Actions:**
    1. Ran dry-run removals for both broken PDF hashes:
       - `3f094b24b162ccb468c5f941eeb439d8ba5c9c59834ad6cdc16e9b6614f4e4b4` (`4th Edition/Shadowrun 4E - Mil Spec Tech.pdf`)
       - `5be2a04265933289f7ec557f66732e37f9049811296db2deb86867e84279ed94` (`Scion 1st/Scion - Seeds of Tomorrow.pdf`)
    2. Applied `blobforge remove` for both jobs.
       - Removed dead-letter markers.
       - Removed raw PDF objects.
       - Removed manifest entries.
       - Removed one error log per job.
    3. Verified queue and manifest state:
       - Dead-letter queue: `0`.
       - Both hashes are no longer present in the manifest.
- **Status:** Broken PDF jobs removed successfully.

## 2026-02-26 (Hydrate Preflight Performance Optimization)
- **Objective:** Reduce per-file remote checks during hydration by precomputing local/remote hash sets.
- **Actions:**
    1. Updated `blobforge/hydrator.py`:
       - Added local preflight pass to compute hashes once for all hydration candidates.
       - Added manifest prefilter (`get_manifest`) to eliminate remote checks for hashes not present in manifest.
       - Added done-availability resolver keyed by unique hashes.
       - Added bulk done-index path for large runs (`DONE_INDEX_THRESHOLD`) with helper fallback paths.
    2. Updated `blobforge/s3_client.py`:
       - Added `list_done_hashes()` to paginate done objects and return parsed `<hash>.zip` identifiers.
    3. Updated tests:
       - `tests/test_hydrator.py`: added manifest-prefilter test asserting reduced `exists` calls.
       - `tests/test_blobforge.py`: added `list_done_hashes` parsing test.
    4. Updated docs/tracking:
       - Updated `docs/hydrate_command.md` with preflight and remote-check strategy.
       - Updated `TODO.md` and `AGENTS.md` findings.
- **Validation:**
    - `uv run python -m pytest tests/test_hydrator.py tests/test_blobforge.py -q` -> `54 passed, 5 subtests passed`.
    - `uv run python -m pytest tests -q` -> `71 passed, 5 subtests passed`.
- **Status:** Optimization implemented and validated.

## 2026-04-28 (Dashboard Performance Investigation)
- **Objective:** Investigate and fix `blobforge dashboard` slowness.
- **Actions:**
    1. Analyzed `blobforge/status.py` and `blobforge/s3_client.py` to identify I/O bottlenecks:
       - `status.py` made ~9 sequential `count_prefix` S3 LIST calls (5 todo priorities + done/failed/dead).
       - `s3_client.py` `scan_processing_detailed()` listed processing jobs then did a sequential `get_object_json` for each active job (N+1 query problem).
       - `list_workers()` fetched each worker JSON sequentially.
    2. Parallelized dashboard data fetching in `status.py`:
       - Used `concurrent.futures.ThreadPoolExecutor(max_workers=8)` to run all `count_prefix` calls and `scan_processing_detailed` concurrently.
       - In verbose mode, `get_active_workers()` also runs in the same executor pool.
    3. Parallelized per-job lock fetching in `s3_client.py`:
       - Extracted `_scan_processing_job()` helper.
       - `scan_processing_detailed()` now uses `ThreadPoolExecutor(max_workers=16)` to fetch all processing lock contents in parallel.
    4. Parallelized worker listing in `s3_client.py`:
       - `list_workers()` now uses `ThreadPoolExecutor(max_workers=8)` to fetch worker metadata in parallel.
    5. Added `limit` parameter to `S3Client.count_prefix()` for future capping of huge prefixes.
    6. Updated `AGENTS.md` findings with root causes and fix summary.
- **Validation:**
    - `uv run python -m pytest tests/ -q` -> `76 passed, 5 subtests passed`.
    - Module imports verified for `blobforge.status` and `blobforge.s3_client`.
- **Status:** Dashboard I/O parallelized. Expected speedup is roughly the number of independent S3 calls (e.g., ~5-8x faster depending on queue sizes and active job counts).
## 2026-07-16 (Coordinator Backup and Legacy-State Retirement)

- **Objective:** Add administrator-triggered Bunny Database backups, remove the retired manifest/log/Telegram stack, and safely clean obsolete S3 coordination objects.
- **Actions:**
    1. Reviewed the current Bunny Edge Script, libSQL schema, object-store signer, Python coordinator paths, CLI surface, tests, and documentation; verified Bunny's documented snapshot behavior and transactional batch semantics.
    2. Added `CoordinatorDatabase.exportBackup()`, which reads the application schema and all active tables in one read transaction and serializes bigint values safely.
    3. Added authenticated `POST /api/v1/admin/backups`, an IndieAuth management-console button, SHA-256/size/row-count reporting, audit recording, and private S3 uploads under `{prefix}backups/coordinator/`.
    4. Removed the S3 manifest implementation and CLI commands, registry job-log implementation and CLI viewer, Telegram bot/package extra, one-time coordinator migration module/API, metadata-repair documentation/tests, and hydrate manifest prefilter.
    5. Made ingestion, workers, dashboards, worker listing, and runtime config coordinator-authoritative; conversion workers continue to use only enrollment tokens and signed object URLs.
    6. Added `S3Client.purge_prefix()` with paginator and 1,000-object delete batching plus `blobforge cleanup-legacy`, which previews by default and deletes only `{prefix}queue/` and `{prefix}registry/` after explicit confirmation.
    7. Regenerated `uv.lock`, updated architecture/user documentation, TODO tracking, and repository findings, and ran whitespace/reference audits.
- **Tooling:** Used `rg`/`sed` for repository discovery and mechanical removal, `apply_patch` for implementation edits, `uv lock` for dependency resolution, Vitest/esbuild for Bunny validation, and pytest through `uv` for Python validation.
- **Validation:**
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --offline --no-sync python -m pytest tests/ -q` -> `92 passed, 5 subtests passed` (one pre-existing `datetime.utcnow` deprecation warning).
    - `cd bunny && npm test -- --run` -> `10 passed`.
    - `cd bunny && npm run build` -> successful Edge Script bundle (`208.9kb`).
    - `git diff --check` -> clean.
- **Status:** Implemented and validated; ready to commit and push.

## 2026-07-16 (Web File Library and Browser Ingestion)

- **Objective:** Make the Bunny management console useful as a complete file library, including PDF ingestion, source/result downloads, rendered output previews, and discovery of completed jobs.
- **Implementation:**
    1. Added a transactional, paginated database query with status/priority filters and case-insensitive search across hash, filename, source, paths, and tags.
    2. Added authenticated admin APIs for upload preparation/finalization and raw/output downloads. Upload finalization verifies raw-object existence before enqueue.
    3. Added exact-key raw PUT and output GET signing plus raw-object HEAD checks to the coordinator object-store boundary.
    4. Rebuilt the jobs panel as a paginated library, so done jobs are queried directly instead of falling behind the operational snapshot's 250-row limit.
    5. Added multi-PDF upload with PDF signature validation, browser SHA-256, direct S3 transfer, and coordinator enqueue; large file bodies never pass through the Edge Script.
    6. Added PDF/ZIP download actions and a dependency-free client-side ZIP/Markdown viewer with archive assets, escaped raw HTML, blob URL cleanup, and expanded-archive limits.
    7. Documented the required exact-origin object-store CORS policy, preview compatibility boundaries, architecture, task status, and repository findings.
- **Validation:**
    - `cd bunny && npm run check` -> passed.
    - `cd bunny && npm test -- --run` -> `11 passed`.
    - `cd bunny && npm run build` -> successful Edge Script bundle (`225.5kb`).
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --offline --no-sync python -m pytest tests/ -q` -> `92 passed, 5 subtests passed` (one pre-existing `datetime.utcnow` deprecation warning).
    - `git diff --check` and source TODO/debug-marker scans -> clean.
- **Status:** Implemented, documented, and validated.

## 2026-07-16 (Maintained Markdown Renderer and Persistent ToC)

- **Objective:** Replace the preview's handwritten Markdown subset with a maintained library and keep document navigation available while reading large results.
- **Research/decision:** Compared Marked and markdown-it using their official documentation. Selected Marked for its focused GFM browser compiler and paired it with DOMPurify, which Marked explicitly recommends because parser output is not sanitized. Avoided a separate ToC plugin: deriving navigation from the sanitized heading DOM produces the exact rendered outline, handles duplicate headings, and keeps the security boundary simpler.
- **Implementation:**
    1. Added pinned `marked` and `dompurify` runtime dependencies plus jsdom-based security/render tests.
    2. Added a reproducible esbuild generation step that embeds the self-hosted browser bundle; no third-party CDN or relaxed script CSP is required.
    3. Removed the handwritten regex renderer. GFM output and raw HTML are sanitized before DOM insertion; links and archive images are constrained after sanitization.
    4. Added stable duplicate-safe heading anchors and a live ToC with active-section highlighting.
    5. Made the ToC a persistent left sidebar inside the reader on desktop and a collapsible drawer on narrow screens.
- **Validation:**
    - `cd bunny && npm run check` -> generated renderer bundle and TypeScript passed.
    - `cd bunny && npm test -- --run` -> `12 passed`, including GFM, XSS sanitization, asset resolution, duplicate heading IDs, UI bundle syntax, and persistent/collapsible ToC markup.
    - `cd bunny && npm run build` -> successful Edge Script bundle (`298.2kb`).
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --offline --no-sync python -m pytest tests/ -q` -> `92 passed, 5 subtests passed` (one pre-existing `datetime.utcnow` deprecation warning).
    - `git diff --check` and handwritten-renderer reference scan -> clean.
- **Status:** Implemented, documented, and validated.

## 2026-07-17 (Live Progress and Failure Diagnostics)

- **Objective:** Make coordinator progress reports timely and useful, and make failed jobs explain why each attempt failed.
- **Findings:** Progress changes were only sent on the normal heartbeat timer; isolated or quiet conversion phases therefore appeared frozen. The worker already sent traceback/context on failure, but the coordinator discarded both, cleared the last progress snapshot, and retained only one short error string.
- **Implementation:**
    1. Added explicit macro-stage percentages from claim through upload and made stage/converter updates wake a coalescing heartbeat publisher (maximum one send per two seconds).
    2. Added atomic child-to-parent progress checkpoints for isolated conversions, covering model loading, conversion, content extraction, and output writing.
    3. Preserved the original job start time across metadata updates so elapsed time covers the full download-to-upload attempt.
    4. Converted packaging and upload exceptions into normal structured job failures instead of letting them escape into worker-loop recovery.
    5. Added append-only `job_failures` storage for every worker failure and expired lease, retaining attempt, worker, traceback, context, and last progress/system snapshot.
    6. Added latest-failure context to file listings, a protected failure-history API, and a Web UI viewer with expandable diagnostics and tracebacks.
    7. Added progress bars, stage/counter/ETA details, concise failure summaries, and ten-second live refreshes to the management console.
    8. Included failure history in portable coordinator backups and documented the data flow and retention behavior.
- **Validation:**
    - `cd bunny && npm run check` -> passed.
    - `cd bunny && npm test -- --run` -> `13 passed`.
    - `cd bunny && npm run build` -> successful Edge Script bundle (`306.1kb`).
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --offline --no-sync python -m pytest tests/ -q` -> `93 passed, 5 subtests passed` (one pre-existing `datetime.utcnow` deprecation warning).
    - `git diff --check` and final status/diff review -> clean.
- **Status:** Implemented, documented, and validated.

## 2026-07-17 (Deterministic Worker Enrollment IDs)

- **Objective:** Remove random worker-ID suffixes and keep reusable provisioning credentials separate from ordinary worker enrollment.
- **Implementation:**
    1. Extracted deterministic label slugging and now use the slug directly as the worker ID.
    2. Changed credential creation to an atomic insert-or-reject operation and return HTTP 409 for duplicate or slug-colliding labels, including revoked IDs.
    3. Updated enrollment UI copy to preview the slug behavior and state that the one-time token belongs to one worker.
    4. Added API coverage for stable slugs and duplicate rejection.
    5. Documented a future dynamic-registration-token flow as the separate mechanism for reusable bootstrap credentials and incremented identities.
- **Validation:**
    - `cd bunny && npm run check` -> passed.
    - `cd bunny && npm test -- --run` -> `13 passed`, including deterministic slug and duplicate-enrollment coverage.
    - `cd bunny && npm run build` -> successful Edge Script bundle (`306.5kb`).
    - `git diff --check` and final working-tree review -> clean.
- **Status:** Implemented, documented, and validated.

## 2026-07-17 (Dependabot Remediation)

- **Objective:** Resolve the 33 open GitHub Dependabot alerts without breaking PDF conversion or hiding upstream compatibility constraints.
- **Patch contract:** Untrusted PDFs must not reach vulnerable parsing/decompression code in locked dependencies; every supported universal-lock resolution must meet patched floors. Existing S3/coordinator behavior and the Marker PDF-to-Markdown conversion path must remain functional.
- **Findings:**
    1. Python 3.9 forced `marker-pdf 0.2.17` and multiple vulnerable legacy resolutions, so complete remediation requires Python 3.10+.
    2. Current Marker/Surya cap Pillow below 11 even though the applicable fixes require 12.2; the cap is stale for the exercised conversion path.
    3. A trial Transformers 5 override failed the real Marker import boundary because Surya uses removed private APIs. The three Transformer advisories affect untrusted model loading or `Trainer`, neither reachable from submitted PDFs.
    4. The remaining Torch advisory affects unused `torch.jit.script` and has no recorded patched version. Torch 2.10 fixes the two reachable sequence/memory advisories while preserving the CUDA 12 fleet.
- **Implementation:**
    1. Raised `requires-python` to 3.10 and removed the Python 3.9 classifier.
    2. Added centralized uv security floors for every applicable advisory and an explicit Pillow override.
    3. Regenerated `uv.lock`, removing all vulnerable versions for applicable alerts.
    4. Added a regression test that parses every universal-lock package resolution and enforces the security floors.
    5. Added `docs/dependency_security.md` with override rationale, trust-boundary analysis, and conditions that invalidate the four non-applicability decisions.
- **Compatibility evidence:** Marker imports passed with Marker 1.10.2, Surya 0.17.1, Pillow 12.3, Transformers 4.57.6, Torch 2.10.0, cryptography 49.0.0, and urllib3 2.7.0. A complete conversion of `assets/lorem.pdf` passed in 148.9 seconds under CPU fallback and produced Markdown successfully.
- **Validation:**
    - `UV_CACHE_DIR=/tmp/uv-cache uv lock --check` -> passed (`118 packages`).
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --no-sync python -m pytest tests/test_dependency_security.py -q` -> `2 passed`; every universal-lock resolution meets its applicable advisory floor.
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --no-sync python -m pytest tests/ -q` -> `95 passed, 5 subtests passed` (one pre-existing `datetime.utcnow` deprecation warning).
    - `cd bunny && npm run check` -> generated Markdown runtime and TypeScript checks passed.
    - `cd bunny && npm test -- --run` -> `13 passed`.
    - `cd bunny && npm run build` -> successful Edge Script bundle (`306.5kb`).
    - `git diff --check` -> clean.
- **Status:** All 29 applicable alerts are remediated in the local universal lock. The three non-applicable Transformers alerts and one unpatched, unused TorchScript alert are documented for dismissal after the change reaches the default branch.
## 2026-08-21 (Inclusive Change-Range Review)

- **Objective:** Review all changes from commit `7ff1c5f3bc01f5eb0382278c7f4f0c481b44d335` (inclusive) through the current working tree for correctness, regressions, edge cases, and test gaps.
- **Progress:**
    1. Confirmed the inclusive base as `7ff1c5f^` (`c9a5a991...`) and enumerated 17 commits through `91c2d24`.
    2. Verified the starting worktree was clean and scoped 35 changed files (2,147 insertions, 550 deletions).
    3. Read the repository task-management, work-log, and findings protocols and registered this review in `TODO.md`.
    4. Traced the Python coordinator client, ingestion, hydration, persistent index, worker/S3 fallback, CLI, Bunny API/database migrations, management UI, and their tests.
    5. Reproduced three uncovered failures with focused in-memory/mock probes: two consecutive hydration runs recomputed the hash and left `file_hashes` empty; `ingest --dry-run` called `upload_raw`; and a pre-existing raw object with no coordinator job prevented `enqueue` from being called.
    6. Identified the unscoped done-set mirror/watermark by tracing the single global index path across coordinator URL changes.
    7. Completed the review, recorded significant findings in `AGENTS.md`, and marked the review task complete in `TODO.md`.
- **Tooling:** Used `git status`/`log`/`diff`/`show`, `rg`, `sed`, and `nl` for code/history tracing; `apply_patch` for required repository records; pytest through `uv`; Vitest, TypeScript, and esbuild through npm; Ruff for a supplemental static pass; and focused Python mock probes for uncovered control-flow paths.
- **Validation:**
    - `UV_CACHE_DIR=/tmp/uv-cache uv run --no-project --with pytest --with xattr --with packaging --with boto3 python -m pytest tests/` -> `126 passed`.
    - `cd bunny && npm test -- --run` -> `22 passed`.
    - `cd bunny && npm run check` -> passed.
    - `cd bunny && npm run build` -> passed (`dist/index.js`, 343.2 kb).
    - `ruff check --select F821 blobforge bunny/src` -> passed; the unrestricted legacy lint baseline reports many pre-existing style findings.
    - `git diff --check c9a5a991...` -> one trailing-space error in `blobforge/config.py:217`.
- **Status:** Review complete with four reportable correctness findings; no product code was modified.
## 2026-08-21 (Missing Worker Conversion Dependency Diagnosis)

- **Objective:** Diagnose a native worker that rapidly failed multiple jobs
  with `ModuleNotFoundError: No module named 'marker'` while isolated conversion
  was enabled by `--abort-outside-window`.
- **Findings:**
    1. Confirmed the active `.venv` resolves no `marker` module; Marker is only
       present in the optional `convert`/`all` dependency groups.
    2. `Worker.__init__()` authenticates, registers, and starts heartbeats
       without validating conversion capability. The polling loop can therefore
       acquire a lease on a base-only CLI installation.
    3. Isolated conversion imports Marker only inside the child after a job is
       claimed. `conversion_child.main()` maps every exception to exit code 1;
       the parent wraps that as `RuntimeError`, and `process()` routes it to
       `_handle_failure()` as though the PDF were defective. Non-isolated mode
       also defers `_init_marker()` until after acquisition and has the same
       failure-classification problem.
    4. The later `Lease is no longer valid` heartbeat is a secondary race: the
       heartbeat thread can retain an old job snapshot across its two-second
       prompt-update coalescing wait and publish it after `fail()` closed the
       lease. It does not cause the conversion failures.
    5. Git history shows the missing startup preflight predates the reviewed
       `7ff1c5f...` range; the August changes only touched isolated child process
       group creation in this path.
- **Tooling:** Used `rg`, `sed`, `nl`, `git blame`, `git log`, and an import-spec
  probe through `uv run --no-sync` to trace runtime, packaging, documentation,
  and history. Consulted the current uv documentation endpoint for optional
  dependency invocation; no external state was changed.
- **Status:** Root cause confirmed. No product code or coordinator state was
  changed; the repository-required TODO/work-log/findings records were updated.

## 2026-08-21 (Review Remediation and Final Verification)

- **Objective:** Fix the four findings from the inclusive `7ff1c5f3...` review
  plus the observed missing-Marker worker failure loop, commit the work
  atomically, and perform a final code review.
- **Implementation:**
    1. Made ingestion stop before raw PUT/enqueue in dry-run mode, recover an
       orphaned raw upload without requiring existing job metadata, and reuse
       the checked transfer for immediate local uploads (`66d1c73`).
    2. Persisted hydration hash misses in the SQLite index; added coordinator
       scope to done rows and version-3 watermarks; migrated legacy unscoped
       mirrors by clearing only ambiguous done data (`29aef30`).
    3. Added a shared conversion-runtime validator and configuration error;
       workers now validate before coordinator contact, isolated children use
       a distinct configuration exit code, late failures release rather than
       fail the lease, and prompt heartbeats revalidate the lease after their
       coalescing delay (`48c0999`).
    4. During the final diff review, noticed that Git LFS materialization could
       outlive the initially checked signed URL. LFS uploads now request a fresh
       transfer after materialization, and a pointer-cleanup failure no longer
       blocks enqueue after a successful upload (`1a032a4`).
- **Regression coverage:** Added tests for dry-run side effects, orphaned
  regular/LFS uploads, transfer reuse/refresh, hash persistence across runs,
  legacy done-mirror migration, coordinator isolation, worker preflight
  ordering, child error classification, no-retry release, and the stale
  heartbeat race.
- **Tooling:** Used `sed`, `rg`, `git log/diff/status`, and focused source/test
  inspection; applied all edits with `apply_patch`; ran tests and Ruff through
  `uv`; ran the Bunny Vitest/TypeScript/esbuild checks through npm; staged
  explicit file sets and created atomic Git commits. A sandboxed first attempt
  to resolve a temporary xattr test environment could not reach PyPI, so the
  approved uv test environment was used. The first long full-suite invocations
  yielded before pytest termination; the runner was corrected to retain and
  poll the session through its exit code.
- **Validation:**
    - Focused ingestion suite -> `22 passed`.
    - Focused hydration/index suite -> `24 passed`.
    - Focused worker/child suite -> `25 passed`; broader worker/recovery/CLI
      checks also passed.
    - Complete Python suite -> `141 passed, 5 subtests passed`.
    - `cd bunny && npm run check` -> passed.
    - `cd bunny && npm test -- --run` -> `22 passed`.
    - `cd bunny && npm run build` -> passed (`dist/index.js`, 343.2 kb).
    - Ruff `F821` pass across `blobforge` and `tests` -> passed.
    - `git diff --check 91c2d24..HEAD` -> passed before the records-only commit.
- **Final review:** Re-read the complete remediation diff after the tests. The
  LFS URL lifetime issue found during that pass was fixed and revalidated; no
  remaining reportable correctness issue was found.
- **Status:** All five requested findings are fixed, documented, tested, and
  committed atomically; final repository records are ready to commit.

## 2026-08-21 (Surya llama-server Failure Diagnosis)

- **Objective:** Explain why an isolated conversion reached Surya OCR and
  failed after several minutes with `llama-server binary not found`.
- **Findings:**
    1. Traced the exception through Marker's OCR builder into Surya's full-page
       recognition path and `SuryaInferenceManager`.
    2. The installed Surya uses a vision-language OCR model
       (`datalab-to/surya-ocr-2`) behind an OpenAI-compatible inference server.
       Its automatic backend policy is NVIDIA -> vLLM, otherwise -> llama.cpp.
    3. This host exposes neither CUDA, `/dev/nvidia0`, nor `nvidia-smi`, so the
       policy selected `llamacpp`; no `llama-server` executable or external
       `SURYA_INFERENCE_URL` is configured.
    4. The `.venv` contains Marker 2.0.0 / Surya 0.22.1, but `uv.lock` pins the
       previously conversion-tested Marker 1.10.2 / Surya 0.17.1. A dry-run of
       `uv sync --extra convert` would restore those locked versions.
    5. The permissive optional dependency plus `uv pip install -e
       ".[convert]"` can resolve the newest major release without consulting
       the project lock; a later `uv run blobforge` without `--extra convert`
       does not necessarily reconcile those optional packages.
    6. The recently added worker preflight behaved as implemented: all Python
       imports and expected symbols existed. The missing dependency is an
       external executable required only when the OCR backend starts.
- **Tooling:** Used `git status/log/blame`, `rg`, `find`, `sed`, installed
  package metadata, Surya/Marker source inspection, `uv run --no-sync`, and
  `uv sync --dry-run` (with the cache redirected to `/tmp`) to inspect versions,
  settings, backend detection, device visibility, and lock reconciliation. An
  initial uv metadata probe could not create a cache temporary file under the
  filesystem sandbox; it was repeated successfully with `UV_CACHE_DIR`.
- **Changes:** Updated only the required TODO, findings, and activity records;
  no packages, runtime code, worker process, lease, or coordinator state were
  changed.
- **Status:** Root cause confirmed. The immediate repository-aligned recovery
  is to sync and run with the locked `convert` extra; supporting Marker 2.x
  would instead require explicitly provisioning llama.cpp/vLLM or an external
  inference server and extending startup validation.

## 2026-08-21 (Conversion Runtime Compatibility Guard)

- **Objective:** Prevent another silent Marker major-version drift, explain the
  output-compatibility impact, and fail before coordinator contact when a
  newer Surya runtime lacks its external inference dependency.
- **Changes:**
    1. Constrained the public `convert` and `all` extras to
       `marker-pdf>=1.10.2,<2` and refreshed only BlobForge's corresponding
       requirement metadata in `uv.lock`.
    2. Extended conversion-host startup validation to recognize Surya's newer
       inference manager. External URLs require no local executable; the
       llama.cpp backend requires `llama-server` or `LLAMA_CPP_BINARY`; and the
       vLLM backend requires Docker. Errors include actionable setup/recovery
       guidance and occur through the existing pre-coordinator worker guard.
    3. Added unit coverage for legacy Surya, local llama.cpp, external-server,
       vLLM, and broken-import paths, plus dependency-bound regression checks.
    4. Documented the Marker 1/2 architecture and output compatibility boundary,
       deployment responsibility, and criteria for a future Marker 2 adoption.
    5. Restored the checkout environment with `uv sync --extra convert`; the
       active runtime is now Marker 1.10.2 / Surya 0.17.1, and its real startup
       preflight passes.
- **Tooling:** Used `apply_patch`, `uv lock`, `uv sync`, focused and full pytest
  runs, Ruff on changed Python files, direct conversion-preflight probes against
  both the drifted and restored environments, `git diff --check`, and targeted
  diff/status inspection. The first offline lock refresh lacked cached Marker
  metadata and the sandboxed online attempt lacked DNS, so the lock query and
  environment sync were repeated with approved network access. A full-tree Ruff
  run reported 424 pre-existing findings; all changed Python files pass Ruff.
- **Verification:** 147 tests and 5 subtests passed; changed-file Ruff passed;
  `uv sync --locked --extra convert --dry-run` reports no changes; the drifted
  Marker 2 environment produced the new missing-`llama-server` error before
  coordinator contact, and the restored Marker 1 runtime passes preflight.
- **Status:** Complete. Production installs remain on the output-compatible
  Marker 1.x line, and accidental newer-runtime drift now fails fast with clear
  host-configuration guidance instead of consuming job retries.

## 2026-08-21 (Recipe-aware Conversion Provenance)

- **Objective:** Separate source-document identity from conversion-recipe
  identity, retain exact runtime provenance, and allow future Marker generations
  to coexist without silently reusing or overwriting incompatible artifacts.
- **Progress:** Started repository mapping. Used status and targeted ripgrep
  searches across Python workers/clients, Bunny routing/database/object storage,
  tests, and architecture documentation to locate current hash-only job and
  `done/<hash>.zip` contracts. Added the implementation task to `TODO.md`.
- **Architecture:** Kept source-document jobs addressable by PDF hash for API and
  hydration compatibility, while binding each claimed job to a canonical recipe
  digest and storing completed artifacts under `(file_hash, recipe_digest)`.
  Exact runtime data is provenance rather than cache identity. Recipe schema 1
  includes the converter generation, output schema, configured model/checkpoint
  identifiers, and output-affecting options. New object keys are
  `store/out/<hash>/<recipe>.zip`; the all-zero digest is reserved for legacy
  hash-only objects.
- **Implementation:**
    1. Added cross-language canonical recipe hashing, exact package/build/model/
       platform/backend provenance, and archive/worker registration metadata.
       External inference URLs are reduced to a boolean to avoid persisting
       internal endpoints or credentials.
    2. Added nullable `jobs.recipe_digest` migration and the
       `conversion_artifacts` table. Claims require a canonical recipe, bind
       previously unbound work atomically, and exclude incompatible workers.
       Completion verifies the lease recipe, canonical body, and provenance
       digest before recording the artifact.
    3. Added recipe-scoped object PUT/HEAD/GET keys, ambiguity recovery for an
       existing recipe artifact, explicit artifact listing/download selection,
       and an authorized conversion-request endpoint. Selecting an existing
       artifact avoids recomputation and advances the done watermark.
    4. Preserved old `store/out/<hash>.zip` objects with a reserved legacy
       recipe. The coordinator exposes them before backfill and persists their
       artifact rows before the first retarget, so later recipe selection cannot
       hide or overwrite them.
    5. Bumped portable coordinator backups to format version 2 and documented
       schema, API, deployment compatibility, and the future Marker A/B flow.
- **Verification:** Python focused tests passed (48); the full Python suite
  passed (155 tests and 5 subtests) with one existing Surya/Pydantic deprecation
  warning. Bunny TypeScript checking, the production bundle build, and all 25
  coordinator/object-store/runtime tests passed. Cross-language Unicode/nested
  recipe hashing has a fixed parity test. Changed recipe Python files pass
  Ruff, `git diff --check` passes, and locked `uv sync` reports no changes.
- **Tooling:** Used `rg`, `sed`, Git status/diff inspection, `apply_patch`,
  `uv sync`, `uv run pytest`, Ruff, npm TypeScript checking/tests/build, and
  direct recipe/provenance probes. Network access was approved only to populate
  missing uv test/lint caches. During implementation, tests exposed and drove
  fixes for a TypeScript name-shadowing error, backup-version expectation, and
  the legacy-artifact retargeting edge case. The final audit also identified
  Python/JavaScript fractional-number serialization as a potential future
  digest divergence; recipes now reject fractional/unsafe numbers and require
  those settings to use strings. It also changed unsupported worker recipe
  values from a generic server error to a specific HTTP 400 response. A broad
  Ruff check over legacy modified modules reported pre-existing style findings;
  focused Ruff checks for the new recipe module and tests pass.
- **Status:** Complete. Final diff review found no remaining correctness issue;
  the recipe-aware artifact feature and its regression coverage are ready as
  one atomic change. Committed as `f34c3c2` (`feat: track recipe-aware
  conversion artifacts`).

## 2026-08-21 (Recipe Provenance Rollout Audit)

- **Objective:** Determine whether recipe-aware conversion artifacts require
  further changes before production use or a future Marker 2 evaluation.
- **Actions:** Used `rg` and `sed` to inspect container build/release workflows,
  package versions, operator API usage, recipe construction, and Surya's active
  settings/model-loading paths. No runtime code or external state was changed.
- **Findings:** The container build does not inject a source revision and the
  Python package remains version 0.3.0, so exact production build provenance
  needs a release-metadata follow-up. Recipe schema 1 covers the current dated
  Marker 1 model identifiers but not every environment-overridable output
  setting or immutable model payload checksum; those must be incorporated
  before custom settings or Marker 2 are enabled. Recipe artifact APIs exist,
  while dedicated CLI/console controls remain optional operator UX work.
- **Status:** Follow-ups recorded in `TODO.md`; current pinned Marker 1/default
  operation is not blocked.

## 2026-08-21 (Production Build Provenance)

- **Objective:** Ensure production worker archives identify the exact BlobForge
  build rather than reporting a stale package version or unknown source.
- **Changes:** Aligned the Python package and lockfile at version 0.4.0. Added a
  final-image `BLOBFORGE_BUILD_REVISION` argument/environment value to the
  `Containerfile`, and made the GitHub container workflow pass the GitHub commit
  SHA for every CPU/CUDA architecture build. Documented the requirement for
  custom builds and added regression coverage for all wiring.
- **Tooling:** Used `rg`, `sed`, `apply_patch`, offline `uv lock`, and focused
  pytest. No image was published or external deployment changed.
- **Verification:** All 9 dependency/provenance tests passed; the lock resolved
  without network access.
- **Status:** Complete; ready for an atomic commit.

## 2026-08-21 (Effective Conversion Recipe Settings)

- **Objective:** Prevent environment-overridden conversion settings from
  silently sharing an artifact identity with the Marker defaults.
- **Changes:** Added a deliberate allowlist of output-affecting Marker and Surya
  settings to recipe schema 1: render format/encoding, PDF flattening, render
  DPI, detection geometry/thresholds, foundation quantization/token/padding
  controls, recognition padding, layout image/slice/limit settings, and table
  image/limit settings. Fractional values are normalized to decimal strings for
  cross-language canonical hashing. Batch sizes, worker counts, compilation,
  cache paths, device choice, and logging/progress controls remain excluded as
  performance-only implementation details. Documented that Marker 2 must expose
  a pinned/verifiable model revision before adoption because its payload cannot
  be safely checksummed before the upstream runtime downloads it.
- **Tooling:** Inspected installed Marker/Surya settings and loaders with `rg`
  and `sed`, edited with `apply_patch`, probed the real current recipe, and ran
  focused pytest and Ruff. Ruff initially lacked network access and passed after
  the approved dependency fetch.
- **Verification:** 31 focused identity/worker tests passed before final test
  isolation cleanup; the real Marker 1 recipe serializes successfully with
  digest `2182c532...`; changed identity files pass Ruff.
- **Status:** Complete; ready for an atomic commit.

## 2026-08-21 (Conversion Artifact CLI)

- **Objective:** Make recipe-aware artifacts operable without custom API calls.
- **Changes:** Added `blobforge artifacts` with human/JSON output and selected/
  legacy markers; added `--recipe-digest` to `download` and `preview`, including
  access to retained artifacts while another recipe is queued; added dry-run
  capable `blobforge request-conversion` to select an existing artifact or queue
  an exact digest; and display worker recipe digests in `workers --verbose`.
  Documented the complete A/B command flow and retained management-console
  controls as optional future UX.
- **Tooling:** Inspected CLI/parser/client structure with `rg` and `sed`, edited
  with `apply_patch`, ran focused pytest, exercised both new command help pages,
  compiled the Python package, and ran Ruff on the new tests. One orchestration
  invocation had a JavaScript interpolation typo before command execution; the
  corrected Ruff run found and fixed only import ordering.
- **Verification:** All 23 focused CLI/client tests passed; both command parsers
  render successfully; Python compilation and new-test Ruff checks pass.
- **Status:** Complete; ready for an atomic commit.

## 2026-08-21 (Provenance Follow-up Final Audit)

- **Objective:** Verify the build provenance, effective recipe settings, and
  artifact CLI changes together and identify anything else actionable now.
- **Actions:** Synced the locked 0.4.0 development environment, ran the complete
  Python and Bunny suites, TypeScript checking and production bundle build,
  probed real recipe/provenance output with an injected deployment revision,
  checked the complete commit-range diff, and confirmed a clean worktree. The
  first sync could not fetch the missing Hatchling build backend under sandboxed
  DNS; the approved retry succeeded.
- **Verification:** 162 Python tests and 5 subtests passed with two upstream
  Marker/Surya Pydantic deprecation warnings. All 25 Bunny tests, TypeScript
  checking, and the production build passed. The runtime probe reported
  BlobForge 0.4.0, the supplied build revision, and recipe digest
  `2182c532...`. Commit-range whitespace validation passed.
- **Review:** No remaining locally actionable correctness issue was found. The
  only conversion-identity prerequisite intentionally left open is requiring a
  pinned/verifiable model revision from the future Marker 2 inference backend;
  BlobForge cannot truthfully derive that checksum before the upstream runtime
  downloads the model. Management-console artifact controls remain optional
  because equivalent authenticated CLI/API workflows are complete.
- **Commits:** `afb7148` embeds worker release provenance; `96a9f50`
  fingerprints effective conversion settings; `39b00c1` exposes artifacts in
  the CLI.
- **Status:** Complete.

## 2026-08-27 (MDAF Migration Completion and v2 Local Stage)

- **Objective:** Finish every historical ZIP-to-MDAF conversion, independently
  verify the complete corpus, materialize the proposed S3 v2 hierarchy locally,
  and close the CPU converter canaries without touching production.
- **Actions:** Completed the resumable migration of all 1,377 paired legacy
  ZIP/PDF records. Four image/HTML-only headings initially violated the MDAF
  non-empty outline-title schema; changed outline derivation to retain their
  Markdown while omitting only unusable semantic nodes, then retried all four
  successfully. Added bounded `--jobs` migration concurrency and used the
  measured two-worker setting on the 32-GiB host. Added read-only catalog/MDAF
  cross-verification and fail-closed local v2 staging, including hard-linked
  source/artifact objects, canonical recipe JSON, and a checksummed run manifest.
- **Validation performance:** Cached the four immutable JSON Schema validators
  and replaced quadratic UTF-8 prefix decoding with constant-time continuation-
  byte boundary checks after whole-document UTF-8 validation. Added regression
  coverage for a span that splits a multibyte character.
- **Final migration result:** Inventory is 1,808 sources, 1,377 legacy artifacts,
  1,377 paired, 1,377 converted, and zero failed. The local stage has exactly
  1,377 source objects, 1,377 MDAFs, one recipe, and one run manifest (2,756
  files). Recipe digest is
  `blake3:8822289b4860301f73b64a2139a3559f2026793a48135fc13b83bc84a67b0c39`;
  stage-manifest digest is
  `blake3:8cb0de0459044c53c2038192af2f8a8e438d9a33c4c4c9502d81f930140fd213`.
- **Evaluation result:** Marker 2 no-OCR completed the two-page fixture in 67.5s
  and passed Vulcan. The corrected eight-page Docling canary completed in 269.3s
  versus Marker 1 in 519.2s; both emitted 18 Markdown headings/two assets and
  similar word counts. Vulcan dry-run import planned one root, 18 section notes,
  and both Docling assets. Added shared fallback outline generation for future
  adapters and documented results in `docs/converter_benchmark_results.md`.
- **Verification:** A staged 111-member legacy MDAF independently passed Vulcan
  with 415 page intervals, 415 polygons, and 415 outline nodes. The final suite
  passed with 197 tests, 5 subtests, and two existing dependency deprecation
  warnings. Python compilation, CLI help, wheel construction/schema inclusion,
  and `git diff --check` passed.
- **Safety:** The full bucket mirror was read with `rclone copy --immutable`.
  No S3/coordinator write, paid OCR API request, remote deletion, worker start,
  or production cutover occurred. The disposable Vulcan vault was removed.
- **Status:** The local migration, v2 staging input, MDAF/converter vertical
  slice, and CPU canaries are complete. Coordinator v2 persistence/dual-read,
  model checksum freezing, human quality scoring, and publication remain gated.

## 2026-08-27 (Local MDAF Migration and Converter Evaluation Build)

- **Objective:** Implement the approved BLAKE3/MDAF vertical slice, locally
  migrate every historical conversion, isolate converter backends for the
  32-GiB CPU machine and APIs, freeze the rulebook corpus, and select a safer v2
  object layout without mutating production.
- **Discovery:** Audited BlobForge's SHA-256 raw/output key paths, worker ZIP
  packaging, coordinator-facing identities, and CLI; read Vulcan's complete MDAF
  v1 specification, schemas, fixtures, Rust validator, and artifact CLI; audited
  `pdf-to-wiki` page-anchor, TOC, heading, and page-label strategies. Read-only
  inventoried `blobforge:` with rclone. The remote has 3,634 objects / 31.98 GiB,
  including 1,808 raw PDFs and 1,377 flat legacy ZIPs, all paired. A real ZIP
  confirmed that production Markdown omitted page-anchor pagination but retained
  Marker TOC page IDs/polygons and page statistics.
- **Local data actions:** Added `.blobforge-migration/` to `.gitignore` and used
  `rclone copy blobforge:pdf ... --immutable`, never `sync`, to create the full
  local mirror in 9m38s. No remote write/delete command was run. Built a WAL-mode
  SQLite migration catalog and began a resumable local bulk conversion after a
  20-artifact no-failure canary. Generated sources, artifacts, databases,
  reports, model caches, and evaluation results remain git-ignored.
- **Implementation:** Added explicit `blake3` and `jsonschema` dependencies;
  canonical parameter JSON, logical MDAF identity, deterministic atomic ZIP
  creation, reviewed Vulcan schemas, schema/semantic/archive/member/provenance/
  UTF-8-span/Markdown-link validation, versioned converter bundle validation,
  subprocess execution, shared packaging, structural comparison metrics,
  corpus manifests, safe v2 object-key constructors, one-pass BLAKE3+SHA-256,
  algorithm-specific xattrs, and an additive algorithm-keyed SQLite digest
  cache. Added `blobforge migrate`, `corpus`, `evaluate`, and `compare-mdaf`
  commands plus Poppler, Marker 1, Marker 2, Docling, and spend-capped Mistral
  adapters in separately locked uv projects.
- **Migration semantics:** Legacy SHA-256 is verified and retained as an alias;
  historical Marker/model versions are explicitly `unavailable`; original
  Markdown, metadata, and assets are native evidence. Exact page anchors become
  final UTF-8 page spans. Where absent, only exact normalized Marker-TOC to
  Markdown-heading matches get page/polygon mappings; no whole-page mapping is
  invented. Every Markdown heading contributes to a complete aligned outline.
  Secret-like legacy metadata fails closed.
- **Environment findings:** Initial isolated resolutions selected CUDA 13
  PyTorch and several GiB of irrelevant NVIDIA packages. Interrupted both
  installs, added explicit PyTorch CPU indexes, regenerated locks, and installed
  `torch 2.10.0+cpu`. Pinned Marker 1.10.2/Surya 0.17.1, Marker 2.0.0/Surya
  0.22.1, Docling 2.122.0, and Mistral SDK 2.9.4. Marker 2 still needed a
  first-run Datalab font download even with OCR disabled.
- **Evaluation:** Frozen `/home/eric/rulebooks` as 43 documents / 9,465 pages /
  1,294,553,125 bytes with manifest identity
  `blake3:44b252c25c8a61dc2771c337cfca9d6b43734cefbac44f2d50b8e5130a3e2b35`.
  Two-page CPU smoke times were Poppler 0.9s, Docling 40.2s, and Marker 1 175.6s;
  all artifacts passed BlobForge and Vulcan with two page mappings. An 8-page
  rulebook Docling run took 262.9s, produced eight page mappings and two assets.
  It exposed absolute temporary image links, so the adapter now rewrites links
  before span calculation and BlobForge rejects absolute/file targets.
- **Verification so far:** The official Vulcan logical-identity vector matches;
  26 focused tests passed after schema/link hardening. A representative real
  migrated artifact passed Vulcan with 14 declared members, 19 page/polygon
  mappings, and 19 outline nodes. Poppler, Docling, and Marker 1 fixture MDAFs
  passed Vulcan independently. Full-suite, regenerated Docling rulebook,
  Marker 2 no-OCR, Marker 1 rulebook, and final bulk migration verification are
  still in progress at this log point.
- **Tooling:** Used `update_plan`, `rclone`, `find`, `sqlite3`, `unzip`, `jq`,
  `rg`, `sed`, `pdfinfo`, `pdftotext`, `uv add/lock/sync/run`, `pytest`, Cargo,
  the current Vulcan CLI, `apply_patch`, local process polling, and official
  Docling, Marker, Mistral, PyPI, and GitHub documentation searches. One Mistral
  environment command combined `uv lock` and `uv sync`; subsequent operations
  remain separate and uv-only.
- **Status:** In progress locally. Production S3/coordinator publication,
  dual-read rollout, paid API calls, and deletion remain intentionally disabled.

## 2026-08-27 (32-GiB Conversion Test Readiness Audit)

- **Objective:** Determine whether the 32-GiB machine and current BlobForge
  checkout are ready to run reusable, comparable conversion tests.
- **Actions:** Inspected repository status, converter/MDAF/evaluation modules,
  pending tasks, installed Python packages, system tools, corpus count and exact
  duplicates, installed Vulcan commands/version, current Vulcan source, schemas,
  examples, validator implementation, and CLI tests. Added
  `docs/conversion_test_readiness.md` with ready components, blockers, minimum
  launch sequence, and full-run gates. Updated the live corpus counts, cost
  model, evaluation documents, tasks, and findings after the redundant Rigger
  variants disappeared from the corpus.
- **Findings:** Hardware is adequate, and Marker 1 plus BLAKE3/Poppler/container
  tools are present. Software is not ready for scored runs: BlobForge lacks the
  shared MDAF producer, converter ABI/adapters, frozen BLAKE3 corpus manifest,
  Docling environment, and comparison harness. Installed Vulcan 0.1.0 lacks the
  `artifact` subcommand even though the checkout implements it. The current
  corpus is 43 unique PDFs / 9,465 pages / 1,234.58 MiB with no exact duplicate.
- **Tooling:** Used `git`, `rg`, `command`, `uv`, `find`, `wc`, `pdfinfo`,
  `pdftotext` discovery, `sha256sum`, `sort`, `xargs`, `awk`, `stat`, `sed`, and
  `apply_patch`. The first `uv pip list` probe hit the sandboxed home-cache lock;
  it was repeated read-only with `UV_CACHE_DIR` under `/tmp`.
- **Status:** Not ready for scored/full-corpus conversion. Ad-hoc Marker 1 is
  technically possible but intentionally deferred to avoid legacy-only output.

## 2026-08-27 (TextPack Cleanup and Reverse Conversion)

- **Objective:** Let large PDF trees either remove generated TextPacks or
  restore them to the hydrated Markdown/assets layout.
- **Actions:** Re-inspected the hydrated maintenance module, CLI parser,
  tests, documentation, repository status, and tracking files. Chose sibling
  `clean-textpacks` and `unpack` operations under the existing `blobforge
  hydrated` group, with PDF-anchored discovery, preview-by-default behavior,
  explicit `--execute`, and overwrite protection through `--force`. Implemented
  TextPack discovery and cleanup plus reverse conversion with CRC, metadata,
  Markdown type/body, duplicate-member, path-traversal, member-type, and
  unexpected-entry checks. Reverse conversion stages assets, rewrites links,
  preserves archives on error, skips existing outputs by default, and deletes
  the archive only after restoration succeeds. Added round-trip, dry-run,
  cleanup, force/skip, alternate `text.markdown`, and malicious traversal tests;
  updated CLI help, README, hydrate design, tasks, and durable findings.
- **Tooling:** Used `update_plan`, `sed`, `rg`, `git status`, `apply_patch`, and
  uv-driven focused pytest and CLI help checks.
- **Verification:** The combined hydrated-maintenance and hydrator suite passed
  all 32 focused tests. The final isolated repository suite passed with 181
  tests and 5 subtests; only two pre-existing dependency deprecation warnings
  remain. Python byte compilation, both new CLI help paths, and
  `git diff --check` passed.
- **Status:** Complete. No user PDF, hydrated output, or TextPack was modified.

## 2026-08-26 (MDAF / BLAKE3 Redesign Discovery)

- **Objective:** Redesign BlobForge around BLAKE3 source identity and MDAF v1
  artifacts, including richer PDF-to-Markdown evidence, converter evaluation,
  API cost analysis, and loss-aware migration of existing ZIP artifacts.
- **Actions:** Read the authoritative Vulcan MDAF v1 specification; inventoried
  the BlobForge repository, current task list, and prior conversion/provenance
  work; located Vulcan's implemented MDAF validator/importer, bundled schemas,
  fixtures, and consumer documentation. Confirmed the working tree was clean at
  discovery start. Mapped current SHA-256/recipe behavior across ingestion,
  xattrs, the local SQLite hash index, hydration, worker packaging, Bunny
  Database, APIs, and S3 key layout. Researched current primary documentation
  for Mistral OCR 4.0, Marker 2.0, Docling, Chandra 2/Datalab, and Google
  Document AI. Documented the proposed four-identity model, staged worker
  pipeline, additive coordinator schema, object layout, publication protocol,
  LFS-aware hash migration, honest legacy packaging, rollout gates, evaluation
  corpus, quality metrics, and API/local-compute cost formulas in
  `docs/mdaf_redesign.md` and `docs/converter_evaluation.md`. Added the program
  backlog to `TODO.md` and the durable findings to `AGENTS.md`.
- **Cost findings:** Mistral lists OCR 4.0 at $4/1,000 pages and annotated pages
  at $5/1,000; the conservative MDAF mapping budget is therefore $500 per
  100,000 pages before retry margin. Google lists Layout Parser at $10/1,000
  pages. Datalab's public price could not be read reliably, so it remains a
  quote/pilot input. Exact corpus cost remains pending a real page inventory.
- **Tooling:** Used `wc`, `sed`, `find`, `rg`, Git status/diff inspection,
  `apply_patch`, the task-plan tracker, and primary-source web search/open/find
  operations. No worker, coordinator, object-store, API account, or deployment
  state was changed.
- **Verification:** `git diff --check` passed after documentation creation.
  Cost-table arithmetic and local break-even formulas were checked directly.
- **Status:** Architecture discovery is complete and ready for review; runtime
  implementation has intentionally not started.

## 2026-08-26 (Priority Rulebook Cost Inventory)

- **Objective:** Calculate a realistic API-conversion budget for the rulebooks
  most important to the future Vulcan wiki workflow.
- **Actions:** Read-only inventoried `/home/eric/rulebooks` with recursive file
  discovery, `pdfinfo`, `pdftotext`, and file metadata. Counted pages and bytes
  for all PDFs, verified that all documents expose text, inspected the one
  encrypted PDF's permissions, and counted existing Markdown/asset outputs.
  Calculated per-book and total Mistral standard/annotated costs, retry and
  repeat-run scenarios, a Google Layout control, and illustrative local runtime
  floors. Recorded the durable inventory and recommended spend plan in
  `docs/rulebook_corpus_cost.md` and the key finding in `AGENTS.md`. A follow-up
  primary-source search of Datalab documentation confirmed per-page billing,
  $5 trial credit, 200 MiB/7,000-page request limits, and one-hour result
  retention, but its exact current rate remains delegated to the dynamic pricing
  page; the plan therefore uses the trial for a measured subset before approval.
- **Findings:** The 17 PDFs total 3,060 pages and 488.5 MiB. A full Mistral OCR
  4.0 annotated pass is approximately $15.30; two passes with 10% margin are
  $33.66. All 17 sources have existing Markdown and asset outputs. One 436-page
  PDF uses AES permissions encryption but permits local copy/print extraction.
- **Tooling:** Used `rg`, `pdfinfo`, `pdftotext`, `stat`, `find`, `awk`, `sort`,
  `wc`, `sed`, shell timeouts, parallel read-only command execution, and
  `apply_patch`, plus official Datalab web search. No corpus file, provider
  account, coordinator, object-store, or deployment state was modified.
- **Status:** Complete; the corpus is small enough to evaluate in full rather
  than sampling.

## 2026-08-26 (Local Converter Requirements and Candidate Survey)

- **Objective:** Determine what is required to run Marker 2 and Docling locally
  and define a broad but decision-useful evaluation set.
- **Actions:** Read-only inspected CPU, RAM, disk, GPU/driver visibility,
  container runtimes, Poppler, uv, and the Python environment. Researched
  primary documentation for Marker 2/Surya backends, Docling standard/VLM
  installation and CLI outputs, MinerU pipeline/hybrid requirements, olmOCR,
  PP-StructureV3/PaddleOCR-VL, Chandra, Unstructured, Datalab, Mistral, Google
  Document AI, and AWS Textract. Documented isolated environment rules, CPU and
  GPU requirements, example uv workflows, native-evidence requirements, local
  and hosted candidate matrices, and a three-stage evaluation order in
  `docs/local_converter_evaluation.md`. Added concrete environment/adapter tasks
  to `TODO.md` and durable hardware findings to `AGENTS.md`.
- **Findings:** The current 4-core/31-GiB workstation can run CPU correctness
  candidates but has no functioning NVIDIA path. Marker 2 fast/no-OCR needs no
  inference server; its selective CPU VLM mode needs external `llama-server`,
  while balanced is a GPU/remote-Surya recipe. Docling standard is the strongest
  immediate CPU candidate and can retain both Markdown and lossless provenance
  JSON. A separate 48-80 GiB NVIDIA host is the efficient route to broad VLM
  coverage. AWS's published US West example prices Layout+Tables at $0.015/page,
  or about $45.90 for this corpus.
- **Tooling:** Used `lscpu`, `free`, `nvidia-smi`, `df`, command discovery, uv,
  `sed`, `apply_patch`, and primary-source web search/open results. The uv Python
  probe could not create a cache temporary file under the sandboxed home cache;
  this did not affect environment inspection or alter the repository.
- **Status:** Complete; no packages, models, containers, GPU hosts, or provider
  services were installed or started.

## 2026-08-27 (Evaluation Plan Corrected for Available Hardware)

- **Objective:** Rework the converter evaluation around the user's actual fleet:
  a Windows/24-GiB host with GTX 1070, a GPU-less 32-GiB desktop, and older
  laptops.
- **Actions:** Researched primary NVIDIA, vLLM, llama.cpp, Surya, and Docling
  documentation for WSL2 Pascal support, CUDA architecture compatibility,
  inference backend requirements, and small Docling VLM support. Updated
  `docs/local_converter_evaluation.md`, `TODO.md`, and `AGENTS.md` so owned
  hardware and hosted controls precede any optional GPU rental.
- **Findings:** WSL2 supports CUDA on Pascal, but the GTX 1070's compute
  capability 6.1 is below vLLM's 7.0 floor. CUDA 13 removed Pascal library and
  offline-compilation support, so experiments require a pinned CUDA 12.x stack.
  Current llama.cpp still targets 6.1, making Surya's 650M GGUF backend worth a
  feasibility probe. Docling's 256-258M GraniteDocling and SmolDocling models
  are plausible 8-GiB candidates. The 32-GiB desktop remains the full-corpus CPU
  worker.
- **Tooling:** Used primary-source web search, `sed`, `rg`, and `apply_patch`.
  No machine, package, model, container, provider, or deployment was changed.
- **Status:** Complete; hardware installation and benchmark tasks remain pending.

## 2026-08-27 (Rulebook-Driven Converter Adapter Architecture)

- **Objective:** Turn the priority rulebooks into both the high-value conversion
  workload and the common acceptance corpus for local and API engines producing
  comparable MDAFs.
- **Actions:** Read the complete authoritative MDAF v1 specification, inspected
  the repository package layout, dependency policy, current recipe identity,
  Marker-only conversion child, worker packaging path, and existing redesign
  and evaluation documents. Added `docs/converter_adapter_architecture.md` with
  a versioned subprocess adapter boundary, private ConversionBundle, shared MDAF
  builder, isolated engine environments, nested corpus sets, comparison outputs,
  gates, and an implementation-ordered vertical slice. Cross-linked and updated
  the redesign, evaluation protocol, task list, and durable findings.
- **Findings:** Converter modules must not directly package MDAF or load all ML
  dependencies into one worker. Adapters should finish byte-changing Markdown
  normalization and return mappings/native evidence through a filesystem ABI;
  the shared builder owns canonical BLAKE3, byte-span validation, provenance,
  archive safety, and Vulcan validation. Whole books measure production utility
  and failures, while 5-10 labeled pages per book and a hidden holdout provide
  defensible quality comparisons.
- **Tooling:** Used `wc`, `sed`, `rg`, and `apply_patch`. No runtime code,
  corpus files, dependencies, APIs, workers, or infrastructure were changed.
- **Status:** Architecture documented for approval; implementation remains
  pending in `TODO.md`.

## 2026-08-27 (Expanded Rulebook Corpus and API Cost Refresh)

- **Objective:** Re-inventory newly added rulebooks and recalculate expected API
  conversion charges from current page counts and current official rates.
- **Actions:** Ran read-only `pdfinfo` and file-size inspection over every PDF,
  grouped pages by rulebook family, hashed every source with SHA-256 to identify
  exact duplicates, and searched the broader rulebook directory for the two
  previously inventoried Trinity files. Reviewed current official Mistral OCR
  4.1, Mistral Batch/limits, Google Document AI, AWS Textract, and Datalab
  documentation. Replaced `docs/rulebook_corpus_cost.md` with the expanded
  inventory, deduplication rules, provider scenarios, sensitivity cases, and
  spend-cap recommendation. Updated current evaluation/adapter documents,
  tasks, and durable findings.
- **Findings:** There are 45 readable paths / 9,853 pages / 1,272.64 MiB raw and
  44 exact-byte-distinct sources / 9,659 pages after removing one 194-page
  duplicate. Thirty paths are new relative to the first inventory, while two
  Trinity PDFs totaling 547 pages are absent. Current deduplicated list prices
  are $38.64 Mistral standard, $48.30 conservative annotated, $96.59 Google
  Layout, and approximately $144.89 AWS Layout+Tables. Mistral's official
  general price table and Batch guide disagree between $0.40/1,000 OCR pages
  and a 50% discount; a metered pilot is required.
- **Tooling:** Used `find`, `sort`, `xargs`, `bash`, `pdfinfo`, `stat`, `awk`,
  `sha256sum`, `rg`, `sed`, `apply_patch`, and primary-source web research. No
  corpus content, API account, dependency, worker, or infrastructure was
  modified.
- **Status:** Cost refresh complete; corpus scope decisions and any paid API run
  remain pending.

## 2026-08-27 (Hydrated Output Maintenance)

- **Objective:** Add safe recursive CLI maintenance for large PDF trees whose
  sibling hydrated Markdown and asset directories create filesystem clutter.
- **Actions:** Inspected the CLI, hydrator, hydration tests and docs,
  repository status, task/work-log conventions, and searched the repository for
  an existing TextPack implementation. Confirmed no `textpack` executable or
  project dependency is installed. Consulted the TextBundle v2 specification:
  `.textpack` is a ZIP containing lowercase `text.*`, `info.json`, and `assets/`,
  and bundled asset references use the `assets/` prefix. Chose an extensible
  `blobforge hydrated` command group with dry-run-first `clean` and `textpack`
  operations. Implemented PDF-anchored discovery, source-path validation,
  recursive cleanup, TextBundle v2 metadata and layout, asset-link rewriting,
  same-directory atomic archive replacement, CRC/structure validation,
  existing-target skip/force semantics, and symlink containment. Added focused
  tests and documented commands and safety behavior in README and the hydrate
  design.
- **Tooling:** Used `git status`/diff, `rg`, `sed`, command discovery, the
  TextBundle primary specification, `update_plan`, `apply_patch`, and uv-driven
  pytest/compile/help checks. The initial uv run needed network access to fetch
  the editable build requirement. The first full suite inherited live
  coordinator environment variables and caused 16 existing mocked worker tests
  to attempt DNS; rerunning with those variables unset passed. No local library
  files, remote service state, dependencies, or infrastructure were changed.
- **Verification:** Focused hydration/maintenance tests passed. The final full
  isolated suite passed with 172 tests, 5 subtests, and only two pre-existing
  dependency deprecation warnings. Python byte compilation, CLI nested help,
  and `git diff --check` also passed.
- **Status:** Complete.

## 2026-08-27 (Self-hosted Backend Foundation)

- **Objective:** Replace Bunny Edge Scripting, Bunny Database, and S3 with a
  conventional Podman-deployable service using a local database and directory,
  while making the persistence model ready for MDAF/BLAKE3 and non-PDF media.
- **Actions:** Audited the Bunny route/schema contract, dependency-free Python
  coordinator client, current PDF-only worker, ingestion flow, container build,
  and CI. Added locked `server` dependencies, a FastAPI service, SQLite WAL
  schema, local object layout, streamed atomic transfers, HMAC capabilities,
  lease fencing, media-filtered claims, static hashed worker bootstrap, artifact
  history, and a `blobforge serve` entry point. Added a validating/idempotent
  import from the checksummed local v2 stage plus a SHA-256-verifying/BLAKE3-
  deriving importer for the 431 raw sources not represented by completed MDAFs.
  Added a lightweight server
  Containerfile, Podman Quadlet/volume/environment examples, full-test CI, and
  GHCR server/CPU-worker/CUDA-worker image tags; changed the Bunny deployment
  workflow to manual-only. Updated the client, current worker media capability,
  README, design pointer, task list, and durable findings.
- **Findings:** Existing workers already consume coordinator-issued transfer
  URLs, so the storage change does not require bucket credentials or an
  immediate worker rewrite. One server can safely coordinate many workers, but
  SQLite makes active-active API replicas an explicit non-goal. Media type must
  be a claim constraint, not merely metadata, or a PDF worker could consume
  future audio/video jobs. Starlette 1.6's file response path deadlocked under
  the installed HTTPX ASGI test transport; an async chunked local-file response
  preserves streaming and makes in-process integration tests deterministic. A
  stage-only cutover would have silently omitted 431 of 1,808 raw sources, so
  completed-artifact and missing-source imports are separate, repeatable phases.
  Final context review found roughly 46 GB of ignored migration/model/reference
  data and dot-prefixed provider environment files not covered by
  `.dockerignore`; all are now explicitly excluded from local and CI builds.
- **Tooling:** Used `rg`, `sed`, `find`, `uv add`, `uv lock`, `uv sync`, Python
  compilation, focused pytest, `apply_patch`, Podman discovery, and official
  Podman Quadlet/GitHub Container Registry documentation. Initial dependency
  resolution was blocked by sandbox DNS and was repeated with approved network
  access. The first rootless Podman boot canary found that a fresh volume hides
  the image-layer ownership; the Quadlet now uses `:U`, after which the server
  started as UID 10001 and its loopback health endpoint returned successfully.
- **Verification:** The full suite passes with 201 tests and 5 subtests; the
  legacy Bunny package still passes its 25 tests and TypeScript check. Python
  byte compilation, source/wheel builds, and `git diff --check` pass. The exact
  server image builds successfully with Podman; a rootless named-volume boot
  canary returned the SQLite/filesystem health payload; Podman's official
  Quadlet generator accepted both unit files and emitted their systemd units.
  The isolated canary container and volume were removed. The complete
  1,377-artifact production-data import remains intentionally pending. A
  read-only catalog/path/size dry run successfully accounted for all 1,808 raw
  sources without creating the proposed data directory.
- **Status:** Foundation implemented; deployment and migration canaries remain
  intentionally gated in `TODO.md`.

## 2026-08-27 (Capability Routing, Legacy Visibility, and Identity)

- **Objective:** Verify legacy artifact labeling/version evidence, allow an
  operator to select PDF conversion backends, make coordinator scheduling
  media-neutral for constrained workers, and prepare the Citadel deployment
  for OIDC and SCIM.
- **Actions:** Audited the complete legacy migrator, staged recipe, MDAF
  provenance/renditions, local importer, recipe-bound leases, worker payloads,
  and Gandalf's Citadel service inventory, Todo Quadlet, Authentik OIDC/SCIM
  reconciliation, Caddy compilation, and quiesced backup pattern. Added
  explicit legacy/backend/version catalog columns and loss-aware import
  provenance. Added a registered recipe catalog, exact recipe listing,
  unambiguous backend selection, multi-capability worker registration and
  claims, selected-capability lease responses, CLI/client selection, and a
  one-capability compatibility advertisement for the existing Marker worker.
  Added Authlib OIDC discovery/code-flow sessions, SCIM-backed request-time
  roles, same-origin checks for cookie mutations, and bearer-protected SCIM 2.0
  discovery plus Users/Groups CRUD/filter/PATCH endpoints. Added locked server
  dependencies, environment examples, tests, architecture/deployment docs, and
  removed a duplicate rendition path from future legacy activity output lists.
- **Findings:** Every one of the 1,377 staged MDAFs is internally marked by its
  legacy activity/rendition/recipe and records every version fact known; the
  historical Marker/model version is genuinely unavailable. The old catalog
  did not expose that distinction, which is now corrected. Span enrichment is
  sparse when old Marker output lacks anchors and must not be described as
  complete. The old worker protocol already filtered media but could advertise
  only one recipe. Gandalf's Todo role is the correct deployment model, but
  BlobForge must keep OIDC `sub` equal to SCIM `externalId`, privately route
  SCIM, and back up the whole local data tree. Authentik's official SCIM
  documentation confirms its default `hashed_user_id` OIDC mode already equals
  the default SCIM external ID, correcting the earlier UUID recommendation.
  No Gandalf mutation or production deployment was performed; vaulted secret
  creation, generated inventory compilation, image publication, import/restore
  canary, and DNS cutover remain explicit gates.
- **Tooling:** Used `rg` and `sed` for repository/Gandalf audits, official
  Authlib, Authentik SCIM, and RFC 7644 references for protocol decisions,
  `uv add`/`uv lock` for dependencies, `apply_patch` for implementation, and
  focused pytest runs. Dependency download required approved network access.
- **Verification:** The environment-isolated full suite passes with 204 tests
  and 5 subtests. Mixed PDF/audio capability selection, recipe discovery, SCIM
  role provisioning/deactivation, and explicit imported-legacy metadata have
  dedicated coverage. Python byte compilation, CLI help, `git diff --check`,
  source/wheel builds, the exact server-image build, and an import probe inside
  that image pass. The first unisolated suite run inherited the live Bunny URL
  and produced 17 sandbox DNS failures; explicitly removing those two external
  environment variables reproduced CI and left one expected mock assertion,
  which was updated for the new capabilities payload before the clean pass.
  Gandalf was audited read-only; no check-mode deployment was claimed.
- **Status:** BlobForge implementation complete; Gandalf deployment remains a
  gated infrastructure follow-up.

## 2026-08-27 (Complete Local Data Migration and Access-Control Boundary)

- **Objective:** Confirm group-limited management access, define a safe tagging
  and resource-authorization direction, and materialize the complete verified
  coordinator recovery unit before the GHCR/Citadel rollout.
- **Actions:** Audited OIDC/SCIM role checks, static/worker token behavior,
  existing job tags, and both migration runbooks. Confirmed 959 GB free and a
  fresh destination. Ran the full stage importer dry-run across all 1,377
  MDAF/source pairs, executed it, validated all 1,808 legacy source records,
  then imported the 431 sources without artifacts. Repeated both execute paths
  to prove idempotency. Independently queried SQLite counts/provenance, matched
  filesystem object counts, checked for duplicate source digests/orphan and
  pending artifacts, checkpointed WAL, ran `PRAGMA quick_check`, and opened the
  exact directory through a fresh coordinator instance to create/reuse its
  persistent capability key. Added `MIGRATION.json`, froze all 3,188 recovery
  files into a relative BLAKE3 manifest, and verified that manifest in full.
  Documented current group gating, the distinction between tags and ACLs, a
  private collection/group-role design, and the exact Citadel data handoff.
- **Results:** The 33 GB recovery unit at
  `.blobforge-migration/local-server-data` has 1,808 sources/jobs, 3,616 aliases,
  1,377 done legacy MDAFs, 431 queued raw-only jobs, 1,808 source objects, 1,377
  artifact objects, zero pending objects, zero orphan artifacts, and one
  canonical recipe. Source bytes total 27,639,911,801 and artifact bytes total
  6,892,298,683. All 1,377 artifacts record `blobforge-zip-v0`, Marker version
  `unavailable`, partial metadata recovery, and
  `page-anchors-and-exact-toc-heading-alignment`. The verified 3,188-line
  manifest is `.blobforge-migration/local-server-data.blake3`; its BLAKE3 is
  `b654923b59e24bd5709aab3e8a9803b351f5c03cba48596baf3df876c36ddf23`.
- **Access finding:** Mapping only an exact SCIM group to `admin` limits all
  interactive access to that group, and Authentik should enforce the same group
  at the application binding. Static client tokens remain unrestricted and
  worker tokens retain compatibility read access, so broader multi-user use is
  gated on scoped service accounts, machine-token narrowing, private
  collections, collection group roles, and deny-by-default query tests. Tags
  remain discovery metadata and are not an authorization mechanism.
- **Tooling:** Used `df`, `du`, `find`, `rg`, `sqlite3`, the two `blobforge
  migrate import-*` commands, a fresh FastAPI application canary, `b3sum`,
  `sha256sum`, `apply_patch`, and `git diff --check`. All migration operations
  were local; no Bunny/S3 mutation, remote upload, DNS change, or deployment
  occurred.
- **Status:** Local data migration complete and frozen for transfer. Repository
  publication, GHCR build, Gandalf provisioning, secure data transfer, restored
  manifest verification, and DNS cutover remain.

## 2026-08-27 (Publish, Transfer, and Citadel Cutover)

- **Objective:** Publish the self-hosted coordinator, transfer the complete
  recovery unit, deploy the group-gated service on Citadel, and cut the
  canonical hostname over without deleting the legacy backend.
- **Actions:** Pushed BlobForge commit `60988ef`; GitHub Actions run
  `33069776111` passed tests and all server/CPU/CUDA image builds. Transferred
  34,536,741,338 bytes in 3,188 files to `/srv/blobforge`, copied the frozen
  manifest as `/srv/blobforge/MIGRATION.blake3`, and verified every file plus
  SQLite and object/catalog counts on Citadel. Added and validated Gandalf's
  digest-pinned Quadlet, OIDC/SCIM integration, private SCIM route, Caddy site,
  and quiesced backup profile. Rotated all deployment credentials after a
  diagnostic exposed a rendered secret-bearing unit command. Fixed generic
  Quadlet quote/backslash escaping, selected the exact `blobforge-admin` group,
  and added regression coverage. The DNS planner was extended with
  retirement-only Bunny `PULLZONE` support and delete-before-add ordering; the
  reviewed plan deleted only the legacy BlobForge Pull Zone record and created
  the managed CNAME to Citadel.
- **Results:** The coordinator container is running and healthy from
  `ghcr.io/tionis/blobforge@sha256:5c503c83b8940af4037135b58f747af7db24070419108e291114ad38186b06bc`.
  Private SCIM readiness succeeds, OIDC uses matching `hashed_user_id` /
  `externalId`, and only `blobforge-admin` maps to `admin`. Public DNS resolves
  through `citadel.tionis.dev` to `159.195.20.56`. The old Bunny/S3 objects were
  not mutated or deleted.
- **Verification:** Citadel reported the exact accepted 1,808/3,616/1,377/431
  database split, 1,808 source objects, 1,377 artifact objects, no
  pending/orphan objects, all manifest hashes valid, and SQLite
  `quick_check=ok`. Gandalf's full suite passed 730 tests plus 4 subtests, 13
  Bunny tests, generated-artifact/DNS/inventory checks, Ansible syntax, and
  production-profile lint. The private Caddy-to-BlobForge route and container
  health pass. The private runbook publish preview could not contact Outline
  because `OUTLINE_API_TOKEN` is absent in this shell.
- **Cutover completion:** After explicit approval, restarted the shared Caddy
  service and verified public TLS/API health, HTTP 404 for public SCIM, the OIDC
  redirect to Authentik, and continued health of an existing Citadel endpoint.
  The provisioned `blobforge-admin` SCIM group currently has zero members, so
  interactive login remains deny-by-default until an administrator is added in
  Authentik.
- **Recovery completion:** After explicit approval, applied Citadel's complete
  Restic role. The BlobForge profile's first quiesced snapshot succeeded in 55
  seconds, committed 715 new repository bytes through existing deduplication,
  and restarted a healthy coordinator. Its isolated restore recovered 32.165
  GiB in 201 seconds and passed restored SQLite `quick_check`. Daily backup and
  weekly restore-test timers are enabled, both profile metrics report success,
  current SQLite remains healthy, and the public API canary passes.

## 2026-08-27 (Self-hosted Root Landing)

- **Objective:** Replace the production root's FastAPI `404 Not Found` response
  with a useful, group-gated browser entry point.
- **Finding:** The self-hosted coordinator implemented OIDC callbacks and APIs
  but intentionally had no replacement for the Bunny management console, so a
  successful Caddy request to `/` correctly reached FastAPI and then fell
  through to its default 404.
- **Actions:** Added a root handler that redirects unauthenticated OIDC browsers
  to `/auth/login` and renders an authorized overview for SCIM-backed sessions
  or client tokens. The page shows coordinator counts and links to OpenAPI,
  snapshot, and recipe endpoints. It is private/no-store, HTML-escapes identity
  and labels, and sets a restrictive CSP plus `nosniff`.
- **Verification:** The full suite passes with 205 tests and 5 subtests,
  including authenticated rendering and unauthenticated OIDC redirect coverage.
  GitHub Actions published revision `6b8aa75`; Gandalf pinned manifest
  `sha256:97f764f71d329c25c0783617595d7ee4b3ec5c586a2e3d481d7612c0ab56f330`
  and deployed it successfully. Production `/` returns HTTP 307 to
  `/auth/login`, the container label matches the intended revision, and health
  remains green. The full Bunny-era file library, worker enrollment, and
  token-management console remains an explicit future feature rather than
  being implied by the landing page.

## 2026-08-27 (Initial SCIM Administrator Reconciliation)

- **Objective:** Resolve the production OIDC callback denial after the initial
  authorized administrator was added to Authentik's `blobforge-admin` group.
- **Finding:** The account was active and already belonged to the Authentik
  access group, while BlobForge still had no corresponding group membership.
  The callback therefore correctly rejected the identity: authorization is
  resolved exclusively from active local SCIM state, not from OIDC claims.
- **Actions:** Inspected the Gandalf OIDC/SCIM reconciliation contract and
  production identity state, then ran the targeted Citadel `blobforge_scim`
  Ansible tag with `blobforge_scim_force_sync=true`. This dispatched an
  Authentik SCIM reconciliation and verified the private backchannel without
  changing the coordinator image or relaxing access control.
- **Verification:** The play completed with no failures and BlobForge's SQLite
  identity tables now contain one member of `blobforge-admin`; the configured
  group maps to the `admin` role. A public GET still returns the intended 307
  redirect to `/auth/login`. Tooling used included `rg`, `sed`, `curl`,
  Ansible, the Authentik Django shell for sanitized read-only checks, and a
  read-only SQLite aggregate query.

## 2026-08-27 (SCIM Root Cause and Browser Error UX)

- **Objective:** Explain why the initial Authentik group grant was not visible
  to BlobForge in real time and replace raw framework errors on browser routes.
- **Production evidence:** Authentik queued the BlobForge membership task at
  `16:04:23.988Z`, processed it at `16:04:24.494Z`, and marked it successful at
  `16:04:24.987Z`. BlobForge received only `GET /scim/v2/Groups/{id}` during
  that task. It received no user creation or group mutation, and OIDC callbacks
  at `16:04:30Z` and `16:04:37Z` correctly returned 403. The forced full sync
  later issued `POST /scim/v2/Users` followed by `PUT /scim/v2/Groups/{id}`.
- **Root cause:** Authentik's filtered-provider membership handler looks up
  existing provider-specific user mappings and returns without mutation when
  none exist. A first access-group addition makes the user newly eligible, but
  that handler does not provision the user first. Its task therefore records a
  successful no-op; the native full-sync fallback is every four hours.
- **Actions:** Added content-negotiated browser exception handling, a friendly
  SCIM authorization explanation with a fresh-login action, and explicit
  handling for expired/reused OAuth codes. `/api/` and `/scim/` retain JSON
  errors. Documented the filtered-SCIM caveat and a pending ordered-provisioning
  fix that preserves least-privilege directory scope.
- **Verification:** The focused server suite passed 8 tests. The full suite
  initially inherited this shell's production coordinator environment and
  produced 16 unrelated network failures; rerunning with those two variables
  explicitly unset passed 207 tests and 5 subtests. Read-only diagnosis used
  `rg`, `sed`, Ansible, Authentik's task database and installed source, and
  filtered systemd journal queries.
- **Deployment:** GitHub Actions run `33096702245` passed its test and
  multi-architecture server-image jobs. Gandalf commit `76d21898` pins and
  deployed BlobForge revision `41adf6d` at manifest
  `sha256:86fb528eec6bdddae0119c866496f9f3222f77c0e29f9749caf7aa297b90fe71`.
  Citadel reported a healthy container at that exact revision. Public probes
  confirmed a no-store/CSP HTML 404 and a recoverable HTML 400 for an invalid
  OIDC callback; the health endpoint remains green.

## 2026-08-30 (Citadel Hosted-Provider Production Canaries)

- **Objective:** Close the hosted-worker production-readiness issues, deploy
  quota-fenced Mistral and Datalab workers, convert a bounded set of priority
  rulebooks, and prove the enlarged recovery boundary.
- **Implementation:** Published hosted image
  `sha256:89f3e7202ba280a3e66938054ce492bf274c6ca7953e41f1bfb74c89288c3256`
  after adding an unprivileged writable uv cache, pypdf 6.14.2 preflight,
  explicit-only recipe claiming, and lifecycle-aware PID-1/subprocess shutdown.
  Gandalf deploys separate concurrency-one provider Quadlets, dedicated worker
  credentials and API keys, immutable 3-request/30-page quota windows,
  per-job ceilings, and provider caches inside `/srv/blobforge`.
- **Canaries:** Replayed the eight-page *Shadows and Mirrors* checkpoints for
  both providers at zero request/page/money exposure. Then converted the full
  nine-page *Massive Monsters* rulebook through both exact recipes. Datalab
  committed one request/nine pages and USD 0.07 billed after reserving its USD
  0.10 ceiling. Mistral committed one request/nine pages and USD 0.036 list
  price; billed cash and credits remain unknown. All jobs completed at retry
  zero, all four MDAFs independently validated, and every artifact has exact
  page mappings and recipe provenance. The two original legacy artifacts were
  not changed or replaced.
- **Verification:** Production ended at 1,381 artifacts, 1,377 `done` jobs,
  431 `todo` jobs, four committed quota rows, zero active provider workers,
  and SQLite `quick_check=ok`. Public `/api/v1/health` reports the SQLite/filesystem
  backend healthy. Mistral and Datalab each stopped with result success,
  `ExecMainStatus=0`, and `NRestarts=0`. The post-canary quiesced Restic backup
  completed in 16 seconds; the isolated full restore test completed in 221
  seconds with result success. Gandalf's mandatory suite passed 758 tests plus
  4 subtests and 13 Bunny tests before each staged steady-state change.
