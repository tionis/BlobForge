# Work Log

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
