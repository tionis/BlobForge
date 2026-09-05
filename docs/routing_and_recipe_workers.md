# Routing and Exact-recipe Workers

BlobForge has a versioned routing policy and an isolated MDAF worker path.
Following operator review of the nine-book offline evaluation, Mistral wiki-v5
is the default worker recipe and the production candidate in policy revision 3.
This promotes post-processing, not a new OCR purchase or broader source scope.
The managed provider model alias remains a reproducibility limitation; retained
native responses remain the replay boundary. Deployment status is separate from
these code defaults (see rollout below).

## Policy contract

`blobforge/routing/pdf-rulebooks-v3.json` is the current canonical policy
document. Revisions 1 and 2 remain frozen on wiki-v2 and wiki-v3 respectively.
The canonical BLAKE3 digest and integer revision accompany every decision. The
resolver accepts media type, source class, native-text ratio, language, layout
class, complex-table/equation flags, quality tier, external-processing
authorization, page count, and a hard cost ceiling. It either returns one exact
recipe digest or a fail-closed list of reasons.

The first policy is intentionally narrow:

- only born-digital PDF pen-and-paper rulebooks with at least 80% usable native
  text are eligible;
- English and undetermined-language inputs are evaluated; other languages are
  not silently assumed equivalent;
- equation-heavy and unknown-layout documents have no route;
- hosted processing requires explicit rights confirmation and a sufficient
  per-document spend ceiling;
- Mistral wiki-v5 requires no canary opt-in; old policies retain their gates;
- privacy/local-only routing returns no recipe until an exact, model-pinned
  local MDAF recipe passes its remaining gates.

Manual recipe overrides are constrained to candidates declared by the same
policy revision. They cannot bypass applicability, rights, cost, or canary
checks. This is preferable to treating an override as permission to violate a
data-handling constraint.

An eight-page Storypath example is read-only and does not enqueue work:

```bash
uv run blobforge route-plan source.pdf \
  --language en \
  --max-cost-usd 0.04 \
  --confirm-api-rights
```

Pass `--apply-job <source-key>` plus coordinator credentials to apply a plan.
The coordinator does not trust the client's chosen digest: it recomputes the
policy decision from the supplied features and its authoritative media type,
requires an active worker advertising the exact recipe, queues/selects it, and
stores the actor, full feature snapshot, policy revision/digest, estimate,
recipe, status, and rationale in one `job.route` audit event. The management UI
still offers the lower-level manual exact-recipe selection for exceptional
operator decisions.

## Historical Mistral wiki-v3 lifecycle recipe

Recipe
`blake3:3f504116b8747b311f07310ea48b53eddaf4a37330ffe6c29e015f06d4185139`
preserves wiki-v2's evaluated output while adding the lifecycle contract,
embedded canonical recipe, and split extraction/post-processing provenance.
It removes a decorative
`◆`, `♦`, `❖`, `•`, or `·` only when the provider already emitted a Markdown
list marker. It also recovers a run of at least two consecutive provider-typed
text blocks whose first visible character is one of those glyphs. A lone block,
inline mechanics such as `At ♦`, and headings such as `• TO ••` remain intact.
The rules do not globally replace `Y`; Marker font/layout recovery is a
separate pending problem.

The cached Storypath response produced direct validated v3 MDAF
`blake3:1cb473b433c901cd2b5259ead4c309ade7b2605459dc06299c52d8fbe74997a5`.
Offline upgrading the prior wiki-v2 artifact produced derivative
`blake3:e984145a8dab8e737134395b1a8d92ced890885f09b3b04ea35b4d9028c917eb`.
Both preserve the paid native response byte-for-byte and make no provider
request. Normalization removed 20 redundant list decorations, recovered 10
list items, and preserved all inspected inline mechanics and headings.

## Worker model

`blobforge recipe-worker` is separate from the legacy Marker/ZIP worker. It:

1. registers an array of exact `(recipe digest, backend, media types, artifact
   type)` capabilities;
2. accepts the coordinator-selected capability on each fenced lease;
3. downloads into an isolated temporary directory with a media-specific input
   suffix;
4. executes the corresponding adapter subprocess through the shared MDAF
   builder and validator;
5. renews the lease during conversion, uploads the validated archive, and
   completes with logical identity, recipe, diagnostics, and timing;
6. releases malformed or unknown claims and records adapter failures without
   turning one recipe failure into another recipe's result.

For tagged BLAKE3 capabilities the coordinator recomputes canonical recipe JSON
before registration; a worker cannot attach arbitrary recipe metadata to a
trusted digest. Offline/revoked workers do not satisfy the routing endpoint's
availability gate.

The dispatcher is media-neutral and tests alternate audio/PDF capabilities in
one worker process. The hosted catalog contains the PDF Mistral wiki-v5 default
and Datalab accurate wiki-v1 candidate. They run under separate provider-account
credentials and quota ledgers. Adding audio later means another exact `AdapterRecipe`;
the claim/dispatch loop does not need to become media-specific.

Mistral and Datalab advertise `claim_unassigned=false`. The coordinator stores
that capability and only offers those workers jobs already assigned to the
exact recipe. This protects the retained unassigned migration queue when a
provider worker starts. Registered capabilities are authoritative for recipe,
media/input kinds, provider account, and assignment mode; claim payloads may
narrow but cannot broaden them.

Run a bounded hosted worker with:

```bash
uv run blobforge recipe-worker \
  --coordinator-url https://blobforge.tionis.dev \
  --token "$BLOBFORGE_COORDINATOR_TOKEN" \
  --max-pages 500 \
  --max-cost-usd 2.00 \
  --confirm-api-rights
```

`MISTRAL_API_KEY` is inherited by the isolated adapter but never appears in
recipe JSON, capability metadata, MDAF, or logs. `--cache-only` explicitly
removes the key from the adapter environment and makes cache misses fail. The
response cache must be persistent because it is the purchase boundary and
retry mechanism. GitHub Actions builds this runtime as
`ghcr.io/tionis/blobforge:latest-worker-hosted` from
`Containerfile.hosted-worker`.

For provider-capable source jobs, the dispatcher runs a network-free adapter
probe and obtains a coordinator reservation before conversion. It settles the
attempt as soon as the provider checkpoint is durable, even if MDAF packaging
later fails. Completed cache hits create zero-purchase ledger entries. See
`api_workers_and_quotas.md` for policies, cooldowns, operator overages, and
ambiguous-attempt reconciliation.

The Mistral worker advertises both source and artifact input. Source claims run
the isolated hosted adapter; artifact claims download the exact immutable
parent MDAF and run `blobforge reprocess` without credentials or provider
access. Capabilities and claims are input-kind constrained, so a source-only
worker cannot accidentally consume an upgrade job. Bulk scheduling and its
preview/execute contract are documented in `recipe_lifecycle.md`.

## Wiki-v5 rollout and historical migration

The coordinator now automatically follows compatible releases during registered
worker claims; see `recipe_lifecycle.md`. Manual preview/reprocessing remains
available for inspection and explicitly scoped maintenance. Pending source jobs
follow only when their existing purchase boundary can be preserved, not by
turning completed artifacts into new paid conversions.

The target is `blobforge/recipes/mistral-ocr-4.1-wiki-v5.json`, exact digest
`blake3:6ca8dda0c845605dd969134e208bfea44988f8ca72ff85fceea428359bf41eec`.
See `mdaf_hierarchy_experiments.md` for the evidence and remaining uncertainty.
There is no MDAF format change in this promotion.

1. Deploy verified, pinned coordinator/worker images through the current
   infrastructure definition. Remove any explicit `--mistral-recipe v3/v4`
   override or set it to `v5`; check the registered exact capability. Preserve
   provider ledgers, response-cache storage, and `claim_unassigned=false`.
2. Preview `reprocess-plan` for each compatible v1–v4 parent recipe, targeting
   the digest above. Review explicit source keys, select only one parent per
   source (prefer the newest compatible retained artifact), and skip active
   work/already-existing targets. Do not blindly execute overlapping cohorts.
3. Execute those reviewed source-key lists. The scheduler queues
   `input_kind=artifact`; this worker path invokes the offline reprocessor,
   never the source converter. Missing native evidence is a reported failure,
   not permission to buy OCR. A bounded `--cache-only` migration worker also
   prevents paid extraction if an unrelated source job is claimed.
4. Verify completed target artifacts, selected recipe, preserved parents,
   hierarchy diagnostics, and unchanged provider purchase totals. Inspect a
   small production canary before processing the rest. Originals remain
   explicitly retrievable with `--recipe-digest` for rollback; do not delete
   them or requeue a source conversion to recover the old output.

The 2026-09-05 rollout deployed revision `26ba2f5` through Gandalf `88c566b9`
after correcting the infrastructure upstream and using its managed SSH config.
The quiesced recovery snapshot and scoped Ansible preview/apply passed. All 35
retained Mistral wiki-v3 artifacts completed offline upgrades after a successful
single-artifact canary; original artifacts remain available. Full validation
confirmed unchanged Markdown, native OCR, and 3,746 assets, with no additional
provider purchases. Eight books retained Markdown hierarchy for insufficient
TOC evidence. Run bulk validation in an isolated read-only container rather than
under the coordinator's small memory budget. Legacy Marker and
Datalab artifacts are not compatible parents for this recipe and were untouched.
Existing unconverted source assignments are separate from this offline batch.
Privacy/local routing remains unavailable rather than silently substituting a
hosted recipe.
