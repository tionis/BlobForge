# Routing and Exact-recipe Workers

BlobForge now has an advisory, versioned routing policy and an isolated MDAF
worker path. Both are deliberately canary-scoped: the evaluated Mistral
candidate is useful enough to test on real jobs, but its managed model alias,
provider billing evidence, hidden holdout, and production rollback canary are
not yet frozen.

## Policy contract

`blobforge/routing/pdf-rulebooks-v2.json` is the current canonical policy
document. Revision 1 remains frozen on Mistral wiki-v2 rather than being edited
in place. The canonical BLAKE3 digest and integer revision accompany every decision. The
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
- Mistral wiki-v3 is status `canary`, so the caller must opt into canary use;
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
  --confirm-api-rights \
  --allow-canary
```

Pass `--apply-job <source-key>` plus coordinator credentials to apply a plan.
The coordinator does not trust the client's chosen digest: it recomputes the
policy decision from the supplied features and its authoritative media type,
requires an active worker advertising the exact recipe, queues/selects it, and
stores the actor, full feature snapshot, policy revision/digest, estimate,
recipe, status, and rationale in one `job.route` audit event. The management UI
still offers the lower-level manual exact-recipe selection for exceptional
operator decisions.

## Mistral wiki-v3 lifecycle recipe

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
one worker process. The hosted catalog contains the PDF Mistral wiki-v3 and
Datalab accurate wiki-v1 canaries. They run under separate provider-account
credentials and quota ledgers. Adding audio later means another exact `AdapterRecipe`;
the claim/dispatch loop does not need to become media-specific.

Mistral and Datalab advertise `claim_unassigned=false`. The coordinator stores
that capability and only offers those workers jobs already assigned to the
exact recipe. This protects the retained unassigned migration queue when a
provider worker starts. Registered capabilities are authoritative for recipe,
media/input kinds, provider account, and assignment mode; claim payloads may
narrow but cannot broaden them.

Run a bounded hosted canary worker with:

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

The wiki-v3 worker advertises both source and artifact input. Source claims run
the isolated hosted adapter; artifact claims download the exact immutable
parent MDAF and run `blobforge reprocess` without credentials or provider
access. Capabilities and claims are input-kind constrained, so a source-only
worker cannot accidentally consume an upgrade job. Bulk scheduling and its
preview/execute contract are documented in `recipe_lifecycle.md`.

## Promotion gates

Before this becomes the default or is broadly deployed:

- run the hidden holdout and a small coordinator production canary;
- confirm provider billing/credit accounting and response-cache backup;
- document rollback to the retained legacy artifact;
- address or explicitly accept the managed model alias/checkpoint limitation;
- add the exact local/privacy recipe rather than routing to a backend alias.
