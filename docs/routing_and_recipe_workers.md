# Routing and Exact-recipe Workers

BlobForge now has an advisory, versioned routing policy and an isolated MDAF
worker path. Both are deliberately canary-scoped: the evaluated Mistral
candidate is useful enough to test on real jobs, but its managed model alias,
provider billing evidence, hidden holdout, and production rollback canary are
not yet frozen.

## Policy contract

`blobforge/routing/pdf-rulebooks-v1.json` is the canonical policy document.
Its canonical BLAKE3 digest and integer revision accompany every decision. The
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
- Mistral wiki-v2 is status `canary`, so the caller must opt into canary use;
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

## Mistral wiki-v2 recipe

Recipe
`blake3:bdd3e060e88f64277834245a42528a54b6b077774123c3806bdd827cf8ea3026`
extends wiki-v1 with evidence-backed list cleanup. It removes a decorative
`◆`, `♦`, `❖`, `•`, or `·` only when the provider already emitted a Markdown
list marker. It also recovers a run of at least two consecutive provider-typed
text blocks whose first visible character is one of those glyphs. A lone block,
inline mechanics such as `At ♦`, and headings such as `• TO ••` remain intact.
The rules do not globally replace `Y`; Marker font/layout recovery is a
separate pending problem.

The cached Storypath response produced validated MDAF
`blake3:aedfe70488c3a376371e64e368dd51b2c3e224d1cf8aa4cea8ad1a23e30e4f0d`.
It removed 20 redundant list decorations, recovered 10 list items, preserved
all inspected inline mechanics/headings, and made no provider request.

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
one worker process. The only deployable catalog entry today is the PDF Mistral
wiki-v2 canary. Adding audio later means adding another exact `AdapterRecipe`;
the claim/dispatch loop does not need to become media-specific.

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

## Promotion gates

Before this becomes the default or is broadly deployed:

- run the hidden holdout and a small coordinator production canary;
- confirm provider billing/credit accounting and response-cache backup;
- document rollback to the retained legacy artifact;
- address or explicitly accept the managed model alias/checkpoint limitation;
- add the exact local/privacy recipe rather than routing to a backend alias.
