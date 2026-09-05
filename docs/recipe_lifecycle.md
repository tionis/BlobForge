# Recipe Lifecycle and Offline MDAF Upgrades

Status: implemented for the Mistral OCR wiki family

Date: 2026-08-30

## Decision

An MDAF is immutable. Improving Markdown, tables, list recovery, headings,
assets, source maps, or other post-processing never edits an existing archive.
BlobForge creates a new self-contained MDAF, records the old artifact in
`derived_from`, and keeps both identities available for comparison and
rollback.

Recipe and MDAF versions answer different questions:

- MDAF `version` describes the archive contract. A recipe upgrade does not
  require MDAF v2 while the v1 schema can represent the result honestly.
- `lifecycle.recipe_version` describes the conversion family. Its semantic
  version major is the expensive extraction compatibility boundary.
- the exact canonical recipe BLAKE3 remains the job and artifact identity.
  Semver communicates compatibility; it never replaces the digest.

Within one recipe major, retained native extraction evidence is sufficient to
re-run every supported post-processing release without contacting the
provider or re-reading the PDF. An incompatible extraction/model/request
change must advance both `recipe_version` and `extraction.major`. BlobForge
then refuses offline reprocessing and requires a new full conversion. A
same-major lifecycle recipe must permit automatic upgrades; a target still
names every accepted predecessor digest explicitly, so compatibility is never
inferred from version numbers alone.

## Artifact evidence contract

New lifecycle-aware artifacts embed the canonical recipe at:

```text
extensions/dev.tionis.blobforge/recipe.json
```

The recipe declares:

- family and semantic recipe version;
- extraction version, exact extraction-recipe digest, and required native
  rendition paths;
- whether replacing extraction requires a recipe major;
- post-processing version/profile; and
- the exact source recipe digests eligible for automatic upgrade.

Conversion provenance uses separate `document-extraction` and
`document-normalization` activities. Native responses are outputs of the
extraction activity; Markdown, assets, mappings, outline, and the embedded
recipe are outputs of post-processing. This separation is essential: changing
normalization must not be represented as a second paid OCR request.

An offline derivative carries the retained native members byte-for-byte and
also embeds:

```text
extensions/dev.tionis.blobforge/previous-recipe.json
extensions/dev.tionis.blobforge/parent-info.json
extensions/dev.tionis.blobforge/parent-provenance.json
```

The derivative remains usable without the parent archive. Its new provenance
records `retained-extraction-evidence` followed by `document-normalization`,
sets `network_access` to false on both activities, and binds the parent,
source recipe, extraction recipe, and target recipe identities.

## Upgrade algorithm

`blobforge reprocess` validates the parent first and fails before publication
unless all of these are true:

1. the target is a valid lifecycle recipe;
2. the source recipe is embedded or resolves by exact digest from the frozen
   local recipe registry;
3. the target explicitly allowlists that source digest;
4. recipe family, recipe major, and exact extraction recipe are compatible;
5. every required native member is present; and
6. a deterministic local handler exists for the family.

The command never accepts provider credentials, never calls a provider, and
refuses to overwrite an existing path:

```bash
uv run blobforge reprocess old.mdaf \
  --recipe blobforge/recipes/mistral-ocr-4.1-wiki-v3.json \
  --output upgraded.mdaf
```

Publication is atomic through the shared MDAF builder. The output is validated
before success is reported. Repeating the operation with identical parent,
recipe, and BlobForge version produces the same logical and physical artifact.

Direct conversion and reprocessing through the same target recipe are allowed
to have different artifact identities. They contain the same normalized
content and native response, but the derivative additionally carries parent
lineage and recovery evidence.

## Versioning examples

| Change | Recipe version | Existing native evidence |
| --- | --- | --- |
| deterministic footer cleanup correction | `1.2.0` to `1.3.0` | automatically reprocess allowlisted predecessors |
| packaging-only deterministic correction | `1.2.0` to `1.2.1` | automatically reprocess allowlisted predecessors |
| provider model/request or extraction algorithm changes | `1.x` to `2.0.0` | cannot upgrade; run expensive extraction again |
| MDAF schema can no longer express required semantics | MDAF v1 to v2 and an exact new recipe | explicit format migration/conversion |

Recipe and routing-policy JSON are immutable once used. A new default creates
a new recipe file and a new routing-policy revision; it never edits the old
documents in place.

## Coordinator scheduling

The coordinator uses the existing one-active-job-per-source row with an
explicit input contract:

- `input_kind=source` leases the original media object for a full conversion;
- `input_kind=artifact` binds `input_artifact_id` and
  `parent_recipe_digest` to one immutable MDAF;
- requesting a normal conversion clears the artifact-input fields; and
- all recipe artifacts remain independently addressable by exact recipe
  digest regardless of the active job selection.

Worker capabilities advertise `input_kinds`. A source-only worker cannot claim
an artifact job even when it advertises the same target recipe. On an artifact
claim the coordinator signs a download for the exact artifact row rather than
the PDF source. The worker runs the offline reprocessor, uploads through the
existing fenced lease URL, and reports `execution_mode=artifact`.

The coordinator independently validates every `mdaf/v1` worker result before
publication. It checks the reported logical identity, embedded lifecycle
recipe, leased target digest, and—on artifact jobs—the exact parent's logical
identity in `derived_from`. Invalid pending output is discarded and cannot
become an artifact record. Parent and target artifacts coexist under their
respective recipe identities.

Bulk planning is read-only by default. It validates lifecycle compatibility
once, reports eligible, already-present, and currently-processing counts, and
can optionally restrict the plan to explicit source keys. `--execute` updates
all eligible job rows in one SQLite transaction and resets retry state for the
new operation:

```bash
uv run blobforge reprocess-plan \
  --source-recipe blake3:OLD \
  --target-recipe blake3:NEW

uv run blobforge reprocess-plan \
  --source-recipe blake3:OLD \
  --target-recipe blake3:NEW \
  --priority 4_low \
  --execute
```

The management console exposes the same guarded preview/queue flow from the
recipe page and from each individual artifact. Planning and execution are
separate audited actions. Jobs already processing are reported but never
retargeted, and existing target artifacts are counted without being queued.

## Automatic compatible release following

The coordinator follows the newest compatible release offered by a registered
worker during its claim transaction. No per-upgrade opt-in is needed. Release
ordering uses the lifecycle semantic version, not filenames, backend aliases,
or the numerical suffix in a display name. The target must be enabled, support
the source media/input kind, explicitly allow the old digest, and retain the
same family, recipe/extraction major, and exact extraction-request digest.
Equal-version competing digests are not automatically chosen.

- Completed artifacts become offline artifact-input jobs with an exact immutable
  parent. Existing target artifacts are selected without another conversion.
  Missing native bytes fail validation; they never trigger source OCR.
- Pending, already-assigned source jobs move to compatible post-processing
  releases only within the same provider account and without any non-released
  provider reservation for that source. Retry history, priority, tags, and quota
  backoff are preserved. These jobs still require their originally authorized
  extraction and remain subject to all normal worker spending/rights gates.
- Active, failed/dead, unassigned, unsupported, and incompatible jobs stay put.
  A source with a committed or unsettled purchase requires checkpoint recovery
  before automatic retargeting; changing a recipe is not a recovery shortcut.
- A finished old worker result is picked up on a subsequent compatible worker
  claim. A failed derivative is not repeatedly requeued by this policy.

Each change emits a `job.recipe-upgrade` audit event with old/target recipe and
input kind. Explicit old artifact retrieval remains available for rollback;
selecting an old completed recipe does not permanently pin it against future
automatic following. New expensive extraction, including an extraction-digest
change without a major bump, always requires explicit operator action.

This is coordinator scheduling policy, not a change to the MDAF v1 format or
the immutable recipe JSON/SPEC contract.

## Current scope

Mistral wiki recipe `1.2.0` is the first lifecycle-aware family. It can upgrade
the frozen wiki-v1 and wiki-v2 recipes from their retained
`renditions/ai.mistral/ocr-response.json`. Other backends must add an equally
strict retained-evidence renderer before becoming upgradeable.

Local archives and coordinator-managed collections can both be upgraded
without retrieving the source PDF. Other backend families still need their own
strict retained-evidence renderers before their recipes may advertise artifact
input support.
