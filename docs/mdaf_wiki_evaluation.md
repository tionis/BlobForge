# MDAF rulebook import evaluation

Date: 2026-09-05

## Implemented repair and end-to-end results

The new opt-in `mistral-ocr-4.1-wiki-v4.json` recipe uses normalization profile
`wiki-v3`. Earlier frozen recipes are unchanged. It retains primary Markdown
and native response bytes while publishing an alternative outline with level
two major sections, level three-and-deeper topics, and explicit front matter.
Numbered chapter openers take precedence; otherwise boundaries require contents
membership and large provider-typed title geometry. Duplicate candidates need
unique observed page-label evidence. Insufficient evidence falls back with a
diagnostic in `extensions/dev.tionis.blobforge/hierarchy.json`.

| Book | Chapter notes | Topic notes | Root source bytes | Assets |
| --- | ---: | ---: | ---: | ---: |
| London Falling | 7 | 325 | 0 | 32 |
| Shadowrun 5E Core | 17 | 1,915 | 0 | 330 |
| Storypath Ultra | 9 | 771 | 0 | 55 |

Counts include root and front matter. All six actual imports had complete,
non-overlapping source-span coverage, byte-identical assets, and existing
targets for every generated navigation and citation link. Chapter imports
resolve 0/152/15 plain page citations; finer topic imports resolve 0/16/2.
Coarse page mappings often overlap multiple topics, so those references stay
unchanged with diagnostics. Other rewritten links include existing heading
links, which are not included in these citation counts.

Only complete singular parenthetical `(p. N)`, `(page N)`, and `(see p. N)`
mentions with a unique observed Arabic footer label are emitted as references.
External/comma-qualified references, ranges, missing labels, and duplicate
labels are not guessed. Vulcan protects code, existing links, and ambiguous
output placements. Duplicate authored headings and mutable-model provenance
remain visible warnings: this repair does not manufacture missing evidence.

This remains heuristic structure recovery, not a guarantee of a publisher's
exact TOC: large fiction titles can become peers, unrecognized sections can
remain within the preceding chapter, and non-Arabic/unobserved labels are not
resolved. Review the report and import preview before choosing chapter or topic
depth. The second Storypath filename has the same original logical identity
and need not be imported twice. Original artifacts must remain archived.

### Local replay and rollout

From the Blobforge checkout, replay without contacting the provider:

```sh
uv run blobforge reprocess original.mdaf \
  --recipe blobforge/recipes/mistral-ocr-4.1-wiki-v4.json \
  --output upgraded.mdaf --source-name 'Known Book.pdf'
```

`--source-name` is optional; use it only for a known recovered filename, not an
inferred title. It changes display metadata and records the override in
provenance. Older lineage is retained under identity-scoped extension paths.
Existing output files are never overwritten.

With the repaired Vulcan, preview and then apply to a new destination:

```sh
vulcan artifact import upgraded.mdaf --destination Books/Known \
  --hierarchy outline --through-level 2 --dry-run --output json
vulcan artifact import upgraded.mdaf --destination Books/Known \
  --hierarchy outline --through-level 2 --no-commit --output json
```

Use level three for finer topics. New hosted runs can explicitly select
`blobforge evaluate mistral-wiki-v4` or the existing recipe-worker command with
`--mistral-recipe v4`. Existing quota, rights-confirmation, cache, and exact-job
recipe requirements still apply. The worker default remains v3. Publish/deploy
and coordinator recipe promotion are separate operator actions; none ran here.

## Observed behavior

Three distinct full-book BlobForge 0.4.0 Mistral wiki-v3 artifacts were inspected
with Vulcan. Two supplied Storypath filenames contain the same logical artifact.
All artifacts validate, with an honest mutable-model warning. No provider was
contacted. The private books and their identities are not test fixtures.

| Book | Assets | Default imported notes, including root | Source bytes remaining in root | Duplicate heading diagnostics |
| --- | ---: | ---: | ---: | ---: |
| London Falling | 32 | 184 | 164,894 / 508,882 | 52 |
| Shadowrun 5E Core | 330 | 869 | 1,535,353 / 2,568,350 | 190 |
| Storypath Ultra | 55 | 712 | 60,672 / 972,625 | 90 |

Both Markdown and outline authority produced the same counts and span sizes.
The supplied outline levels match the Markdown ATX markers. Vulcan's default
selects only level two, creating a flat collection while unselected level-one
sections remain in the root. Selecting levels one and two indiscriminately
would yield 345, 1,927, and 790 notes respectively, which does not establish
better semantic organization.

A complete London import into a disposable vault preserved 32 assets
byte-for-byte. All 32 Markdown image links and 183 generated navigation links
resolved to existing files. Notes carry artifact identity, Markdown byte spans,
and source page intervals. The imported tree does not copy native evidence or
provenance sidecars; retain the original MDAF.

Every sample has zero normalized source references. Page mentions in prose are
not links. The Mistral renderer deliberately returns `references: []`, and the
shared packager derives `outline.json` using `markdown_outline`. These are
capability limitations, not evidence that the underlying OCR lost the prose.

## Source-name repair

The coordinator knows `original_name`, but `RecipeWorker` downloads every PDF
to a private `source.pdf`. `run_converter` previously derived the manifest title
and source name exclusively from that staging path. All inspected books were
therefore titled `source` with source name `source.pdf`.

The worker now passes the display name as an optional, separate packager input.
The packager removes POSIX/Windows directory components, normalizes Unicode NFC,
and strips Unicode control/format characters. Empty/dot-only names fall back to
the staging basename. The supplied name is never used for filesystem access or
passed to a hosted provider. Direct local callers retain the filename fallback.

This repairs lost existing source metadata; it does not change extraction,
normalization, provider cache keys, recipe definitions, or existing artifacts.
Future output identities can differ because their manifest has the corrected
name. No completed job is automatically rerun or replaced. Offline reprocessing
currently preserves the parent's name, so it cannot recover a title already lost
from a historical artifact without an explicit metadata-enrichment input.

## Hierarchy follow-up

Use chapters with nested topic notes as the initial candidate, subject to user
preference and whole-book review. This is not implemented by the source-name fix.

London's native contents page lists six adventure/handout groups. Storypath and
Shadowrun mix tables, heading-like TOC entries, and printed page numbers. Native
responses retain block geometry, but do not supply a corrected document-wide
chapter tree. A blanket heading shift or font-size guess is insufficient.

A new recipe should:

1. Retain explicit TOC evidence and uniquely align entries to body headings,
   excluding the TOC occurrence itself. Handle multiline titles and repeated
   labels conservatively; report unmatched/ambiguous entries.
2. Build an alternative aligned outline with chapters and their topics, while
   preserving primary Markdown, tables, assets, and unclassified content.
3. Establish printed-page-to-source-page correspondence before interpreting
   page references. Page index alone does not establish a printed page label.
4. Preserve final UTF-8 spans, extraction evidence, and immutable lineage; use
   the existing offline reprocessor for a new allowlisted recipe version.
5. Compare root size, tiny notes, duplicate names, child placement, and source
   coverage with Vulcan, then inspect actual reading order and navigation.
   Structural counts alone do not establish quality.

## Deployment

The source-name repair is in the shared Python packager and recipe worker. Ship
it through the existing tested hosted-worker image workflow, pin the resulting
image in the deployment repository, and drain active work before restarting.
Verify the next newly converted artifact's title and source name. No coordinator
schema migration, quota change, or paid reconversion is required for rollout.

Do not queue historical books for paid source conversion merely to repair their
title or hierarchy. A later explicit derivative workflow should reuse their
retained native responses and preserve the parent artifacts.
