# Marker 1.10.2 composite recipe

## Decision

The local born-digital PDF baseline is one immutable composite recipe:
`marker-1.10.2-enriched-v1`. It runs pinned `marker-pdf==1.10.2`, retains the
raw Marker Markdown and sanitized Marker metadata, then applies the frozen
`pdf-enrichment/v1` Poppler evidence and alignment logic before packaging the
MDAF. The final Markdown is unchanged by enrichment; source mappings and the
outline are calculated against its final UTF-8 bytes.

The recipe uses lifecycle schema v3. Marker extraction is the expensive major
version boundary, while PDF enrichment is versioned post-processing. A future
compatible alignment improvement can therefore produce an artifact derivative
from retained native evidence. A Marker/model/output change requires a new
extraction major and must not silently upgrade existing artifacts.

The recipe retains these native members:

- `renditions/com.datalab.marker/raw.md`;
- `renditions/com.datalab.marker/metadata.json`;
- `renditions/org.freedesktop.poppler/pdf-evidence.json`;
- `extensions/dev.tionis.blobforge.pdf-enrichment/report.json`.

Marker/Surya model aliases are still provider-mutable. The artifact records
that limitation explicitly; promotion beyond the current compatibility tier
still requires immutable model checkpoint evidence.

## Coordinator installation and legacy assignment

The coordinator installs the recipe definition at startup even when no Marker
worker is online. This makes recipe filtering and explicit assignment stable
and avoids using worker presence as configuration storage.

Startup also performs an idempotent, transactional assignment for imported
legacy sources that satisfy every condition below:

- PDF source input;
- current recipe is null;
- state is `todo`, `failed`, or `dead`;
- tags contain both `legacy-import` and `metadata-unavailable`.

These are the raw-only sources for which the old system has no artifact. The
assignment never overrides an administrator's Mistral/Datalab selection and
never changes the 1,377 completed legacy jobs, whose job recipe and artifact
recipe continue to identify the immutable enrichment artifact they actually
contain. A nonzero startup assignment is recorded once in the audit log.

The Marker worker advertises the exact composite digest with
`claim_unassigned=false`; all work therefore remains an explicit coordinator
decision even though the legacy migration supplies that decision in bulk.
