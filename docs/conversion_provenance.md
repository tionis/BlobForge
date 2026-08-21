# Recipe-aware conversion provenance

BlobForge separates the identity of a source document from the identity of a
conversion artifact. The source PDF remains content-addressed by its SHA-256
`document_hash`. A conversion is additionally identified by a 64-character
`conversion_recipe_digest`, allowing outputs from Marker 1, Marker 2, or a
future output schema to coexist without overwriting one another.

## Recipe identity

`blobforge/conversion_identity.py` serializes the recipe as canonical JSON and
hashes it with SHA-256. Recipe version 1 includes:

- the recipe schema version;
- the conversion engine and its compatibility generation;
- BlobForge's output schema identifier;
- configured Surya model/checkpoint identifiers; and
- explicit output-affecting options.

Exact package patch versions are deliberately not part of the recipe unless
they change one of those compatibility inputs. This avoids fragmenting the
artifact cache for a packaging-only update. Any new converter option that can
change Markdown or assets must be added to the recipe before that option is
used in production.

Recipe numbers are limited to safe integers so Python and JavaScript emit the
same canonical JSON. Fractional output settings must be encoded as strings.

## Exact provenance

Every new `info.json`, worker registration, and coordinator artifact record
also stores diagnostic provenance:

- exact BlobForge, Marker, Surya, pdftext, Torch, Transformers, and Pillow
  versions;
- the BlobForge build or Git revision when discoverable;
- the complete recipe and its digest;
- Python implementation/version and host OS/architecture; and
- selected inference backend, whether an external server is used, and the
  llama.cpp executable basename.

The external inference URL itself is never stored because it may contain
internal topology or credentials. Provenance describes what produced an
artifact; unlike the recipe, it does not determine cache compatibility.

## Coordinator lifecycle

A worker advertises its recipe when it claims work. An unbound todo job becomes
bound to that digest atomically with the lease. Failed/released retries retain
the binding, and workers advertising a different recipe cannot claim it.
Claims without a valid recipe digest are rejected, preventing an outdated
worker from creating an untracked result after this migration; already-active
legacy leases can still finish against their legacy object key.

Recipe-aware output objects use:

```text
{prefix}store/out/{document_hash}/{recipe_digest}.zip
```

On successful completion, the `conversion_artifacts` table records the object
key, recipe, provenance, producing worker, archive size, and timestamp under a
`(file_hash, recipe_digest)` primary key. Completion rejects a recipe digest
that differs from the lease's bound recipe.

Existing outputs remain at `{prefix}store/out/{document_hash}.zip`. Migrated
done jobs initially retain a null recipe digest and are exposed using the
reserved all-zero legacy recipe identifier; no guess is made about their
converter provenance. Before a legacy job is retargeted, the coordinator
persists that legacy artifact row, so it remains selectable after newer recipes
are queued or promoted. The object store maps the reserved identifier back to
the original hash-only key rather than a recipe subdirectory.

## Selecting and comparing recipes

The hash-only job and download APIs continue to refer to the currently selected
artifact, preserving hydration and existing clients. Recipe-aware APIs are:

- `GET /api/v1/jobs/{hash}/artifacts` lists all retained artifacts and their
  provenance.
- `POST /api/v1/jobs/{hash}/download-url` accepts an optional
  `recipe_digest` to download a specific artifact.
- `POST /api/v1/jobs/{hash}/convert` with a `recipe_digest` explicitly queues
  that recipe. If the artifact already exists, it selects it without another
  conversion. A processing job cannot be retargeted.

The conversion request endpoint uses the same trusted ingestion/admin-token
authorization as enqueueing. A requested digest remains queued until a worker
advertising that exact recipe is available. Selecting or completing an artifact
advances the done watermark so hydration can rediscover the document.

This provides the intended Marker 2 evaluation workflow: deploy a separate
experimental worker, obtain its advertised recipe digest, request that recipe
for a representative corpus, and compare both retained artifact sets. Promoting
the new recipe changes only the selected artifact; it does not destroy the
Marker 1 result.

Coordinator backup format version 2 includes `conversion_artifacts` and the
new nullable `jobs.recipe_digest` column.
