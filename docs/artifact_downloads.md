# Finding and downloading retained artifacts

The job UI displays the immutable recipe's lifecycle version and post-processing
profile separately from the artifact format (`mdaf/v1`). For example, the
wiki-v5 release declares recipe **1.4.0** and profile **wiki-v4**. These are
different version axes, not interchangeable names. Recipes without lifecycle
metadata explicitly show that the version is unavailable; the digest remains
the exact identity. Opening job details refreshes the recipe catalog.

Job search matches all query words across source names, source keys, decoded
paths and tags, case-insensitively with Unicode normalization. Spaces, dots,
hyphens and underscores are interchangeable separators. Existing state,
priority and recipe filters still apply. Search responses cannot overwrite a
newer query. `%` is literal, not a SQL wildcard.

Browser artifact downloads use the source basename with `.mdaf` for MDAF v1,
or `.zip` for legacy archives. Unicode names are sent using `filename*` with an
ASCII fallback. Directory components, control characters and unsafe portable
filename characters are removed or replaced. Missing names fall back to the
source key. Storage hashes, signed capabilities and retained artifact bytes
are unchanged; no MDAF SPEC or recipe revision is required.

## CLI

```sh
# Download one PDF's selected retained MDAF, preserving the source name.
blobforge download "My Rulebook.pdf" --mdaf -o ./references/

# Preview then download all matching retained MDAFs for a wiki corpus.
blobforge download --search "rulebook" --mdaf -o ./references/ --dry-run --json
blobforge download --search "rulebook" --mdaf -o ./references/ --json

# Source keys and exact historical recipe selection remain supported.
blobforge download SOURCE_KEY --recipe-digest RECIPE_DIGEST -o ./book.mdaf
```

Filename lookup and bulk search require administrator access. Source-key lookup
continues using the ordinary artifact API. A positional filename must identify
one source; an exact filename wins over partial matches, but duplicate exact
names require a source key. `--search` explicitly selects all matching sources,
including sources with retained results while another conversion is queued.

Artifact selection uses the job's selected recipe, or its sole retained artifact;
ambiguous histories require `--recipe-digest`. `--mdaf` rejects a single non-MDAF
selection and skips such sources in a bulk search. It never falls back to another
recipe merely to find an MDAF. Nothing is converted, queued or upgraded by this
command. JSON reports exact recipes, output paths and skipped sources.

Output directories must exist. The entire plan is checked for name collisions
and existing files before downloading; duplicate basenames require separate
commands with explicit filenames. Files are staged beside the destination and
published atomically. Existing files are protected unless `--force` is supplied;
symlinks are refused. Failed downloads leave no partial destination file. If a
later bulk download fails, earlier completed files remain and are reported.
