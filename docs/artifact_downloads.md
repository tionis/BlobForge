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
