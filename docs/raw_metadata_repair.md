# Raw Metadata Repair

## Purpose

`blobforge repair-metadata` restores BlobForge's expected raw-PDF S3 metadata when
object bodies survived a migration but custom metadata did not.

This is specifically useful after provider-to-provider copies done with tools that
do not preserve custom S3 metadata, such as some `rclone sync` workflows.

## Source Of Truth

The command uses the manifest (`registry/manifest.json`) as the repair source.

For each manifest entry, it reconstructs:

- `original-name`: basename of the first manifest path
- `tags`: JSON-encoded manifest tags
- `size`: manifest size in bytes as a string

## Repair Strategy

For each target hash:

1. Confirm the hash exists in the manifest.
2. Confirm the raw object exists at `store/raw/<hash>.pdf`.
3. Read existing raw-object metadata.
4. Build the expected BlobForge metadata from the manifest entry.
5. Rewrite object metadata in place via same-key server-side copy.

The metadata rewrite preserves unrelated existing metadata keys such as
provider-specific fields like `src_last_modified_millis`.

## Command Behavior

Default behavior repairs only missing BlobForge keys.

Use `--force` to also overwrite mismatched BlobForge metadata values.

Use `--dry-run` to preview the repair plan without writing.

Examples:

```bash
# Preview repairs for all manifest entries
blobforge repair-metadata --dry-run

# Repair all missing raw-object metadata
blobforge repair-metadata

# Repair specific hashes only
blobforge repair-metadata <hash1> <hash2>

# Force metadata back to manifest values even if keys exist but differ
blobforge repair-metadata --force <hash>
```

## Limitations

- If a raw PDF is missing, this command does not recreate it.
- If the manifest is already wrong, the repair will faithfully write the wrong data.
- The command restores BlobForge metadata only; it does not rebuild the manifest.
