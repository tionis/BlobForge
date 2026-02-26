"""
BlobForge Hydrator - Materialize completed conversions next to source PDFs.

Walks local PDF files, resolves their content hash, checks for completed
conversion archives in S3, and writes local outputs:
- <pdf_stem>.md
- <pdf_stem>.assets/
"""
import os
import shutil
import tempfile
import uuid
import zipfile
from typing import Any, Dict, List, Optional, Set, Tuple

from .config import S3_PREFIX_DONE
from .s3_client import S3Client
from .utils import compute_sha256_with_cache

# Use a bulk done-index scan for larger runs to avoid many per-hash HEAD requests.
DONE_INDEX_THRESHOLD = 200


def discover_pdf_files(paths: List[str]) -> List[str]:
    """
    Expand a list of files/directories into a sorted list of PDF files.
    Directories are scanned recursively.
    """
    pdf_files: List[str] = []

    for path in paths:
        absolute = os.path.abspath(path)
        if os.path.isfile(absolute):
            if absolute.lower().endswith(".pdf"):
                pdf_files.append(absolute)
            else:
                print(f"Skipping {absolute}: Not a PDF file")
            continue

        if os.path.isdir(absolute):
            for root, _, files in os.walk(absolute):
                for filename in files:
                    if filename.lower().endswith(".pdf"):
                        pdf_files.append(os.path.join(root, filename))
            continue

        print(f"Warning: {absolute} does not exist, skipping")

    return sorted(pdf_files)


def _rewrite_markdown_asset_paths(markdown_text: str, assets_dir_name: str) -> str:
    """
    Marker output references "assets/...". Rewrite those references so each
    hydrated markdown file points to its sibling "<stem>.assets/" directory.
    """
    if assets_dir_name == "assets":
        return markdown_text
    return markdown_text.replace("assets/", f"{assets_dir_name}/")


def _write_text_atomic(path: str, text: str) -> None:
    """Write text atomically to avoid partial files on interruption."""
    directory = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".blobforge-hydrate-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def _asset_member_to_relative_path(member: str) -> Optional[str]:
    """
    Convert archive member path "assets/..." to a validated relative path.
    Returns None for non-asset entries or unsafe paths.
    """
    normalized = member.replace("\\", "/")
    if not normalized.startswith("assets/"):
        return None
    if normalized.endswith("/"):
        return None

    relative = normalized[len("assets/"):]
    if not relative:
        return None

    relative = os.path.normpath(relative)
    if relative in ("", ".") or relative.startswith("..") or os.path.isabs(relative):
        return None

    return relative


def _extract_assets_to_staging(archive_path: str, staging_assets_dir: str) -> int:
    """Extract assets from a conversion zip into a staging directory."""
    extracted = 0

    with zipfile.ZipFile(archive_path, "r") as archive:
        for member in archive.namelist():
            relative = _asset_member_to_relative_path(member)
            if relative is None:
                continue

            destination = os.path.join(staging_assets_dir, relative)
            destination_parent = os.path.dirname(destination)
            if destination_parent:
                os.makedirs(destination_parent, exist_ok=True)

            with archive.open(member) as source, open(destination, "wb") as target:
                shutil.copyfileobj(source, target)
            extracted += 1

    return extracted


def _replace_directory(staging_dir: str, target_dir: str) -> None:
    """
    Replace target_dir with staging_dir, preserving a rollback path if needed.
    Both paths must be on the same filesystem.
    """
    backup_dir = None
    if os.path.exists(target_dir):
        if not os.path.isdir(target_dir):
            raise RuntimeError(f"Cannot replace non-directory path: {target_dir}")
        backup_dir = f"{target_dir}.bak.{uuid.uuid4().hex[:8]}"
        os.replace(target_dir, backup_dir)

    try:
        os.replace(staging_dir, target_dir)
    except Exception:
        if backup_dir and os.path.isdir(backup_dir) and not os.path.exists(target_dir):
            os.replace(backup_dir, target_dir)
        raise
    else:
        if backup_dir:
            shutil.rmtree(backup_dir, ignore_errors=True)


def _read_markdown_from_archive(archive_path: str) -> str:
    """Read content.md from a conversion archive."""
    with zipfile.ZipFile(archive_path, "r") as archive:
        try:
            return archive.read("content.md").decode("utf-8", errors="replace")
        except KeyError as exc:
            raise RuntimeError("Conversion archive is missing content.md") from exc


def _hydrate_output_from_archive(
    archive_path: str,
    markdown_path: str,
    assets_dir_path: str,
    assets_dir_name: str
) -> Tuple[bool, int]:
    """
    Materialize markdown and assets for one PDF from an existing zip archive.

    Returns:
        (wrote_assets, asset_count)
    """
    markdown_text = _read_markdown_from_archive(archive_path)
    markdown_text = _rewrite_markdown_asset_paths(markdown_text, assets_dir_name)

    staging_root = tempfile.mkdtemp(prefix=".blobforge-hydrate-assets-", dir=os.path.dirname(assets_dir_path) or ".")
    staging_assets = os.path.join(staging_root, "assets")
    os.makedirs(staging_assets, exist_ok=True)

    try:
        asset_count = _extract_assets_to_staging(archive_path, staging_assets)
        _write_text_atomic(markdown_path, markdown_text)

        if asset_count > 0:
            _replace_directory(staging_assets, assets_dir_path)
            shutil.rmtree(staging_root, ignore_errors=True)
            return True, asset_count

        shutil.rmtree(staging_root, ignore_errors=True)
        if os.path.isdir(assets_dir_path):
            shutil.rmtree(assets_dir_path, ignore_errors=True)
        return False, 0
    except Exception:
        shutil.rmtree(staging_root, ignore_errors=True)
        raise


def _extract_done_hash_from_key(key: str) -> Optional[str]:
    """Parse <prefix>/done/<hash>.zip keys into <hash>."""
    prefix = f"{S3_PREFIX_DONE}/"
    if not key.startswith(prefix) or not key.endswith(".zip"):
        return None
    file_hash = key[len(prefix):-4]
    if len(file_hash) == 64:
        return file_hash
    return None


def _build_done_hash_index(client: Any) -> Optional[Set[str]]:
    """
    Build a set of all completed hashes from the done prefix.
    Returns None if index build is unavailable/failed.
    """
    # Preferred fast path if the client provides a dedicated helper.
    if hasattr(client, "list_done_hashes"):
        try:
            return set(client.list_done_hashes())
        except Exception as exc:
            print(f"[WARN] Could not build done index via client helper: {exc}")

    # Fallback to generic list_objects when available.
    if not hasattr(client, "list_objects"):
        return None

    try:
        done_hashes: Set[str] = set()
        for obj in client.list_objects(f"{S3_PREFIX_DONE}/"):
            key = obj.get("Key", "")
            parsed = _extract_done_hash_from_key(key)
            if parsed:
                done_hashes.add(parsed)
        return done_hashes
    except Exception as exc:
        print(f"[WARN] Could not build done index from list_objects: {exc}")
        return None


def _resolve_manifest_hashes(client: Any) -> Optional[Set[str]]:
    """Load manifest hashes once for prefiltering. Returns None on fallback."""
    if not hasattr(client, "get_manifest"):
        return None

    try:
        manifest = client.get_manifest()
        entries = manifest.get("entries", {}) if isinstance(manifest, dict) else {}
        return set(entries.keys())
    except Exception as exc:
        print(f"[WARN] Could not load manifest for prefiltering: {exc}")
        return None


def _resolve_done_availability(
    client: Any,
    candidate_hashes: Set[str],
) -> Dict[str, bool]:
    """
    Resolve done availability for candidate hashes with the minimum request count.
    """
    availability: Dict[str, bool] = {}
    if not candidate_hashes:
        return availability

    if len(candidate_hashes) >= DONE_INDEX_THRESHOLD:
        done_index = _build_done_hash_index(client)
        if done_index is not None:
            for file_hash in candidate_hashes:
                availability[file_hash] = file_hash in done_index
            print(
                f"Preflight: resolved done availability via index "
                f"({len(candidate_hashes)} hashes checked)"
            )
            return availability

    for file_hash in candidate_hashes:
        done_key = f"{S3_PREFIX_DONE}/{file_hash}.zip"
        try:
            availability[file_hash] = bool(client.exists(done_key))
        except Exception:
            availability[file_hash] = False

    print(
        f"Preflight: resolved done availability via per-hash checks "
        f"({len(candidate_hashes)} hashes checked)"
    )
    return availability


def hydrate(paths: List[str], force: bool = False, dry_run: bool = False, s3: Optional[S3Client] = None) -> int:
    """
    Hydrate local markdown and assets for PDFs that already have completed
    conversions in BlobForge.
    """
    client = s3 or S3Client()
    pdf_files = discover_pdf_files(paths)

    if not pdf_files:
        print("No PDF files found.")
        return 0

    print(f"Found {len(pdf_files)} PDF(s). Starting local hash preflight...")

    stats: Dict[str, int] = {
        "found": len(pdf_files),
        "hydrated": 0,
        "skipped_existing_markdown": 0,
        "missing_conversion": 0,
        "errors": 0,
    }

    work_items: List[Dict[str, str]] = []

    for pdf_path in pdf_files:
        base_dir = os.path.dirname(pdf_path)
        stem = os.path.splitext(os.path.basename(pdf_path))[0]
        markdown_path = os.path.join(base_dir, f"{stem}.md")
        assets_dir_name = f"{stem}.assets"
        assets_dir_path = os.path.join(base_dir, assets_dir_name)

        if os.path.exists(markdown_path) and not force:
            stats["skipped_existing_markdown"] += 1
            continue

        try:
            file_hash = compute_sha256_with_cache(pdf_path)
        except Exception as exc:
            print(f"[ERROR] Failed to compute hash for {pdf_path}: {exc}")
            stats["errors"] += 1
            continue

        work_items.append({
            "pdf_path": pdf_path,
            "hash": file_hash,
            "markdown_path": markdown_path,
            "assets_dir_name": assets_dir_name,
            "assets_dir_path": assets_dir_path,
        })

    if not work_items:
        print("No files require hydration after local preflight.")
        print("\n--- Hydrate Summary ---")
        print(f"  Found PDFs:              {stats['found']}")
        print(f"  Hydrated:                {stats['hydrated']}")
        print(f"  Skipped (markdown exists): {stats['skipped_existing_markdown']}")
        print(f"  Missing conversions:     {stats['missing_conversion']}")
        print(f"  Errors:                  {stats['errors']}")
        return 1 if stats["errors"] > 0 else 0

    unique_hashes = {item["hash"] for item in work_items}
    print(
        f"Preflight: computed hashes for {len(work_items)} file(s), "
        f"{len(unique_hashes)} unique hash(es)."
    )

    manifest_hashes = _resolve_manifest_hashes(client)
    if manifest_hashes is not None:
        candidate_hashes = unique_hashes & manifest_hashes
        prefiltered_missing = unique_hashes - candidate_hashes
        print(
            f"Preflight: manifest filter kept {len(candidate_hashes)} hash(es), "
            f"prefiltered {len(prefiltered_missing)} as missing."
        )
    else:
        candidate_hashes = set(unique_hashes)
        prefiltered_missing = set()
        print("Preflight: manifest unavailable; checking all local hashes against done store.")

    conversion_available: Dict[str, bool] = {
        file_hash: False for file_hash in prefiltered_missing
    }
    conversion_available.update(_resolve_done_availability(client, candidate_hashes))

    archive_cache: Dict[str, str] = {}

    with tempfile.TemporaryDirectory(prefix="blobforge-hydrate-") as tmp_dir:
        for index, item in enumerate(work_items, start=1):
            pdf_path = item["pdf_path"]
            file_hash = item["hash"]
            markdown_path = item["markdown_path"]
            assets_dir_name = item["assets_dir_name"]
            assets_dir_path = item["assets_dir_path"]

            print(f"[{index}/{len(work_items)}] {pdf_path}")

            available = conversion_available.get(file_hash, False)
            if not available:
                print(f"  [MISS] No completed conversion for hash {file_hash[:12]}...")
                stats["missing_conversion"] += 1
                continue

            if dry_run:
                print(f"  [DRY-RUN] Would write {os.path.basename(markdown_path)} and {assets_dir_name}/")
                stats["hydrated"] += 1
                continue

            done_key = f"{S3_PREFIX_DONE}/{file_hash}.zip"
            archive_path = archive_cache.get(file_hash)
            if archive_path is None:
                archive_path = os.path.join(tmp_dir, f"{file_hash}.zip")
                try:
                    client.download_file(done_key, archive_path)
                    archive_cache[file_hash] = archive_path
                except Exception as exc:
                    print(f"  [ERROR] Failed to download conversion zip: {exc}")
                    stats["errors"] += 1
                    continue

            try:
                wrote_assets, asset_count = _hydrate_output_from_archive(
                    archive_path=archive_path,
                    markdown_path=markdown_path,
                    assets_dir_path=assets_dir_path,
                    assets_dir_name=assets_dir_name,
                )
                if wrote_assets:
                    print(f"  [HYDRATED] {os.path.basename(markdown_path)} ({asset_count} assets)")
                else:
                    print(f"  [HYDRATED] {os.path.basename(markdown_path)} (no assets)")
                stats["hydrated"] += 1
            except Exception as exc:
                print(f"  [ERROR] Failed to hydrate local outputs: {exc}")
                stats["errors"] += 1

    print("\n--- Hydrate Summary ---")
    print(f"  Found PDFs:              {stats['found']}")
    print(f"  Hydrated:                {stats['hydrated']}")
    print(f"  Skipped (markdown exists): {stats['skipped_existing_markdown']}")
    print(f"  Missing conversions:     {stats['missing_conversion']}")
    print(f"  Errors:                  {stats['errors']}")

    return 1 if stats["errors"] > 0 else 0
