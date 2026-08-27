"""Maintenance operations for outputs materialized by ``blobforge hydrate``."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Iterable, List, Optional

from .hydrator import discover_pdf_files


@dataclass(frozen=True)
class HydratedOutput:
    """Paths belonging to one hydrated PDF."""

    pdf_path: str
    markdown_path: str
    assets_path: str
    textpack_path: str


def discover_hydrated_outputs(paths: List[str]) -> List[HydratedOutput]:
    """Find hydrated outputs anchored to PDFs with a sibling ``<stem>.md``."""
    outputs: List[HydratedOutput] = []
    seen_markdown = set()
    for pdf_path in discover_pdf_files(paths):
        base_dir = os.path.dirname(pdf_path)
        stem = os.path.splitext(os.path.basename(pdf_path))[0]
        markdown_path = os.path.join(base_dir, f"{stem}.md")
        if markdown_path in seen_markdown or not os.path.exists(markdown_path):
            continue
        seen_markdown.add(markdown_path)
        outputs.append(
            HydratedOutput(
                pdf_path=pdf_path,
                markdown_path=markdown_path,
                assets_path=os.path.join(base_dir, f"{stem}.assets"),
                textpack_path=os.path.join(base_dir, f"{stem}.textpack"),
            )
        )
    return outputs


def discover_textpacks(paths: List[str]) -> List[HydratedOutput]:
    """Find TextPacks anchored to PDFs with the same directory and stem."""
    outputs: List[HydratedOutput] = []
    seen_textpacks = set()
    for pdf_path in discover_pdf_files(paths):
        base_dir = os.path.dirname(pdf_path)
        stem = os.path.splitext(os.path.basename(pdf_path))[0]
        textpack_path = os.path.join(base_dir, f"{stem}.textpack")
        if textpack_path in seen_textpacks or not os.path.exists(textpack_path):
            continue
        seen_textpacks.add(textpack_path)
        outputs.append(
            HydratedOutput(
                pdf_path=pdf_path,
                markdown_path=os.path.join(base_dir, f"{stem}.md"),
                assets_path=os.path.join(base_dir, f"{stem}.assets"),
                textpack_path=textpack_path,
            )
        )
    return outputs


def _validate_source(output: HydratedOutput) -> None:
    if os.path.islink(output.markdown_path):
        raise RuntimeError(f"Markdown path is a symbolic link: {output.markdown_path}")
    if not os.path.isfile(output.markdown_path):
        raise RuntimeError(f"Markdown path is not a regular file: {output.markdown_path}")
    if os.path.islink(output.assets_path):
        raise RuntimeError(f"Assets path is a symbolic link: {output.assets_path}")
    if os.path.exists(output.assets_path) and not os.path.isdir(output.assets_path):
        raise RuntimeError(f"Assets path is not a directory: {output.assets_path}")


def _remove_source(output: HydratedOutput) -> None:
    """Remove a validated Markdown/assets pair after any replacement is durable."""
    if os.path.isdir(output.assets_path):
        shutil.rmtree(output.assets_path)
    os.unlink(output.markdown_path)


def clean(paths: List[str], execute: bool = False) -> int:
    """Preview or delete all PDF-anchored hydrated Markdown/assets pairs."""
    outputs = discover_hydrated_outputs(paths)
    removed = 0
    errors = 0

    if not outputs:
        print("No hydrated outputs found.")
        return 0

    for output in outputs:
        try:
            _validate_source(output)
            suffix = " and assets" if os.path.isdir(output.assets_path) else ""
            if execute:
                _remove_source(output)
                print(f"[REMOVED] {output.markdown_path}{suffix}")
            else:
                print(f"[DRY-RUN] Would remove {output.markdown_path}{suffix}")
            removed += 1
        except Exception as exc:
            print(f"[ERROR] {output.pdf_path}: {exc}")
            errors += 1

    verb = "Removed" if execute else "Would remove"
    print(f"\n{verb} {removed} hydrated output(s); errors: {errors}.")
    if not execute:
        print("Re-run with --execute to apply these changes.")
    return 1 if errors else 0


def _asset_files(assets_path: str) -> Iterable[tuple[str, str]]:
    if not os.path.isdir(assets_path):
        return
    for root, directories, filenames in os.walk(assets_path):
        for name in directories:
            directory_path = os.path.join(root, name)
            if os.path.islink(directory_path):
                raise RuntimeError(f"Asset directory is a symbolic link: {directory_path}")
        directories.sort()
        filenames.sort()
        for name in filenames:
            source_path = os.path.join(root, name)
            if os.path.islink(source_path) or not os.path.isfile(source_path):
                raise RuntimeError(f"Asset is not a regular file: {source_path}")
            relative = os.path.relpath(source_path, assets_path).replace(os.sep, "/")
            yield source_path, f"assets/{relative}"


def _create_textpack(output: HydratedOutput, force: bool) -> None:
    """Atomically create and validate a TextBundle v2 compressed archive."""
    if os.path.exists(output.textpack_path) and not force:
        raise FileExistsError(f"TextPack already exists: {output.textpack_path}")
    if os.path.isdir(output.textpack_path):
        raise RuntimeError(f"TextPack target is a directory: {output.textpack_path}")

    with open(output.markdown_path, "r", encoding="utf-8") as handle:
        markdown = handle.read()
    assets_dir_name = os.path.basename(output.assets_path)
    markdown = markdown.replace(f"{assets_dir_name}/", "assets/")
    metadata = {
        "version": 2,
        "type": "net.daringfireball.markdown",
        "transient": False,
        "dev.tionis.blobforge": {
            "version": 1,
            "sourcePDF": os.path.basename(output.pdf_path),
        },
    }

    target_dir = os.path.dirname(output.textpack_path) or "."
    fd, temporary_path = tempfile.mkstemp(
        prefix=".blobforge-textpack-", suffix=".tmp", dir=target_dir
    )
    os.close(fd)
    try:
        with zipfile.ZipFile(
            temporary_path, "w", compression=zipfile.ZIP_DEFLATED
        ) as archive:
            archive.writestr("text.md", markdown.encode("utf-8"))
            archive.writestr(
                "info.json",
                json.dumps(metadata, indent=2, ensure_ascii=False).encode("utf-8") + b"\n",
            )
            archive.writestr("assets/", b"")
            for source_path, archive_path in _asset_files(output.assets_path):
                archive.write(source_path, archive_path)

        with zipfile.ZipFile(temporary_path, "r") as archive:
            if archive.testzip() is not None:
                raise RuntimeError("TextPack archive failed CRC validation")
            names = set(archive.namelist())
            if not {"text.md", "info.json", "assets/"}.issubset(names):
                raise RuntimeError("TextPack archive is missing required entries")
            if json.loads(archive.read("info.json"))["version"] != 2:
                raise RuntimeError("TextPack archive metadata is invalid")

        os.replace(temporary_path, output.textpack_path)
    finally:
        if os.path.exists(temporary_path):
            os.unlink(temporary_path)


def textpack(paths: List[str], execute: bool = False, force: bool = False) -> int:
    """Preview or replace hydrated outputs with standard ``.textpack`` files."""
    outputs = discover_hydrated_outputs(paths)
    packed = 0
    skipped = 0
    errors = 0

    if not outputs:
        print("No hydrated outputs found.")
        return 0

    for output in outputs:
        try:
            _validate_source(output)
            if os.path.exists(output.textpack_path) and not force:
                print(f"[SKIP] TextPack already exists: {output.textpack_path}")
                skipped += 1
                continue
            if execute:
                _create_textpack(output, force=force)
                _remove_source(output)
                print(f"[PACKED] {output.textpack_path}")
            else:
                replacement = " (overwrite)" if os.path.exists(output.textpack_path) else ""
                print(
                    f"[DRY-RUN] Would create {output.textpack_path}{replacement} "
                    "and remove its Markdown/assets"
                )
            packed += 1
        except Exception as exc:
            print(f"[ERROR] {output.pdf_path}: {exc}")
            errors += 1

    verb = "Packed" if execute else "Would pack"
    print(f"\n{verb} {packed} hydrated output(s); skipped: {skipped}; errors: {errors}.")
    if not execute:
        print("Re-run with --execute to apply these changes.")
    return 1 if errors else 0


def _validate_textpack_path(output: HydratedOutput) -> None:
    if os.path.islink(output.textpack_path):
        raise RuntimeError(f"TextPack path is a symbolic link: {output.textpack_path}")
    if not os.path.isfile(output.textpack_path):
        raise RuntimeError(f"TextPack path is not a regular file: {output.textpack_path}")


def clean_textpacks(paths: List[str], execute: bool = False) -> int:
    """Preview or delete PDF-anchored ``.textpack`` files."""
    outputs = discover_textpacks(paths)
    removed = 0
    errors = 0

    if not outputs:
        print("No PDF-anchored TextPacks found.")
        return 0

    for output in outputs:
        try:
            _validate_textpack_path(output)
            if execute:
                os.unlink(output.textpack_path)
                print(f"[REMOVED] {output.textpack_path}")
            else:
                print(f"[DRY-RUN] Would remove {output.textpack_path}")
            removed += 1
        except Exception as exc:
            print(f"[ERROR] {output.pdf_path}: {exc}")
            errors += 1

    verb = "Removed" if execute else "Would remove"
    print(f"\n{verb} {removed} TextPack(s); errors: {errors}.")
    if not execute:
        print("Re-run with --execute to apply these changes.")
    return 1 if errors else 0


def _safe_asset_path(member: zipfile.ZipInfo) -> Optional[str]:
    """Return a safe relative asset path, or ``None`` for directory entries."""
    name = member.filename
    if "\\" in name or not name.startswith("assets/"):
        raise RuntimeError(f"Unsafe or unexpected TextPack member: {name}")
    relative = name[len("assets/"):]
    if not relative:
        return None
    path = PurePosixPath(relative)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise RuntimeError(f"Unsafe TextPack asset path: {name}")
    if member.is_dir():
        return None
    unix_mode = member.external_attr >> 16
    if unix_mode and unix_mode & 0o170000 not in (0, 0o100000):
        raise RuntimeError(f"TextPack asset is not a regular file: {name}")
    return "/".join(path.parts)


def _unpack_textpack(output: HydratedOutput) -> int:
    """Validate and restore one TextPack, returning the extracted asset count."""
    staging_root = tempfile.mkdtemp(
        prefix=".blobforge-unpack-", dir=os.path.dirname(output.textpack_path) or "."
    )
    staging_assets = os.path.join(staging_root, "assets")
    os.makedirs(staging_assets)
    try:
        with zipfile.ZipFile(output.textpack_path, "r") as archive:
            if archive.testzip() is not None:
                raise RuntimeError("TextPack archive failed CRC validation")
            members = archive.infolist()
            names = [member.filename for member in members]
            if len(names) != len(set(names)):
                raise RuntimeError("TextPack archive contains duplicate members")
            text_names = [
                name
                for name in names
                if "/" not in name and name.startswith("text.") and name != "text."
            ]
            if len(text_names) != 1 or "info.json" not in names:
                raise RuntimeError("TextPack must contain one text.* file and info.json")

            metadata = json.loads(archive.read("info.json"))
            if metadata.get("version") != 2:
                raise RuntimeError("TextPack metadata version must be 2")
            if metadata.get("type", "net.daringfireball.markdown") != (
                "net.daringfireball.markdown"
            ):
                raise RuntimeError("TextPack body is not declared as Markdown")
            markdown = archive.read(text_names[0]).decode("utf-8")
            assets_dir_name = os.path.basename(output.assets_path)
            markdown = markdown.replace("assets/", f"{assets_dir_name}/")

            asset_count = 0
            seen_assets = set()
            allowed_roots = {"info.json", text_names[0]}
            for member in members:
                if member.filename in allowed_roots:
                    continue
                relative = _safe_asset_path(member)
                if relative is None:
                    continue
                if relative in seen_assets:
                    raise RuntimeError(f"Duplicate TextPack asset path: {relative}")
                seen_assets.add(relative)
                destination = os.path.join(staging_assets, *relative.split("/"))
                os.makedirs(os.path.dirname(destination), exist_ok=True)
                with archive.open(member) as source, open(destination, "wb") as target:
                    shutil.copyfileobj(source, target)
                asset_count += 1

        from .hydrator import _replace_directory, _write_text_atomic

        _write_text_atomic(output.markdown_path, markdown)
        if asset_count:
            _replace_directory(staging_assets, output.assets_path)
        elif os.path.isdir(output.assets_path):
            shutil.rmtree(output.assets_path)
        return asset_count
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)


def unpack(paths: List[str], execute: bool = False, force: bool = False) -> int:
    """Preview or restore TextPacks to Markdown/assets and remove each archive."""
    outputs = discover_textpacks(paths)
    unpacked = 0
    skipped = 0
    errors = 0

    if not outputs:
        print("No PDF-anchored TextPacks found.")
        return 0

    for output in outputs:
        try:
            _validate_textpack_path(output)
            target_exists = os.path.exists(output.markdown_path) or os.path.exists(
                output.assets_path
            )
            if target_exists and not force:
                print(f"[SKIP] Hydrated output already exists for {output.pdf_path}")
                skipped += 1
                continue
            if os.path.islink(output.markdown_path) or os.path.islink(output.assets_path):
                raise RuntimeError("Refusing to replace a symbolic-link output path")
            if os.path.exists(output.markdown_path) and not os.path.isfile(
                output.markdown_path
            ):
                raise RuntimeError(f"Markdown target is not a file: {output.markdown_path}")
            if os.path.exists(output.assets_path) and not os.path.isdir(output.assets_path):
                raise RuntimeError(f"Assets target is not a directory: {output.assets_path}")

            if execute:
                asset_count = _unpack_textpack(output)
                os.unlink(output.textpack_path)
                print(
                    f"[UNPACKED] {output.markdown_path} "
                    f"({asset_count} asset{'s' if asset_count != 1 else ''})"
                )
            else:
                replacement = " (overwrite)" if target_exists else ""
                print(
                    f"[DRY-RUN] Would unpack {output.textpack_path}{replacement} "
                    "and remove the archive"
                )
            unpacked += 1
        except Exception as exc:
            print(f"[ERROR] {output.pdf_path}: {exc}")
            errors += 1

    verb = "Unpacked" if execute else "Would unpack"
    print(f"\n{verb} {unpacked} TextPack(s); skipped: {skipped}; errors: {errors}.")
    if not execute:
        print("Re-run with --execute to apply these changes.")
    return 1 if errors else 0
