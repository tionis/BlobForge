"""Frozen BLAKE3 corpus manifests for repeatable converter evaluations."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .mdaf.digest import blake3_bytes, canonical_json_bytes
from .utils import compute_hashes_with_cache


@dataclass(frozen=True)
class CorpusManifestResult:
    path: Path
    digest: str
    documents: int
    pages: int
    bytes: int


def _pdf_pages(path: Path) -> int:
    completed = subprocess.run(
        ["pdfinfo", str(path)], capture_output=True, text=True, check=True
    )
    for line in completed.stdout.splitlines():
        if line.startswith("Pages:"):
            return int(line.split(":", 1)[1].strip())
    raise ValueError(f"pdfinfo returned no page count for {path}")


def build_manifest(root: str | Path, output: str | Path) -> CorpusManifestResult:
    corpus_root = Path(root).resolve()
    paths = sorted(
        (path for path in corpus_root.rglob("*") if path.is_file() and path.suffix.casefold() == ".pdf"),
        key=lambda path: path.relative_to(corpus_root).as_posix().encode("utf-8"),
    )
    documents = []
    for path in paths:
        stat = path.stat()
        hashes = compute_hashes_with_cache(str(path))
        documents.append(
            {
                "path": path.relative_to(corpus_root).as_posix(),
                "media_type": "application/pdf",
                "size_bytes": stat.st_size,
                "pages": _pdf_pages(path),
                "digest": f"blake3:{hashes['blake3']}",
                "alternate_digests": [f"sha256:{hashes['sha256']}"],
            }
        )
    body = {
        "format": "blobforge-evaluation-corpus",
        "version": 1,
        "documents": documents,
    }
    digest = blake3_bytes(canonical_json_bytes(body))
    manifest = {
        **body,
        "manifest_digest": digest,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_root": str(corpus_root),
    }
    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return CorpusManifestResult(
        destination,
        digest,
        len(documents),
        sum(item["pages"] for item in documents),
        sum(item["size_bytes"] for item in documents),
    )
