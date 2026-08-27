"""Pure constructors for the versioned BlobForge v2 object namespace."""

from __future__ import annotations

import re

DIGEST_RE = re.compile(r"^blake3:([0-9a-f]{64})$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _hex(digest: str) -> str:
    match = DIGEST_RE.fullmatch(digest)
    if not match:
        raise ValueError("expected a canonical tagged BLAKE3 digest")
    return match.group(1)


def _id(value: str, label: str) -> str:
    if not ID_RE.fullmatch(value):
        raise ValueError(f"unsafe {label}")
    return value


def source_key(digest: str) -> str:
    value = _hex(digest)
    return f"store/v2/sources/blake3/{value[:2]}/{value}"


def recipe_key(digest: str) -> str:
    value = _hex(digest)
    return f"store/v2/recipes/blake3/{value[:2]}/{value}.json"


def artifact_key(digest: str, attempt_id: str) -> str:
    value = _hex(digest)
    attempt = _id(attempt_id, "attempt ID")
    return f"store/v2/artifacts/mdaf/v1/blake3/{value[:2]}/{value}/{attempt}.mdaf"


def checkpoint_key(attempt_id: str, stage: str, digest: str) -> str:
    attempt = _id(attempt_id, "attempt ID")
    stage_value = _id(stage, "stage")
    value = _hex(digest)
    return f"store/v2/checkpoints/{attempt}/{stage_value}/blake3/{value}"


def migration_manifest_key(run_id: str) -> str:
    return f"store/v2/migrations/{_id(run_id, 'migration run ID')}/manifest.json"
