"""Canonical BLAKE3 operations defined by MDAF v1."""

from __future__ import annotations

import json
from pathlib import Path
from typing import BinaryIO, Iterable, Mapping

from blake3 import blake3


def tagged(hex_digest: str) -> str:
    return f"blake3:{hex_digest}"


def blake3_bytes(data: bytes) -> str:
    return tagged(blake3(data).hexdigest())


def blake3_reader(reader: BinaryIO, chunk_size: int = 1024 * 1024) -> str:
    hasher = blake3()
    while chunk := reader.read(chunk_size):
        hasher.update(chunk)
    return tagged(hasher.hexdigest())


def blake3_file(path: str | Path) -> str:
    with Path(path).open("rb") as handle:
        return blake3_reader(handle)


def canonical_json_bytes(value: object) -> bytes:
    """Serialize an MDAF parameters object canonically, without a final LF."""
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def json_document_bytes(value: object) -> bytes:
    """Serialize a human-inspectable control document deterministically."""
    return (
        json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True)
        + "\n"
    ).encode("utf-8")


def parameters_digest(parameters: Mapping[str, object]) -> str:
    return blake3_bytes(canonical_json_bytes(parameters))


def logical_identity(records: Iterable[tuple[str, int, str]]) -> str:
    """Compute the representation-independent MDAF logical identity."""
    hasher = blake3()
    for path, size, digest in sorted(records, key=lambda item: item[0].encode("utf-8")):
        encoded_path = json.dumps(path, ensure_ascii=False, separators=(",", ":"))
        record = (
            f'{{"path":{encoded_path},"size":{size},"digest":"{digest}"}}\n'
        )
        hasher.update(record.encode("utf-8"))
    return tagged(hasher.hexdigest())
