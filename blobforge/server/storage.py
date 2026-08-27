"""Atomic local storage and signed transfer capabilities."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from blake3 import blake3


def _safe_component(value: str) -> str:
    if not value or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-:" for char in value):
        raise ValueError("invalid storage identifier")
    return value


@dataclass(frozen=True)
class StoredObject:
    path: Path
    size: int
    sha256: str
    blake3: str


class LocalStorage:
    def __init__(self, root: Path):
        self.root = root
        self.sources = root / "objects" / "sources"
        self.artifacts = root / "objects" / "artifacts"
        self.pending = root / "pending"
        self.trash_root = root / "trash"
        for path in (self.sources, self.artifacts, self.pending, self.trash_root):
            path.mkdir(parents=True, exist_ok=True)

    def source_path(self, algorithm: str, digest: str) -> Path:
        algorithm = _safe_component(algorithm)
        digest = _safe_component(digest)
        return self.sources / algorithm / digest[:2] / digest

    def pending_output_path(self, source_key: str, lease_token: str) -> Path:
        return self.pending / _safe_component(source_key) / _safe_component(lease_token)

    def artifact_path(self, source_key: str, recipe_digest: str, identity: str) -> Path:
        return self.artifacts / _safe_component(source_key)[:2] / _safe_component(source_key) / _safe_component(recipe_digest) / _safe_component(identity)

    def trash(self, paths: list[Path], reason: str) -> list[str]:
        """Move managed objects into a recoverable trash tree."""
        root = self.root.resolve()
        destination_root = self.trash_root / f"{int(time.time() * 1000)}-{_safe_component(reason)}"
        moved: list[str] = []
        for path in paths:
            resolved = path.resolve()
            try:
                relative = resolved.relative_to(root)
            except ValueError as exc:
                raise ValueError("refusing to trash a path outside the storage root") from exc
            if not resolved.is_file():
                continue
            destination = destination_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(resolved, destination)
            moved.append(str(destination.relative_to(root)))
        return moved

    @staticmethod
    def inspect(path: Path) -> StoredObject:
        sha = hashlib.sha256()
        b3 = blake3()
        size = 0
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                sha.update(chunk)
                b3.update(chunk)
                size += len(chunk)
        return StoredObject(path, size, sha.hexdigest(), b3.hexdigest())

    @staticmethod
    def atomic_stream(stream: BinaryIO, destination: Path) -> StoredObject:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{secrets.token_hex(8)}.tmp")
        sha = hashlib.sha256()
        b3 = blake3()
        size = 0
        try:
            with temporary.open("xb") as target:
                while chunk := stream.read(1024 * 1024):
                    target.write(chunk)
                    sha.update(chunk)
                    b3.update(chunk)
                    size += len(chunk)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return StoredObject(destination, size, sha.hexdigest(), b3.hexdigest())


class CapabilitySigner:
    def __init__(self, secret_path: Path, ttl_seconds: int):
        secret_path.parent.mkdir(parents=True, exist_ok=True)
        if not secret_path.exists():
            temporary = secret_path.with_suffix(".tmp")
            temporary.write_bytes(secrets.token_bytes(32))
            os.chmod(temporary, 0o600)
            os.replace(temporary, secret_path)
        self.secret = secret_path.read_bytes()
        self.ttl_seconds = ttl_seconds

    def issue(self, method: str, scope: str, subject: str) -> tuple[int, str]:
        expires = int(time.time()) + self.ttl_seconds
        message = f"{method}\n{scope}\n{subject}\n{expires}".encode()
        return expires, hmac.new(self.secret, message, hashlib.sha256).hexdigest()

    def verify(self, method: str, scope: str, subject: str, expires: int, signature: str) -> bool:
        if expires < int(time.time()):
            return False
        expected = self.issue_for_expiry(method, scope, subject, expires)
        return hmac.compare_digest(expected, signature)

    def issue_for_expiry(self, method: str, scope: str, subject: str, expires: int) -> str:
        message = f"{method}\n{scope}\n{subject}\n{expires}".encode()
        return hmac.new(self.secret, message, hashlib.sha256).hexdigest()
