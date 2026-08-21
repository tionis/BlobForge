"""Stable conversion recipe identity and exact runtime provenance."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import sys
from collections.abc import Iterable
from importlib import import_module
from importlib.metadata import PackageNotFoundError, distribution, version
from pathlib import Path
from typing import Any

RECIPE_SCHEMA_VERSION = 1
PROVENANCE_SCHEMA_VERSION = 1
OUTPUT_SCHEMA = "blobforge-markdown-v1"
MAX_SAFE_RECIPE_INTEGER = 2**53 - 1
TRACKED_DISTRIBUTIONS = (
    "blobforge",
    "marker-pdf",
    "surya-ocr",
    "pdftext",
    "torch",
    "transformers",
    "pillow",
)


def _distribution_versions(names: Iterable[str] = TRACKED_DISTRIBUTIONS) -> dict[str, str]:
    versions: dict[str, str] = {}
    for name in names:
        try:
            versions[name] = version(name)
        except PackageNotFoundError:
            versions[name] = "unavailable"
    return versions


def _major_generation(value: str) -> str:
    """Return a compatibility generation without depending on packaging."""
    major = value.split(".", 1)[0]
    return major if major.isdigit() else "unavailable"


def _configured_models() -> dict[str, str]:
    """Read configured Surya model identifiers without loading model weights."""
    try:
        settings = import_module("surya.settings").settings
    except (ImportError, AttributeError):
        return {}
    names = [
        name
        for name in dir(settings)
        if name.endswith(("_CHECKPOINT", "_MODEL_ID", "_REPO_ID"))
    ]
    return {
        name.lower(): str(getattr(settings, name))
        for name in sorted(names)
        if getattr(settings, name, None)
    }


def _source_revision() -> str:
    """Find a build or VCS revision without invoking Git at worker startup."""
    configured = os.getenv("BLOBFORGE_BUILD_REVISION", "").strip()
    if configured:
        return configured
    try:
        direct_url = distribution("blobforge").read_text("direct_url.json")
        if direct_url:
            vcs_info = json.loads(direct_url).get("vcs_info") or {}
            commit_id = str(vcs_info.get("commit_id") or "")
            if commit_id:
                return commit_id
    except (PackageNotFoundError, json.JSONDecodeError, AttributeError):
        pass

    git_dir = Path(__file__).resolve().parents[1] / ".git"
    try:
        if git_dir.is_file():
            pointer = git_dir.read_text(encoding="utf-8").strip()
            if pointer.startswith("gitdir: "):
                git_dir = (git_dir.parent / pointer.removeprefix("gitdir: ")).resolve()
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref: "):
            return head or "unknown"
        reference = head.removeprefix("ref: ")
        loose_ref = git_dir / reference
        if loose_ref.is_file():
            return loose_ref.read_text(encoding="utf-8").strip() or "unknown"
        packed_refs = (git_dir / "packed-refs").read_text(encoding="utf-8")
        for line in packed_refs.splitlines():
            if line and not line.startswith(("#", "^")):
                commit_id, ref_name = line.split(" ", 1)
                if ref_name == reference:
                    return commit_id
    except (OSError, ValueError):
        pass
    return "unknown"


def current_conversion_recipe() -> dict[str, Any]:
    """Describe settings that intentionally define output compatibility."""
    marker_version = _distribution_versions(("marker-pdf",))["marker-pdf"]
    return {
        "schema_version": RECIPE_SCHEMA_VERSION,
        "engine": "marker",
        "engine_generation": _major_generation(marker_version),
        "output_schema": OUTPUT_SCHEMA,
        "models": _configured_models(),
        # BlobForge currently invokes Marker with its release defaults. Add any
        # future output-affecting overrides here before exposing them to users.
        "options": {},
    }


def _validate_recipe_value(value: Any) -> None:
    """Restrict recipes to values with identical Python/JavaScript encoding."""
    if value is None or isinstance(value, (str, bool)):
        return
    if isinstance(value, int):
        if abs(value) > MAX_SAFE_RECIPE_INTEGER:
            raise ValueError("Conversion recipe integers must be JavaScript-safe")
        return
    if isinstance(value, float):
        raise TypeError("Conversion recipes must encode fractional numbers as strings")
    if isinstance(value, list):
        for item in value:
            _validate_recipe_value(item)
        return
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("Conversion recipe object keys must be strings")
        for item in value.values():
            _validate_recipe_value(item)
        return
    raise TypeError("Conversion recipe contains a non-JSON value")


def conversion_recipe_digest(recipe: dict[str, Any] | None = None) -> str:
    """Hash a recipe using canonical JSON for stable cross-process identity."""
    selected_recipe = current_conversion_recipe() if recipe is None else recipe
    _validate_recipe_value(selected_recipe)
    canonical = json.dumps(
        selected_recipe,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def current_conversion_provenance(
    recipe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Capture exact diagnostic runtime data without making it cache identity."""
    selected_recipe = current_conversion_recipe() if recipe is None else recipe
    inference_url = os.getenv("SURYA_INFERENCE_URL", "")
    return {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "recipe_digest": conversion_recipe_digest(selected_recipe),
        "recipe": selected_recipe,
        "packages": _distribution_versions(),
        "blobforge_revision": _source_revision(),
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "inference": {
            "backend": os.getenv("SURYA_INFERENCE_BACKEND") or "automatic",
            "external_server": bool(inference_url),
            "llama_cpp_binary": os.path.basename(
                os.getenv("LLAMA_CPP_BINARY", "llama-server")
            ),
        },
        "executable": os.path.basename(sys.executable),
    }
