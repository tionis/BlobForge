"""Executable adapter metadata for exact, deployable conversion recipes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .mdaf import blake3_bytes, canonical_json_bytes


@dataclass(frozen=True)
class AdapterRecipe:
    key: str
    backend: str
    recipe: Mapping[str, Any]
    recipe_digest: str
    media_types: tuple[str, ...]
    artifact_type: str
    input_suffix: str
    command: tuple[str, ...]
    parameters: Mapping[str, Any]
    environment: Mapping[str, str]
    deployment_status: str

    def capability(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "recipe_digest": self.recipe_digest,
            "recipe": dict(self.recipe),
            "media_types": list(self.media_types),
            "artifact_type": self.artifact_type,
        }


def mistral_wiki_v2_recipe(
    *,
    repository: str | Path | None = None,
    max_pages: int,
    max_cost_usd: float,
    response_cache: str | Path,
    api_rights_confirmed: bool,
    cache_only: bool = False,
) -> AdapterRecipe:
    """Build the canary runtime without embedding credentials in metadata."""
    if max_pages < 1:
        raise ValueError("max_pages must be positive")
    if max_cost_usd <= 0:
        raise ValueError("max_cost_usd must be positive")
    if not api_rights_confirmed:
        raise ValueError("hosted workers require explicit API-rights confirmation")
    root = Path(repository or Path(__file__).resolve().parent.parent).resolve()
    recipe_path = root / "blobforge" / "recipes" / "mistral-ocr-4.1-wiki-v2.json"
    raw_path = root / "blobforge" / "recipes" / "mistral-ocr-4.1-v1.json"
    recipe = json.loads(recipe_path.read_text(encoding="utf-8"))
    raw_recipe = json.loads(raw_path.read_text(encoding="utf-8"))
    digest = blake3_bytes(canonical_json_bytes(recipe))
    expected = "blake3:bdd3e060e88f64277834245a42528a54b6b077774123c3806bdd827cf8ea3026"
    if digest != expected:
        raise RuntimeError(f"mistral-wiki-v2 recipe identity changed: {digest}")
    adapter = root / "evaluators" / "mistral" / "adapter.py"
    project = adapter.parent
    environment = {
        "BLOBFORGE_MISTRAL_RESPONSE_CACHE": str(Path(response_cache).expanduser())
    }
    if cache_only:
        environment["MISTRAL_API_KEY"] = ""
    return AdapterRecipe(
        key="mistral-wiki-v2",
        backend="mistral-ocr-wiki",
        recipe=recipe,
        recipe_digest=digest,
        media_types=("application/pdf",),
        artifact_type="mdaf/v1",
        input_suffix=".pdf",
        command=("uv", "run", "--project", str(project), "python", str(adapter)),
        parameters={
            "api_rights_confirmed": True,
            "do_ocr": True,
            "do_table_structure": True,
            "extract_images": True,
            "generate_picture_images": True,
            "max_cost_usd": max_cost_usd,
            "max_pages": max_pages,
            "model": None,
            "normalization_profile": "wiki-v2",
            "provider_request_digest": blake3_bytes(canonical_json_bytes(raw_recipe)),
            "recipe_digest": digest,
        },
        environment=environment,
        deployment_status="canary",
    )
