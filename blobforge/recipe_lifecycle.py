"""Version and upgrade rules for extraction/post-processing recipe families."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .mdaf import blake3_bytes, canonical_json_bytes

SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
RECIPE_MEMBER_PATH = "extensions/dev.tionis.blobforge/recipe.json"
PARENT_PROVENANCE_PATH = (
    "extensions/dev.tionis.blobforge/parent-provenance.json"
)
PARENT_INFO_PATH = "extensions/dev.tionis.blobforge/parent-info.json"
PREVIOUS_RECIPE_PATH = "extensions/dev.tionis.blobforge/previous-recipe.json"


@dataclass(frozen=True, order=True)
class RecipeVersion:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, value: Any) -> "RecipeVersion":
        match = SEMVER.fullmatch(value) if isinstance(value, str) else None
        if match is None:
            raise ValueError("recipe lifecycle version must be MAJOR.MINOR.PATCH")
        return cls(*(int(part) for part in match.groups()))


@dataclass(frozen=True)
class RecipeLifecycle:
    digest: str
    family: str
    version: RecipeVersion
    extraction_major: int
    extraction_version: RecipeVersion
    extraction_recipe_digest: str
    native_members: tuple[str, ...]
    postprocessing_version: RecipeVersion
    postprocessing_profile: str
    automatic: bool
    upgrade_from: tuple[str, ...]


def recipe_digest(recipe: Mapping[str, Any]) -> str:
    return blake3_bytes(canonical_json_bytes(recipe))


def parse_recipe_lifecycle(recipe: Mapping[str, Any]) -> RecipeLifecycle:
    if recipe.get("schema") != "dev.tionis.blobforge.recipe/v3":
        raise ValueError("recipe does not declare lifecycle schema v3")
    lifecycle = recipe.get("lifecycle")
    if not isinstance(lifecycle, dict):
        raise ValueError("recipe lifecycle must be an object")
    extraction = lifecycle.get("extraction")
    postprocessing = lifecycle.get("postprocessing")
    upgrade = lifecycle.get("upgrade")
    if not all(isinstance(value, dict) for value in (extraction, postprocessing, upgrade)):
        raise ValueError("recipe lifecycle stages and upgrade policy must be objects")
    version = RecipeVersion.parse(lifecycle.get("recipe_version"))
    extraction_major = extraction.get("major")
    if (
        isinstance(extraction_major, bool)
        or not isinstance(extraction_major, int)
        or extraction_major < 1
    ):
        raise ValueError("extraction major must be a positive integer")
    if version.major != extraction_major:
        raise ValueError("recipe major must equal extraction major")
    extraction_version = RecipeVersion.parse(extraction.get("version"))
    if extraction_version.major != extraction_major:
        raise ValueError("extraction version major must equal extraction major")
    if extraction.get("replace_requires_recipe_major") is not True:
        raise ValueError("expensive extraction replacement must require a recipe major")
    extraction_digest = extraction.get("recipe_digest")
    if not isinstance(extraction_digest, str) or not re.fullmatch(
        r"blake3:[0-9a-f]{64}", extraction_digest
    ):
        raise ValueError("extraction recipe digest must be canonical BLAKE3")
    native_members = extraction.get("native_members")
    if (
        not isinstance(native_members, list)
        or not native_members
        or any(not isinstance(path, str) or not path for path in native_members)
        or len(set(native_members)) != len(native_members)
    ):
        raise ValueError("extraction native_members must be unique non-empty paths")
    family = lifecycle.get("family")
    profile = postprocessing.get("profile")
    if not isinstance(family, str) or not family:
        raise ValueError("recipe lifecycle family is required")
    if not isinstance(profile, str) or not profile:
        raise ValueError("post-processing profile is required")
    automatic = upgrade.get("automatic")
    if not isinstance(automatic, bool):
        raise ValueError("upgrade.automatic must be a boolean")
    if automatic is not True:
        raise ValueError(
            "same-extraction lifecycle recipes must permit automatic upgrades; "
            "use a new recipe/extraction major for incompatible evolution"
        )
    from_digests = upgrade.get("from_recipe_digests")
    if (
        not isinstance(from_digests, list)
        or any(
            not isinstance(value, str)
            or not re.fullmatch(r"blake3:[0-9a-f]{64}", value)
            for value in from_digests
        )
        or len(set(from_digests)) != len(from_digests)
    ):
        raise ValueError("upgrade source digests must be unique canonical BLAKE3")
    return RecipeLifecycle(
        digest=recipe_digest(recipe),
        family=family,
        version=version,
        extraction_major=extraction_major,
        extraction_version=extraction_version,
        extraction_recipe_digest=extraction_digest,
        native_members=tuple(native_members),
        postprocessing_version=RecipeVersion.parse(postprocessing.get("version")),
        postprocessing_profile=profile,
        automatic=automatic,
        upgrade_from=tuple(from_digests),
    )


def assert_reprocessable(
    source_recipe: Mapping[str, Any],
    target_recipe: Mapping[str, Any],
) -> tuple[str, RecipeLifecycle]:
    """Return source digest and target lifecycle, or reject unsafe upgrades."""
    source_digest = recipe_digest(source_recipe)
    target = parse_recipe_lifecycle(target_recipe)
    if not target.automatic or source_digest not in target.upgrade_from:
        raise ValueError("target recipe does not authorize automatic upgrade from source")
    source_base = source_recipe.get("base_recipe")
    source_extraction_digest = (
        source_base.get("digest") if isinstance(source_base, dict) else None
    )
    if source_recipe.get("schema") == "dev.tionis.blobforge.recipe/v3":
        source_lifecycle = parse_recipe_lifecycle(source_recipe)
        source_extraction_digest = source_lifecycle.extraction_recipe_digest
        if source_lifecycle.family != target.family:
            raise ValueError("recipe families differ")
        if source_lifecycle.version.major != target.version.major:
            raise ValueError("recipe major changed; expensive extraction must be rerun")
        if source_lifecycle.version >= target.version:
            raise ValueError("target recipe must be newer than source recipe")
    if source_extraction_digest != target.extraction_recipe_digest:
        raise ValueError("extraction recipe changed; expensive extraction must be rerun")
    return source_digest, target


def load_known_recipe(digest: str, root: str | Path | None = None) -> dict[str, Any]:
    """Resolve an old artifact's recipe digest from the immutable local registry."""
    recipe_root = Path(root or Path(__file__).with_name("recipes"))
    for path in sorted(recipe_root.glob("*.json")):
        recipe = json.loads(path.read_text(encoding="utf-8"))
        if recipe_digest(recipe) == digest:
            return recipe
    raise ValueError(f"recipe {digest} is not embedded and is unknown locally")
