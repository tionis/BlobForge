"""Fail-closed routing for the evaluated born-digital rulebook class."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from ..mdaf import blake3_bytes, canonical_json_bytes

# Policy documents are immutable once used. Advance this pointer to a new file;
# never repurpose an earlier revision for a different recipe.
POLICY_PATH = Path(__file__).with_name("pdf-rulebooks-v3.json")


@dataclass(frozen=True)
class RoutingFeatures:
    media_type: str
    source_class: str
    page_count: int
    native_text_ratio: float
    language: str = "und"
    quality_tier: str = "quality"
    layout_class: str = "standard"
    complex_tables: bool = False
    equations: bool = False
    external_processing_allowed: bool = False
    max_cost_usd: float | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.page_count, bool)
            or not isinstance(self.page_count, int)
            or self.page_count < 1
        ):
            raise ValueError("page_count must be positive")
        if (
            not math.isfinite(self.native_text_ratio)
            or not 0 <= self.native_text_ratio <= 1
        ):
            raise ValueError("native_text_ratio must be between 0 and 1")
        if self.max_cost_usd is not None and (
            not math.isfinite(self.max_cost_usd) or self.max_cost_usd < 0
        ):
            raise ValueError("max_cost_usd must be finite and non-negative")


@dataclass(frozen=True)
class RoutingDecision:
    policy: str
    policy_revision: int
    policy_digest: str
    features: Mapping[str, Any]
    recipe_digest: str | None
    backend: str | None
    candidate_status: str | None
    estimated_cost_usd: float | None
    eligible: bool
    rationale: tuple[str, ...]

    def as_json(self) -> dict[str, Any]:
        value = asdict(self)
        value["rationale"] = list(self.rationale)
        return value


def load_pdf_rulebook_policy() -> dict[str, Any]:
    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    if policy.get("schema") != "dev.tionis.blobforge.routing-policy/v1":
        raise ValueError("unsupported PDF rulebook routing policy schema")
    for name, candidate in policy.get("candidates", {}).items():
        recipe_path = (POLICY_PATH.parent / candidate["recipe_path"]).resolve()
        recipe = json.loads(recipe_path.read_text(encoding="utf-8"))
        actual = blake3_bytes(canonical_json_bytes(recipe))
        if actual != candidate.get("recipe_digest"):
            raise ValueError(f"routing candidate {name} recipe digest mismatch")
    return policy


def _decision(
    policy: Mapping[str, Any],
    features: RoutingFeatures,
    *,
    candidate: Mapping[str, Any] | None,
    estimated_cost: float | None,
    reasons: list[str],
) -> RoutingDecision:
    eligible = candidate is not None and not reasons
    return RoutingDecision(
        policy=str(policy["policy"]),
        policy_revision=int(policy["revision"]),
        policy_digest=blake3_bytes(canonical_json_bytes(policy)),
        features=asdict(features),
        recipe_digest=str(candidate["recipe_digest"]) if eligible else None,
        backend=str(candidate["backend"]) if eligible else None,
        candidate_status=str(candidate["status"]) if eligible else None,
        estimated_cost_usd=estimated_cost,
        eligible=eligible,
        rationale=tuple(
            reasons or ["selected the exact evaluated hosted-quality recipe"]
        ),
    )


def route_pdf(
    features: RoutingFeatures,
    *,
    allow_canary: bool = False,
    recipe_override: str | None = None,
    policy: Mapping[str, Any] | None = None,
) -> RoutingDecision:
    """Resolve features to an exact recipe, or explain why routing is blocked.

    Manual overrides select only a recipe already declared by this policy and
    never bypass media, privacy, applicability, or spend constraints.
    """
    selected_policy = dict(policy or load_pdf_rulebook_policy())
    scope = selected_policy["scope"]
    candidates = selected_policy["candidates"]
    reasons: list[str] = []

    if features.media_type not in scope["media_types"]:
        reasons.append(f"unsupported media type: {features.media_type}")
    if features.source_class != scope["source_class"]:
        reasons.append(f"unsupported source class: {features.source_class}")
    if features.native_text_ratio < float(scope["minimum_native_text_ratio"]):
        reasons.append("native-text ratio is below the born-digital applicability gate")
    if features.language not in scope["languages"]:
        reasons.append(
            f"language has not passed this policy's evaluation: {features.language}"
        )
    if features.layout_class not in {"standard", "complex-tables"}:
        reasons.append(
            "layout class has not passed this policy's evaluation: "
            f"{features.layout_class}"
        )
    if features.equations:
        reasons.append("equation-heavy documents have not passed this policy's evaluation")
    if not features.external_processing_allowed:
        reasons.append("external API processing is not authorized")

    candidate_name = (
        selected_policy["selection"]["complex_tables"]
        if features.complex_tables or features.layout_class == "complex-tables"
        else selected_policy["selection"].get(features.quality_tier)
    )
    candidate = candidates.get(candidate_name) if candidate_name else None
    if candidate is None:
        reasons.append(f"no candidate for quality tier: {features.quality_tier}")

    if recipe_override:
        matches = [
            value
            for value in candidates.values()
            if value["recipe_digest"] == recipe_override
        ]
        if not matches:
            reasons.append("recipe override is not declared by this policy revision")
            candidate = None
        else:
            candidate = matches[0]

    estimated_cost = None
    if candidate is not None:
        estimated_cost = round(
            features.page_count * float(candidate["cost_usd_per_page"]), 6
        )
        if features.max_cost_usd is None:
            reasons.append("a hard cost ceiling is required")
        elif estimated_cost > features.max_cost_usd:
            reasons.append(
                f"estimated ${estimated_cost:.3f} exceeds ${features.max_cost_usd:.3f} ceiling"
            )
        if candidate["status"] == "canary" and not allow_canary:
            reasons.append("candidate is canary-only; explicit canary opt-in is required")

    return _decision(
        selected_policy,
        features,
        candidate=candidate,
        estimated_cost=estimated_cost,
        reasons=reasons,
    )
