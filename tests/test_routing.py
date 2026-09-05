import json
import math
from pathlib import Path

import pytest

from blobforge.mdaf import blake3_bytes, canonical_json_bytes
from blobforge.routing import RoutingFeatures, route_pdf
from blobforge.routing.policy import POLICY_PATH


def _features(**changes):
    values = {
        "media_type": "application/pdf",
        "source_class": "born-digital-pnp-rulebook",
        "page_count": 100,
        "native_text_ratio": 0.99,
        "language": "en",
        "quality_tier": "quality",
        "external_processing_allowed": True,
        "max_cost_usd": 0.40,
    }
    values.update(changes)
    return RoutingFeatures(**values)


def test_quality_route_resolves_default_promoted_recipe():
    decision = route_pdf(_features())
    assert decision.eligible
    assert decision.recipe_digest == (
        "blake3:6ca8dda0c845605dd969134e208bfea44988f8ca72ff85fceea428359bf41eec"
    )
    assert decision.estimated_cost_usd == 0.4
    assert decision.policy_revision == 3
    assert decision.policy_digest.startswith("blake3:")


def test_routing_policy_revisions_are_immutable_and_distinct():
    old_path = POLICY_PATH.with_name("pdf-rulebooks-v1.json")
    old = json.loads(old_path.read_text(encoding="utf-8"))
    current = json.loads(POLICY_PATH.read_text(encoding="utf-8"))

    assert old["revision"] == 1
    assert old["candidates"]["hosted-quality"]["recipe_digest"] == (
        "blake3:bdd3e060e88f64277834245a42528a54b6b077774123c3806bdd827cf8ea3026"
    )
    assert current["revision"] == 3
    assert current["candidates"]["hosted-quality"]["recipe_digest"] == (
        "blake3:6ca8dda0c845605dd969134e208bfea44988f8ca72ff85fceea428359bf41eec"
    )
    assert blake3_bytes(canonical_json_bytes(old)) != blake3_bytes(
        canonical_json_bytes(current)
    )


def test_routing_fails_closed_for_privacy_scan_cost_and_canary_status():
    private = route_pdf(_features(external_processing_allowed=False), allow_canary=True)
    assert not private.eligible
    assert "not authorized" in " ".join(private.rationale)

    scanned = route_pdf(_features(native_text_ratio=0.5), allow_canary=True)
    assert not scanned.eligible
    assert "born-digital" in " ".join(scanned.rationale)

    expensive = route_pdf(_features(max_cost_usd=0.39), allow_canary=True)
    assert not expensive.eligible
    assert "exceeds" in " ".join(expensive.rationale)

    previous = json.loads(POLICY_PATH.with_name("pdf-rulebooks-v2.json").read_text())
    not_promoted = route_pdf(_features(), policy=previous)
    assert not not_promoted.eligible
    assert "canary-only" in " ".join(not_promoted.rationale)

    equations = route_pdf(_features(equations=True), allow_canary=True)
    assert not equations.eligible
    assert "equation-heavy" in " ".join(equations.rationale)


def test_override_cannot_bypass_policy_or_rights():
    digest = "blake3:6ca8dda0c845605dd969134e208bfea44988f8ca72ff85fceea428359bf41eec"
    blocked = route_pdf(
        _features(external_processing_allowed=False),
        allow_canary=True,
        recipe_override=digest,
    )
    assert not blocked.eligible

    unknown = route_pdf(_features(), allow_canary=True, recipe_override="blake3:" + "0" * 64)
    assert not unknown.eligible
    assert "not declared" in " ".join(unknown.rationale)


def test_feature_validation():
    with pytest.raises(ValueError, match="page_count"):
        _features(page_count=0)
    with pytest.raises(ValueError, match="native_text_ratio"):
        _features(native_text_ratio=1.1)
    with pytest.raises(ValueError, match="page_count"):
        _features(page_count=1.5)
    with pytest.raises(ValueError, match="max_cost_usd"):
        _features(max_cost_usd=math.nan)
