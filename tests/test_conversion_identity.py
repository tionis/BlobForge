"""Tests for conversion recipe identity and runtime provenance."""

from unittest.mock import patch

import pytest

from blobforge.conversion_identity import (
    conversion_recipe_digest,
    current_conversion_provenance,
    current_conversion_recipe,
)


def test_recipe_digest_is_canonical_and_ignores_mapping_order():
    left = {"schema_version": 1, "engine": "marker", "options": {"a": 1, "b": 2}}
    right = {"options": {"b": 2, "a": 1}, "engine": "marker", "schema_version": 1}

    assert conversion_recipe_digest(left) == conversion_recipe_digest(right)
    assert len(conversion_recipe_digest(left)) == 64


def test_recipe_digest_rejects_fractional_numbers():
    with pytest.raises(TypeError, match="fractional numbers as strings"):
        conversion_recipe_digest({"threshold": 1.5})


def test_recipe_digest_rejects_unsafe_integers():
    with pytest.raises(ValueError, match="JavaScript-safe"):
        conversion_recipe_digest({"limit": 2**53})


def test_marker_major_changes_recipe_identity_but_patch_version_does_not():
    with patch(
        "blobforge.conversion_identity._configured_models", return_value={}
    ), patch(
        "blobforge.conversion_identity._distribution_versions",
        return_value={"marker-pdf": "1.10.2"},
    ):
        marker_one = current_conversion_recipe()
    with patch(
        "blobforge.conversion_identity._configured_models", return_value={}
    ), patch(
        "blobforge.conversion_identity._distribution_versions",
        return_value={"marker-pdf": "1.11.0"},
    ):
        marker_one_patch = current_conversion_recipe()
    with patch(
        "blobforge.conversion_identity._configured_models", return_value={}
    ), patch(
        "blobforge.conversion_identity._distribution_versions",
        return_value={"marker-pdf": "2.0.0"},
    ):
        marker_two = current_conversion_recipe()

    assert conversion_recipe_digest(marker_one) == conversion_recipe_digest(marker_one_patch)
    assert conversion_recipe_digest(marker_one) != conversion_recipe_digest(marker_two)


def test_configured_model_change_changes_recipe_identity():
    with patch(
        "blobforge.conversion_identity._distribution_versions",
        return_value={"marker-pdf": "1.10.2"},
    ), patch(
        "blobforge.conversion_identity._configured_models",
        return_value={"layout_model_checkpoint": "layout/revision-1"},
    ):
        first = current_conversion_recipe()
    with patch(
        "blobforge.conversion_identity._distribution_versions",
        return_value={"marker-pdf": "1.10.2"},
    ), patch(
        "blobforge.conversion_identity._configured_models",
        return_value={"layout_model_checkpoint": "layout/revision-2"},
    ):
        second = current_conversion_recipe()

    assert conversion_recipe_digest(first) != conversion_recipe_digest(second)


def test_provenance_records_exact_versions_and_redacts_external_url(monkeypatch):
    monkeypatch.setenv("SURYA_INFERENCE_URL", "https://secret.internal/v1")
    monkeypatch.setenv("SURYA_INFERENCE_BACKEND", "llamacpp")
    monkeypatch.setenv("LLAMA_CPP_BINARY", "/opt/llama/bin/llama-server")
    monkeypatch.setenv("BLOBFORGE_BUILD_REVISION", "abc123")
    recipe = {"schema_version": 1, "engine": "marker", "options": {}}

    with patch(
        "blobforge.conversion_identity._distribution_versions",
        return_value={"marker-pdf": "2.0.0", "surya-ocr": "0.22.1"},
    ):
        provenance = current_conversion_provenance(recipe)

    assert provenance["packages"]["marker-pdf"] == "2.0.0"
    assert provenance["blobforge_revision"] == "abc123"
    assert provenance["recipe_digest"] == conversion_recipe_digest(recipe)
    assert provenance["inference"] == {
        "backend": "llamacpp",
        "external_server": True,
        "llama_cpp_binary": "llama-server",
    }
    assert "secret.internal" not in str(provenance)
