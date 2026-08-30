import json
from pathlib import Path

import pytest

from blobforge.recipe_lifecycle import (
    RecipeVersion,
    assert_reprocessable,
    parse_recipe_lifecycle,
    recipe_digest,
)

RECIPES = Path(__file__).parents[1] / "blobforge" / "recipes"


def _recipe(name):
    return json.loads((RECIPES / name).read_text(encoding="utf-8"))


def test_lifecycle_ties_recipe_major_to_expensive_extraction():
    target = _recipe("mistral-ocr-4.1-wiki-v3.json")
    lifecycle = parse_recipe_lifecycle(target)
    assert lifecycle.version == RecipeVersion(1, 2, 0)
    assert lifecycle.extraction_major == 1
    assert lifecycle.postprocessing_version == RecipeVersion(2, 0, 0)
    assert lifecycle.native_members == (
        "renditions/ai.mistral/ocr-response.json",
    )


def test_known_v2_recipe_can_upgrade_without_extraction_change():
    source = _recipe("mistral-ocr-4.1-wiki-v2.json")
    target = _recipe("mistral-ocr-4.1-wiki-v3.json")
    source_digest, lifecycle = assert_reprocessable(source, target)
    assert source_digest == recipe_digest(source)
    assert source_digest in lifecycle.upgrade_from


def test_major_or_extraction_change_cannot_be_reprocessed():
    source = _recipe("mistral-ocr-4.1-wiki-v2.json")
    target = _recipe("mistral-ocr-4.1-wiki-v3.json")
    target["lifecycle"]["extraction"]["recipe_digest"] = "blake3:" + "0" * 64
    with pytest.raises(ValueError, match="extraction recipe changed"):
        assert_reprocessable(source, target)


def test_incompatible_evolution_must_advance_extraction_major():
    target = _recipe("mistral-ocr-4.1-wiki-v3.json")
    target["lifecycle"]["upgrade"]["automatic"] = False
    with pytest.raises(ValueError, match="new recipe/extraction major"):
        parse_recipe_lifecycle(target)

    target = _recipe("mistral-ocr-4.1-wiki-v3.json")
    target["lifecycle"]["extraction"]["version"] = "2.0.0"
    with pytest.raises(ValueError, match="extraction version major"):
        parse_recipe_lifecycle(target)

    target = _recipe("mistral-ocr-4.1-wiki-v3.json")
    target["lifecycle"]["recipe_version"] = "2.0.0"
    with pytest.raises(ValueError, match="recipe major"):
        parse_recipe_lifecycle(target)


def test_unlisted_source_recipe_is_not_automatically_upgraded():
    source = _recipe("datalab-convert-accurate-wiki-v1.json")
    target = _recipe("mistral-ocr-4.1-wiki-v3.json")
    with pytest.raises(ValueError, match="does not authorize"):
        assert_reprocessable(source, target)
