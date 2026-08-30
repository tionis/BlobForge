import json
import zipfile
from pathlib import Path

import pytest

from blobforge.mdaf import MdafMemberInput, MdafSource, build_mdaf, canonical_json_bytes
from blobforge.mdaf.builder import activity, markdown_outline
from blobforge.recipe_lifecycle import (
    PARENT_INFO_PATH,
    PARENT_PROVENANCE_PATH,
    PREVIOUS_RECIPE_PATH,
    RECIPE_MEMBER_PATH,
    recipe_digest,
)
from blobforge.reprocessing import reprocess_mdaf

RECIPES = Path(__file__).parents[1] / "blobforge" / "recipes"


def _parent(tmp_path):
    source_recipe = json.loads(
        (RECIPES / "mistral-ocr-4.1-wiki-v2.json").read_text(encoding="utf-8")
    )
    native = {
        "model": "mistral-ocr-4-1",
        "usage_info": {"pages_processed": 1},
        "pages": [
            {
                "index": 0,
                "markdown": "HEADER\n\n◆ First\n\n◆ Second",
                "dimensions": {"width": 100, "height": 200},
                "blocks": [
                    {"type": "header", "content": "HEADER"},
                    {"type": "text", "content": "◆ First"},
                    {"type": "text", "content": "◆ Second"},
                ],
                "images": [],
            }
        ],
    }
    native_bytes = (json.dumps(native, sort_keys=True, indent=2) + "\n").encode()
    text = "HEADER\n\n◆ First\n\n◆ Second"
    conversion = activity(
        activity_id="activity:convert",
        kind="document-extraction",
        tools=[
            {"name": "mistralai", "version": "2.9.4"},
            {"name": "blobforge-wiki-normalizer", "version": "2.0.0"},
        ],
        models=[
            {
                "provider": "mistral-ai",
                "identifier": "mistral-ocr-4-1",
                "returned_identifier": "mistral-ocr-4-1",
                "resolution": "mutable-alias",
            }
        ],
        inputs=["source:document"],
        outputs=[
            "text.md",
            "provenance.json",
            "outline.json",
            "renditions/ai.mistral/ocr-response.json",
        ],
        parameters={"recipe_digest": recipe_digest(source_recipe)},
    )
    parent = tmp_path / "parent.mdaf"
    result = build_mdaf(
        parent,
        text=text,
        title="Fixture",
        sources=[
            MdafSource(
                "document",
                "application/pdf",
                "blake3:" + "1" * 64,
                name="fixture.pdf",
            )
        ],
        activities=[conversion],
        producer={"name": "blobforge", "version": "0.4.0"},
        extra_members=[
            MdafMemberInput(
                "renditions/ai.mistral/ocr-response.json",
                native_bytes,
                "rendition",
                "activity:convert",
                "application/json",
                namespace="ai.mistral",
            )
        ],
        outline=markdown_outline(text),
    )
    return parent, result.identity, native_bytes, source_recipe


def test_reprocesses_retained_response_into_immutable_self_contained_derivative(
    tmp_path,
):
    parent, parent_identity, native_bytes, source_recipe = _parent(tmp_path)
    target_path = RECIPES / "mistral-ocr-4.1-wiki-v3.json"
    target_recipe = json.loads(target_path.read_text(encoding="utf-8"))
    output = tmp_path / "upgraded.mdaf"
    result = reprocess_mdaf(parent, target_path, output)
    assert result.parent_identity == parent_identity
    assert result.source_recipe_digest == recipe_digest(source_recipe)
    assert result.target_recipe_digest == recipe_digest(target_recipe)
    assert result.normalization_stats["headers_removed"] == 1
    assert result.normalization_stats["text_list_items_recovered"] == 2

    with zipfile.ZipFile(output) as archive:
        assert archive.read("renditions/ai.mistral/ocr-response.json") == native_bytes
        assert archive.read(RECIPE_MEMBER_PATH) == canonical_json_bytes(target_recipe)
        assert archive.read(PREVIOUS_RECIPE_PATH) == canonical_json_bytes(source_recipe)
        assert archive.read(PARENT_INFO_PATH)
        assert archive.read(PARENT_PROVENANCE_PATH)
        assert archive.read("text.md").decode() == "- First\n\n- Second"
        manifest = json.loads(archive.read("info.json"))
        assert manifest["derived_from"] == [parent_identity]
        provenance = json.loads(archive.read("provenance.json"))
        assert [item["kind"] for item in provenance["activities"]] == [
            "retained-extraction-evidence",
            "document-normalization",
        ]
        assert all(
            item["parameters"]["network_access"] is False
            for item in provenance["activities"]
        )


def test_reprocessing_is_deterministic_and_refuses_overwrite(tmp_path):
    parent, _, _, _ = _parent(tmp_path)
    target = RECIPES / "mistral-ocr-4.1-wiki-v3.json"
    first = reprocess_mdaf(parent, target, tmp_path / "first.mdaf")
    second = reprocess_mdaf(parent, target, tmp_path / "second.mdaf")
    assert first.identity == second.identity
    assert first.path.read_bytes() == second.path.read_bytes()
    with pytest.raises(FileExistsError):
        reprocess_mdaf(parent, target, first.path)
