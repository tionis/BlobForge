import json
import zipfile

import pytest

from blobforge.mdaf import (
    MdafMemberInput,
    MdafSource,
    blake3_bytes,
    build_mdaf,
    logical_identity,
    validate_mdaf,
)
from blobforge.mdaf.builder import activity


def test_logical_identity_matches_vulcan_vector():
    files = {
        "info.json": b"{}\n",
        "provenance.json": b"{}\n",
        "text.md": b"# Example\n",
    }
    records = [(path, len(data), blake3_bytes(data)) for path, data in files.items()]
    assert logical_identity(records) == (
        "blake3:12f3c291b4437cd46527f53851ce359d305e3f6e89694548d4d7cf9faba2b899"
    )


def test_build_and_validate_mdaf(tmp_path):
    text = "# Café\n\nExample.\n"
    activities = [
        activity(
            activity_id="activity:extract",
            kind="document-extraction",
            tools=[{"name": "test-extractor", "version": "1.0.0"}],
            inputs=["source:document"],
            outputs=["text.md", "assets/image.txt", "source-map.json", "provenance.json"],
            parameters={"mode": "test"},
        )
    ]
    result = build_mdaf(
        tmp_path / "result.mdaf",
        text=text,
        sources=[
            MdafSource(
                id="document",
                media_type="application/pdf",
                digest="blake3:" + "0" * 64,
                alternate_digests=("sha256:" + "1" * 64,),
                name="example.pdf",
            )
        ],
        activities=activities,
        producer={"name": "blobforge", "version": "0.4.0"},
        extra_members=[
            MdafMemberInput(
                path="assets/image.txt",
                data=b"asset\n",
                role="asset",
                created_by="activity:extract",
                media_type="text/plain",
            )
        ],
        source_map={
            "mappings": [
                {
                    "document": {"start": 0, "end": len(text.encode())},
                    "source": {
                        "source_id": "document",
                        "selectors": [
                            {"type": "interval", "unit": "page", "start": 0, "end": 1}
                        ],
                    },
                    "confidence": 1,
                    "method": "dev.tionis.blobforge/test",
                }
            ],
            "references": [],
        },
    )

    validated = validate_mdaf(result.path)
    assert validated.identity == result.identity
    assert validated.member_count == 5
    with zipfile.ZipFile(result.path) as archive:
        assert archive.namelist() == sorted(archive.namelist())
        assert json.loads(archive.read("source-map.json"))["document_digest"] == blake3_bytes(
            text.encode()
        )


def test_validate_rejects_tampered_declared_digest(tmp_path):
    result = build_mdaf(
        tmp_path / "result.mdaf",
        text="# Example\n",
        sources=[MdafSource("document", "application/pdf", "blake3:" + "0" * 64)],
        activities=[
            activity(
                activity_id="activity:extract",
                kind="document-extraction",
                tools=[{"name": "test", "version": "1"}],
                inputs=["source:document"],
                outputs=["text.md", "provenance.json"],
                parameters={},
            )
        ],
        producer={"name": "blobforge", "version": "0.4.0"},
    )
    rewritten = tmp_path / "tampered.mdaf"
    with zipfile.ZipFile(result.path) as source, zipfile.ZipFile(rewritten, "w") as target:
        for item in source.infolist():
            data = source.read(item)
            if item.filename == "text.md":
                data += b"tampered"
            target.writestr(item, data)
    with pytest.raises(ValueError, match="digest/size mismatch"):
        validate_mdaf(rewritten)


def test_validate_rejects_absolute_markdown_asset_path(tmp_path):
    result = build_mdaf(
        tmp_path / "absolute.mdaf",
        text="![bad](/tmp/secret/image.png)\n",
        sources=[MdafSource("document", "application/pdf", "blake3:" + "0" * 64)],
        activities=[
            activity(
                activity_id="activity:test",
                kind="test",
                tools=[{"name": "test", "version": "1"}],
                inputs=["source:document"],
                outputs=["text.md", "provenance.json"],
                parameters={},
            )
        ],
        producer={"name": "test", "version": "1"},
    )
    with pytest.raises(ValueError, match="absolute local Markdown target"):
        validate_mdaf(result.path)


def test_validate_rejects_span_on_utf8_continuation_byte(tmp_path):
    text = "# Café\n"
    result = build_mdaf(
        tmp_path / "split.mdaf",
        text=text,
        sources=[MdafSource("document", "application/pdf", "blake3:" + "0" * 64)],
        activities=[
            activity(
                activity_id="activity:test",
                kind="test",
                tools=[{"name": "test", "version": "1"}],
                inputs=["source:document"],
                outputs=["text.md", "source-map.json", "provenance.json"],
                parameters={},
            )
        ],
        producer={"name": "test", "version": "1"},
        source_map={
            "mappings": [
                {
                    "document": {"start": 6, "end": 7},
                    "source": {
                        "source_id": "document",
                        "selectors": [
                            {"type": "interval", "unit": "page", "start": 0, "end": 1}
                        ],
                    },
                    "confidence": 1,
                    "method": "test",
                }
            ]
        },
    )
    with pytest.raises(ValueError, match="splits UTF-8"):
        validate_mdaf(result.path)


def test_validate_rejects_unknown_activity_input(tmp_path):
    result = build_mdaf(
        tmp_path / "unknown-input.mdaf",
        text="# Example\n",
        sources=[MdafSource("document", "application/pdf", "blake3:" + "0" * 64)],
        activities=[
            activity(
                activity_id="activity:test",
                kind="test",
                tools=[{"name": "test", "version": "1"}],
                inputs=["artifact:blake3:" + "1" * 64],
                outputs=["text.md", "provenance.json"],
                parameters={},
            )
        ],
        producer={"name": "test", "version": "1"},
    )
    with pytest.raises(ValueError, match="unknown activity input"):
        validate_mdaf(result.path)
