import json
import zipfile

import pytest
from pypdf import PdfWriter
from pypdf.constants import PageLabelStyle

from blobforge.mdaf import MdafSource, build_mdaf, validate_mdaf
from blobforge.mdaf.builder import activity
from blobforge.mdaf.page_labels import page_label_ranges


def _pdf(path, pages, labels=()):
    writer = PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=72, height=72)
    for start, end, style, prefix, first in labels:
        writer.set_page_label(start, end, style=style, prefix=prefix, start=first)
    with open(path, "wb") as handle:
        writer.write(handle)
    return path


def test_page_label_ranges_reads_the_pdfs_own_labels(tmp_path):
    path = _pdf(
        tmp_path / "labeled.pdf",
        6,
        [
            (0, 1, None, "Cover", 0),
            (2, 3, PageLabelStyle.UPPERCASE_ROMAN, None, 1),
            (4, 5, PageLabelStyle.DECIMAL, "p", 7),
        ],
    )
    assert page_label_ranges(path) == [
        {"start": 0, "style": "none", "prefix": "Cover"},
        {"start": 2, "style": "roman-upper"},
        {"start": 4, "style": "decimal", "prefix": "p", "first": 7},
    ]


def test_page_label_ranges_none_without_labels(tmp_path):
    assert page_label_ranges(_pdf(tmp_path / "plain.pdf", 2)) is None
    assert page_label_ranges(tmp_path / "missing.pdf") is None


def _build(tmp_path, **extra):
    text = "# Book\n"
    return build_mdaf(
        tmp_path / "result.mdaf",
        text=text,
        sources=[
            MdafSource(
                id="document", media_type="application/pdf", digest="blake3:" + "0" * 64
            )
        ],
        activities=[
            activity(
                activity_id="activity:extract",
                kind="document-extraction",
                tools=[{"name": "test-extractor", "version": "1.0.0"}],
                inputs=["source:document"],
                outputs=["text.md", "source-map.json", "provenance.json"],
                parameters={},
            )
        ],
        producer={"name": "blobforge", "version": "0.4.0"},
        **extra,
    )


def test_build_mdaf_writes_page_labels_into_the_source_map(tmp_path):
    labels = {
        "document": [
            {"start": 0, "style": "alpha-upper"},
            {"start": 2, "style": "decimal"},
        ]
    }
    result = _build(
        tmp_path, source_map={"mappings": [], "references": []}, page_labels=labels
    )
    validate_mdaf(result.path)
    with zipfile.ZipFile(result.path) as archive:
        assert json.loads(archive.read("source-map.json"))["page_labels"] == labels


def test_build_mdaf_refuses_page_labels_without_a_source_map(tmp_path):
    with pytest.raises(ValueError):
        _build(tmp_path, page_labels={"document": [{"start": 0, "style": "decimal"}]})
