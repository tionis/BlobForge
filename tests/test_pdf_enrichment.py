import hashlib
import json
import sqlite3
import subprocess
import zipfile
from pathlib import Path

from blobforge.enrichment import (
    PdfBlock,
    PdfEvidence,
    PdfLine,
    PdfPage,
    PdfWord,
    align_markdown_to_pdf,
    extract_pdf_evidence,
    sanitize_poppler_xhtml,
    segment_markdown,
    validate_alignment_publication,
)
from blobforge.enrichment.legacy import (
    enrich_legacy_mdaf,
    enrichment_recipe,
    enrichment_recipe_digest,
)
from blobforge.legacy_migration import (
    EnrichmentWorkItem,
    convert_one,
    enrich_one,
    enrichment_summary,
    enrichment_work_items,
    inventory,
    pending_enrichment_hashes,
    select_enrichment_work_items,
    verify_enrichments,
)
from blobforge.mdaf import MdafSource, blake3_file, build_mdaf, validate_mdaf
from blobforge.mdaf.builder import activity


def test_poppler_xhtml_removes_only_xml_forbidden_c0_controls():
    valid = b"<word>tab\t line\n carriage\r &amp; unicode \xc3\xa9</word>"
    assert sanitize_poppler_xhtml(valid) == (valid, 0)
    dirty = b"<word>before\x00\x08\x0b\x0c\x0e\x18\x1fafter</word>"
    cleaned, removed = sanitize_poppler_xhtml(dirty)
    assert cleaned == b"<word>beforeafter</word>"
    assert removed == 7


def _evidence(*blocks):
    pages = {}
    for block in blocks:
        pages.setdefault(block.page, []).append(block)
    return PdfEvidence(
        "fixture",
        "1",
        tuple(PdfPage(page, 100, 100, tuple(pages[page])) for page in sorted(pages)),
    )


def _word_block(block_id, page, order, words):
    pdf_words = tuple(
        PdfWord(f"{block_id}-w{index}", text, x, 10, width, 10)
        for index, (text, x, width) in enumerate(words)
    )
    line = PdfLine(
        f"{block_id}-l0",
        " ".join(word.text for word in pdf_words),
        min(word.x for word in pdf_words),
        10,
        max(word.x + word.width for word in pdf_words) - min(word.x for word in pdf_words),
        10,
        pdf_words,
    )
    return PdfBlock(
        block_id,
        page,
        order,
        line.text,
        line.x,
        line.y,
        line.width,
        line.height,
        (line,),
    )


def test_markdown_segmentation_uses_final_utf8_bytes():
    markdown = "# Café\n\nAlpha beta.\n"
    blocks = segment_markdown(markdown)
    assert [(item.kind, item.text) for item in blocks] == [
        ("heading", "# Café"),
        ("paragraph", "Alpha beta."),
    ]
    encoded = markdown.encode("utf-8")
    assert encoded[blocks[0].start : blocks[0].end].decode("utf-8") == "# Café"
    assert blocks[1].start == len("# Café\n\n".encode("utf-8"))


def test_frozen_enrichment_recipe_has_reviewed_identity_and_poppler_version():
    recipe = enrichment_recipe("25.03.0")
    assert enrichment_recipe(validate_runtime=False) == recipe
    assert enrichment_recipe_digest(recipe) == (
        "blake3:0e7e6c1ba4bb6a8920a58cd08fe3c957bd48b729cbccc5733ffec3d47876a569"
    )
    try:
        enrichment_recipe("99.0.0")
    except RuntimeError as exc:
        assert "requires pdftotext 25.03.0" in str(exc)
    else:
        raise AssertionError("a mismatched Poppler version must fail closed")


def test_alignment_rejects_duplicate_location_without_seed_and_uses_page_seed():
    evidence = _evidence(
        PdfBlock("a", 0, 0, "Repeated title", 1, 2, 20, 5),
        PdfBlock("b", 1, 1, "Repeated title", 3, 4, 20, 5),
    )
    markdown = "# Repeated title\n"
    ambiguous = align_markdown_to_pdf(markdown, evidence)
    assert ambiguous.mapped_blocks == 0
    assert ambiguous.diagnostics[0].reason == "ambiguous"

    seeded = align_markdown_to_pdf(
        markdown,
        evidence,
        seed_mappings=[
            {
                "document": {"start": 0, "end": len(markdown.encode("utf-8"))},
                "source": {
                    "source_id": "document",
                    "selectors": [
                        {"type": "interval", "unit": "page", "start": 1, "end": 2}
                    ],
                },
            }
        ],
    )
    assert seeded.mapped_blocks == 1
    selectors = seeded.mappings[0]["source"]["selectors"]
    assert selectors[0] == {"type": "interval", "unit": "page", "start": 1, "end": 2}
    assert selectors[1]["type"] == "rectangle"


def test_alignment_clips_out_of_page_geometry_but_preserves_mapping():
    evidence = _evidence(PdfBlock("a", 0, 0, "Outside text", -4, -2, 30, 10))
    aligned = align_markdown_to_pdf("Outside text\n", evidence)
    assert aligned.mapped_blocks == 1
    rectangle = aligned.mappings[0]["source"]["selectors"][1]
    assert rectangle == {
        "type": "rectangle",
        "unit": "point",
        "x": 0.0,
        "y": 0.0,
        "width": 26.0,
        "height": 8.0,
    }
    assert aligned.diagnostics[0].reason == "geometry-clipped"


def test_alignment_uses_future_anchor_as_an_upper_page_bound():
    evidence = _evidence(
        PdfBlock("correct", 0, 0, "Primary Pool 5 7", 1, 2, 40, 5),
        PdfBlock("heading", 0, 1, "Enormous", 1, 10, 20, 5),
        PdfBlock("tempting", 2, 2, "Primary Pool 5-7", 1, 2, 40, 5),
    )
    markdown = "Primary Pool 5-7\n\n# Enormous\n"
    heading_start = len("Primary Pool 5-7\n\n".encode())
    aligned = align_markdown_to_pdf(
        markdown,
        evidence,
        seed_mappings=[
            {
                "document": {"start": heading_start, "end": len(markdown.rstrip().encode())},
                "source": {
                    "source_id": "document",
                    "selectors": [
                        {"type": "interval", "unit": "page", "start": 0, "end": 1}
                    ],
                },
            }
        ],
    )
    assert aligned.mapped_blocks == 2
    assert [mapping["source"]["selectors"][0]["start"] for mapping in aligned.mappings] == [0, 0]


def test_alignment_refines_split_markdown_to_disjoint_word_regions():
    evidence = _evidence(
        _word_block(
            "joined",
            0,
            0,
            (("First", 0, 10), ("paragraph", 12, 20), ("Second", 40, 12), ("paragraph", 54, 20)),
        )
    )
    aligned = align_markdown_to_pdf("First paragraph.\n\nSecond paragraph.\n", evidence)
    assert aligned.mapped_blocks == 2
    assert aligned.region_mapped_blocks == 2
    rectangles = [mapping["source"]["selectors"][1] for mapping in aligned.mappings]
    assert rectangles[0]["x"] == 0
    assert rectangles[0]["width"] == 32
    assert rectangles[1]["x"] == 40
    assert rectangles[1]["width"] == 34


def test_alignment_publishes_page_only_when_block_geometry_is_too_coarse():
    evidence = _evidence(
        PdfBlock("coarse", 0, 0, "Target words fit nearly exact extra", 1, 2, 80, 20)
    )
    aligned = align_markdown_to_pdf("Target words fit nearly exact\n", evidence)
    assert aligned.mapped_blocks == 1
    assert aligned.page_only_mapped_blocks == 1
    assert aligned.mappings[0]["method"].endswith("page-alignment-v2")
    assert aligned.mappings[0]["source"]["selectors"] == [
        {"type": "interval", "unit": "page", "start": 0, "end": 1}
    ]
    assert any(item.reason == "page-only" for item in aligned.diagnostics)


def test_alignment_downgrades_fuzzy_cross_block_region_to_page_only():
    evidence = _evidence(
        _word_block("left", 0, 0, (("Target", 0, 10), ("words", 12, 10))),
        _word_block(
            "right",
            0,
            1,
            (("fit", 50, 6), ("nearly", 58, 12), ("exact", 72, 10), ("source", 84, 12)),
        ),
    )
    aligned = align_markdown_to_pdf("Target words fit nearly exact\n", evidence)
    assert aligned.mapped_blocks == 1
    assert aligned.page_only_mapped_blocks == 1
    assert len(aligned.mappings[0]["source"]["selectors"]) == 1


def test_alignment_rejects_reusing_one_printed_label():
    evidence = _evidence(
        _word_block("label", 0, 0, (("Qualities", 10, 20),))
    )
    aligned = align_markdown_to_pdf("Qualities.\n\n# Qualities\n", evidence)
    assert aligned.mapped_blocks == 1
    assert any(item.reason == "evidence-reused" for item in aligned.diagnostics)


def test_alignment_publication_audit_rejects_regressions_and_rectangle_reuse():
    rectangle = {
        "type": "rectangle",
        "unit": "point",
        "x": 1,
        "y": 2,
        "width": 3,
        "height": 4,
    }
    source_map = {
        "mappings": [
            {
                "document": {"start": 0, "end": 5},
                "source": {
                    "source_id": "document",
                    "selectors": [
                        {"type": "interval", "unit": "page", "start": 2, "end": 3},
                        rectangle,
                    ],
                },
                "method": "dev.tionis.blobforge/poppler-word-region-alignment-v2",
            },
            {
                "document": {"start": 6, "end": 10},
                "source": {
                    "source_id": "document",
                    "selectors": [
                        {"type": "interval", "unit": "page", "start": 1, "end": 2},
                        rectangle,
                    ],
                },
                "method": "dev.tionis.blobforge/poppler-word-region-alignment-v2",
            },
        ]
    }
    errors = validate_alignment_publication(
        source_map,
        {
            "summary": {
                "mapped_blocks": 2,
                "region_mapped_blocks": 2,
                "page_only_mapped_blocks": 0,
            }
        },
    )
    assert any("regresses" in error for error in errors)
    # Page is part of rectangle identity, so make reuse happen independently.
    source_map["mappings"][1]["source"]["selectors"][0]["start"] = 2
    source_map["mappings"][1]["source"]["selectors"][0]["end"] = 3
    errors = validate_alignment_publication(
        source_map,
        {
            "summary": {
                "mapped_blocks": 2,
                "region_mapped_blocks": 2,
                "page_only_mapped_blocks": 0,
            }
        },
    )
    assert any("reuses published rectangle" in error for error in errors)


def test_size_aware_scheduler_never_selects_two_large_documents():
    items = [
        EnrichmentWorkItem("large-a", 100, 400, True),
        EnrichmentWorkItem("large-b", 100, 350, True),
        EnrichmentWorkItem("small-a", 10, 20, False),
        EnrichmentWorkItem("small-b", 10, 30, False),
    ]
    selected, remaining = select_enrichment_work_items(
        items, 3, large_running=False
    )
    assert [item.legacy_sha256 for item in selected] == [
        "large-a",
        "small-a",
        "small-b",
    ]
    assert [item.legacy_sha256 for item in remaining] == ["large-b"]

    selected, remaining = select_enrichment_work_items(
        items, 2, large_running=True
    )
    assert [item.legacy_sha256 for item in selected] == ["small-a", "small-b"]
    assert [item.legacy_sha256 for item in remaining] == ["large-a", "large-b"]


def test_poppler_evidence_and_legacy_derived_mdaf(tmp_path):
    repository = Path(__file__).resolve().parent.parent
    source = repository / "assets/lorem.pdf"
    evidence = extract_pdf_evidence(source)
    assert len(evidence.pages) == 2
    assert evidence.blocks
    assert all(block.lines for block in evidence.blocks)
    assert all(line.words for block in evidence.blocks for line in block.lines)
    assert all(block.width > 0 and block.height > 0 for block in evidence.blocks)

    extracted = subprocess.run(
        ["pdftotext", "-layout", str(source), "-"],
        capture_output=True,
        check=True,
        text=True,
    ).stdout
    markdown = extracted.replace("\f", "\n\n").rstrip() + "\n"
    sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    base = build_mdaf(
        tmp_path / "base.mdaf",
        text=markdown,
        title="Lorem",
        sources=[
            MdafSource(
                "document",
                "application/pdf",
                blake3_file(source),
                (f"sha256:{sha256}",),
                source.name,
            )
        ],
        activities=[
            activity(
                activity_id="activity:legacy",
                kind="document-extraction",
                tools=[{"name": "fixture", "version": "1"}],
                inputs=["source:document"],
                outputs=["text.md", "provenance.json"],
                parameters={},
            )
        ],
        producer={"name": "fixture", "version": "1"},
    )
    result = enrich_legacy_mdaf(source, base.path, tmp_path / "enriched.mdaf")
    validated = validate_mdaf(result.path)
    assert validated.manifest["derived_from"] == [base.identity]
    assert result.alignment.mapped_blocks > 0
    assert result.source_pages == 2
    with zipfile.ZipFile(result.path) as archive:
        assert archive.read("text.md").decode("utf-8") == markdown
        source_map = json.loads(archive.read("source-map.json"))
        assert any(
            any(selector["type"] == "rectangle" for selector in item["source"]["selectors"])
            for item in source_map["mappings"]
        )
        report = json.loads(
            archive.read("extensions/dev.tionis.blobforge.pdf-enrichment/report.json")
        )
        assert report["summary"]["mapped_blocks"] == result.alignment.mapped_blocks
        assert any(
            name.startswith("renditions/org.freedesktop.poppler/")
            for name in archive.namelist()
        )


def test_resumable_legacy_enrichment_catalog(tmp_path):
    repository = Path(__file__).resolve().parent.parent
    source = repository / "assets/lorem.pdf"
    source_bytes = source.read_bytes()
    sha256 = hashlib.sha256(source_bytes).hexdigest()
    workspace = tmp_path / "migration"
    raw_root = workspace / "remote/pdf/store/raw"
    out_root = workspace / "remote/pdf/store/out"
    raw_root.mkdir(parents=True)
    out_root.mkdir(parents=True)
    (raw_root / f"{sha256}.pdf").write_bytes(source_bytes)
    markdown = subprocess.run(
        ["pdftotext", "-layout", str(source), "-"],
        capture_output=True,
        check=True,
        text=True,
    ).stdout.replace("\f", "\n\n").rstrip() + "\n"
    with zipfile.ZipFile(out_root / f"{sha256}.zip", "w") as archive:
        archive.writestr("content.md", markdown)
        archive.writestr("info.json", json.dumps({"original_filename": source.name}))

    assert inventory(workspace).paired == 1
    base_path = convert_one(sha256, workspace)
    base_digest = hashlib.sha256(base_path.read_bytes()).hexdigest()
    work = enrichment_work_items([sha256], workspace, large_pages=2)
    assert [(item.pages, item.large) for item in work] == [(2, True)]
    with sqlite3.connect(workspace / "catalog.sqlite3") as connection:
        assert connection.execute(
            "SELECT pages FROM legacy_pdf_metadata WHERE legacy_sha256=?", (sha256,)
        ).fetchone() == (2,)
    recipe_digest, pending = pending_enrichment_hashes(workspace, 20)
    assert pending == [sha256]

    # A killed process can leave both a processing row and a partial destination.
    # The next bounded run must rediscover the row and atomically replace the file.
    source_digest = blake3_file(source)
    destination = (
        workspace
        / "generated"
        / source_digest.removeprefix("blake3:")[:2]
        / source_digest.removeprefix("blake3:")
        / "enriched"
        / f"{recipe_digest.removeprefix('blake3:')}.mdaf"
    )
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"interrupted archive")
    with sqlite3.connect(workspace / "catalog.sqlite3") as connection:
        connection.execute(
            """UPDATE legacy_enrichments SET status='processing'
               WHERE legacy_sha256=? AND recipe_digest=?""",
            (sha256, recipe_digest),
        )
    assert pending_enrichment_hashes(workspace, 20) == (recipe_digest, [sha256])

    first = enrich_one(sha256, workspace)
    first_identity = validate_mdaf(first).identity
    assert pending_enrichment_hashes(workspace, 20) == (recipe_digest, [])
    summary = enrichment_summary(workspace)
    assert (summary.eligible, summary.converted, summary.failed) == (1, 1, 0)
    assert summary.mapped_blocks > 0
    assert summary.measured_documents == 1
    assert summary.measured_pages == 2
    assert summary.elapsed_seconds > 0
    assert summary.peak_rss_bytes > 0
    assert summary.output_bytes == first.stat().st_size
    with sqlite3.connect(workspace / "catalog.sqlite3") as connection:
        attempt = connection.execute(
            """SELECT status, elapsed_seconds, peak_rss_bytes, peak_rss_method,
                      source_pages, output_size_bytes
               FROM legacy_enrichment_attempts"""
        ).fetchone()
    assert attempt[0] == "converted"
    assert attempt[1] > 0
    assert attempt[2] > 0
    assert attempt[3] in {
        "process-tree-rss-sampled-50ms",
        "process-high-water-rss",
    }
    assert attempt[4:] == (2, first.stat().st_size)
    assert hashlib.sha256(base_path.read_bytes()).hexdigest() == base_digest
    verification = verify_enrichments(workspace)
    assert (verification.checked, verification.valid, verification.errors) == (1, 1, ())

    second = enrich_one(sha256, workspace)
    assert validate_mdaf(second).identity == first_identity
    assert enrichment_summary(workspace).converted == 1
