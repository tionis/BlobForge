import hashlib
import json
import subprocess
import zipfile
from pathlib import Path

from blobforge.enrichment import (
    PdfBlock,
    PdfEvidence,
    PdfPage,
    align_markdown_to_pdf,
    extract_pdf_evidence,
    segment_markdown,
)
from blobforge.enrichment.legacy import enrich_legacy_mdaf
from blobforge.legacy_migration import (
    convert_one,
    enrich_one,
    enrichment_summary,
    inventory,
    pending_enrichment_hashes,
    verify_enrichments,
)
from blobforge.mdaf import MdafSource, blake3_file, build_mdaf, validate_mdaf
from blobforge.mdaf.builder import activity


def _evidence(*blocks):
    pages = {}
    for block in blocks:
        pages.setdefault(block.page, []).append(block)
    return PdfEvidence(
        "fixture",
        "1",
        tuple(PdfPage(page, 100, 100, tuple(pages[page])) for page in sorted(pages)),
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


def test_poppler_evidence_and_legacy_derived_mdaf(tmp_path):
    repository = Path(__file__).resolve().parent.parent
    source = repository / "assets/lorem.pdf"
    evidence = extract_pdf_evidence(source)
    assert len(evidence.pages) == 2
    assert evidence.blocks
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
    recipe_digest, pending = pending_enrichment_hashes(workspace, 20)
    assert pending == [sha256]

    first = enrich_one(sha256, workspace)
    first_identity = validate_mdaf(first).identity
    assert pending_enrichment_hashes(workspace, 20) == (recipe_digest, [])
    summary = enrichment_summary(workspace)
    assert (summary.eligible, summary.converted, summary.failed) == (1, 1, 0)
    assert summary.mapped_blocks > 0
    assert hashlib.sha256(base_path.read_bytes()).hexdigest() == base_digest
    verification = verify_enrichments(workspace)
    assert (verification.checked, verification.valid, verification.errors) == (1, 1, ())

    second = enrich_one(sha256, workspace)
    assert validate_mdaf(second).identity == first_identity
    assert enrichment_summary(workspace).converted == 1
