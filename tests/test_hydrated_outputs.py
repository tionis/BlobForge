import json
import zipfile

from blobforge.hydrated_outputs import (
    clean,
    clean_textpacks,
    discover_hydrated_outputs,
    discover_textpacks,
    textpack,
    unpack,
)


def _hydrated_pdf(tmp_path, stem="rules", assets=True):
    pdf = tmp_path / f"{stem}.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    markdown = tmp_path / f"{stem}.md"
    markdown.write_text(
        f"![map]({stem}.assets/maps/page.png)\n", encoding="utf-8"
    )
    assets_path = tmp_path / f"{stem}.assets"
    if assets:
        (assets_path / "maps").mkdir(parents=True)
        (assets_path / "maps" / "page.png").write_bytes(b"image")
    return pdf, markdown, assets_path


def test_discovery_requires_a_sibling_pdf_and_markdown(tmp_path):
    _hydrated_pdf(tmp_path, "known")
    (tmp_path / "orphan.md").write_text("orphan", encoding="utf-8")
    (tmp_path / "plain.pdf").write_bytes(b"%PDF-1.4\n%%EOF\n")

    outputs = discover_hydrated_outputs([str(tmp_path)])

    assert [output.markdown_path for output in outputs] == [str(tmp_path / "known.md")]


def test_clean_is_a_dry_run_by_default(tmp_path):
    _, markdown, assets = _hydrated_pdf(tmp_path)

    assert clean([str(tmp_path)]) == 0
    assert markdown.exists()
    assert assets.exists()


def test_clean_removes_markdown_and_assets_when_executed(tmp_path):
    pdf, markdown, assets = _hydrated_pdf(tmp_path)

    assert clean([str(tmp_path)], execute=True) == 0
    assert pdf.exists()
    assert not markdown.exists()
    assert not assets.exists()


def test_textpack_creates_standard_archive_then_removes_sources(tmp_path):
    pdf, markdown, assets = _hydrated_pdf(tmp_path)

    assert textpack([str(tmp_path)], execute=True) == 0

    target = tmp_path / "rules.textpack"
    assert pdf.exists()
    assert target.exists()
    assert not markdown.exists()
    assert not assets.exists()
    with zipfile.ZipFile(target) as archive:
        assert set(archive.namelist()) == {
            "text.md",
            "info.json",
            "assets/",
            "assets/maps/page.png",
        }
        assert archive.read("text.md") == b"![map](assets/maps/page.png)\n"
        assert archive.read("assets/maps/page.png") == b"image"
        metadata = json.loads(archive.read("info.json"))
        assert metadata["version"] == 2
        assert metadata["type"] == "net.daringfireball.markdown"
        assert metadata["dev.tionis.blobforge"]["sourcePDF"] == "rules.pdf"


def test_textpack_dry_run_preserves_sources_and_writes_nothing(tmp_path):
    _, markdown, assets = _hydrated_pdf(tmp_path)

    assert textpack([str(tmp_path)]) == 0
    assert markdown.exists()
    assert assets.exists()
    assert not (tmp_path / "rules.textpack").exists()


def test_textpack_skips_existing_target_without_removing_sources(tmp_path):
    _, markdown, assets = _hydrated_pdf(tmp_path)
    target = tmp_path / "rules.textpack"
    target.write_bytes(b"existing")

    assert textpack([str(tmp_path)], execute=True) == 0
    assert target.read_bytes() == b"existing"
    assert markdown.exists()
    assert assets.exists()


def test_textpack_force_replaces_existing_target(tmp_path):
    _, markdown, assets = _hydrated_pdf(tmp_path)
    target = tmp_path / "rules.textpack"
    target.write_bytes(b"existing")

    assert textpack([str(tmp_path)], execute=True, force=True) == 0
    assert zipfile.is_zipfile(target)
    assert not markdown.exists()
    assert not assets.exists()


def test_textpack_rejects_symlinked_assets_without_removing_sources(tmp_path):
    _, markdown, assets = _hydrated_pdf(tmp_path, assets=False)
    assets.mkdir()
    outside = tmp_path / "outside.png"
    outside.write_bytes(b"outside")
    (assets / "link.png").symlink_to(outside)

    assert textpack([str(tmp_path)], execute=True) == 1
    assert markdown.exists()
    assert assets.exists()
    assert not (tmp_path / "rules.textpack").exists()


def test_textpack_rejects_symlinked_asset_directory(tmp_path):
    _, markdown, assets = _hydrated_pdf(tmp_path, assets=False)
    assets.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "private.png").write_bytes(b"outside")
    (assets / "linked").symlink_to(outside, target_is_directory=True)

    assert textpack([str(tmp_path)], execute=True) == 1
    assert markdown.exists()
    assert assets.exists()
    assert not (tmp_path / "rules.textpack").exists()


def test_textpack_rejects_symlinked_markdown(tmp_path):
    pdf = tmp_path / "rules.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    outside = tmp_path / "outside.md"
    outside.write_text("private", encoding="utf-8")
    markdown = tmp_path / "rules.md"
    markdown.symlink_to(outside)

    assert textpack([str(tmp_path)], execute=True) == 1
    assert markdown.is_symlink()
    assert outside.read_text(encoding="utf-8") == "private"
    assert not (tmp_path / "rules.textpack").exists()


def _packed_pdf(tmp_path, stem="rules"):
    pdf, _, _ = _hydrated_pdf(tmp_path, stem=stem)
    assert textpack([str(pdf)], execute=True) == 0
    return pdf, tmp_path / f"{stem}.textpack"


def test_textpack_discovery_requires_matching_pdf(tmp_path):
    _, target = _packed_pdf(tmp_path, "known")
    (tmp_path / "orphan.textpack").write_bytes(b"orphan")

    outputs = discover_textpacks([str(tmp_path)])

    assert [output.textpack_path for output in outputs] == [str(target)]


def test_clean_textpacks_is_dry_run_by_default(tmp_path):
    pdf, target = _packed_pdf(tmp_path)

    assert clean_textpacks([str(tmp_path)]) == 0
    assert pdf.exists()
    assert target.exists()


def test_clean_textpacks_removes_only_archive_when_executed(tmp_path):
    pdf, target = _packed_pdf(tmp_path)

    assert clean_textpacks([str(tmp_path)], execute=True) == 0
    assert pdf.exists()
    assert not target.exists()


def test_unpack_is_dry_run_by_default(tmp_path):
    _, target = _packed_pdf(tmp_path)

    assert unpack([str(tmp_path)]) == 0
    assert target.exists()
    assert not (tmp_path / "rules.md").exists()
    assert not (tmp_path / "rules.assets").exists()


def test_unpack_restores_markdown_and_assets_then_removes_archive(tmp_path):
    pdf, target = _packed_pdf(tmp_path)

    assert unpack([str(tmp_path)], execute=True) == 0
    assert pdf.exists()
    assert not target.exists()
    assert (tmp_path / "rules.md").read_text(encoding="utf-8") == (
        "![map](rules.assets/maps/page.png)\n"
    )
    assert (tmp_path / "rules.assets" / "maps" / "page.png").read_bytes() == b"image"


def test_unpack_skips_existing_outputs_without_force(tmp_path):
    _, target = _packed_pdf(tmp_path)
    markdown = tmp_path / "rules.md"
    markdown.write_text("keep", encoding="utf-8")

    assert unpack([str(tmp_path)], execute=True) == 0
    assert target.exists()
    assert markdown.read_text(encoding="utf-8") == "keep"


def test_unpack_force_replaces_existing_outputs(tmp_path):
    _, target = _packed_pdf(tmp_path)
    markdown = tmp_path / "rules.md"
    markdown.write_text("stale", encoding="utf-8")
    assets = tmp_path / "rules.assets"
    assets.mkdir()
    (assets / "stale.png").write_bytes(b"stale")

    assert unpack([str(tmp_path)], execute=True, force=True) == 0
    assert not target.exists()
    assert markdown.read_text(encoding="utf-8").startswith("![map](rules.assets/")
    assert not (assets / "stale.png").exists()
    assert (assets / "maps" / "page.png").read_bytes() == b"image"


def test_unpack_rejects_traversal_and_retains_archive(tmp_path):
    pdf = tmp_path / "rules.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    target = tmp_path / "rules.textpack"
    with zipfile.ZipFile(target, "w") as archive:
        archive.writestr("text.md", "![x](assets/../../escape.txt)")
        archive.writestr("info.json", json.dumps({"version": 2}))
        archive.writestr("assets/../../escape.txt", b"escape")

    assert unpack([str(tmp_path)], execute=True) == 1
    assert target.exists()
    assert not (tmp_path / "rules.md").exists()
    assert not (tmp_path / "escape.txt").exists()


def test_unpack_accepts_text_markdown_body_name(tmp_path):
    pdf = tmp_path / "rules.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    target = tmp_path / "rules.textpack"
    with zipfile.ZipFile(target, "w") as archive:
        archive.writestr("text.markdown", "body\n")
        archive.writestr("info.json", json.dumps({"version": 2}))

    assert unpack([str(tmp_path)], execute=True) == 0
    assert not target.exists()
    assert (tmp_path / "rules.md").read_text(encoding="utf-8") == "body\n"
    assert not (tmp_path / "rules.assets").exists()
