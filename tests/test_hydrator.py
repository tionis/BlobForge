import io
import json
import zipfile

import pytest

from blobforge.config import S3_PREFIX_DONE
from blobforge.coordinator_client import CoordinatorError
from blobforge.hash_index import HashIndex
from blobforge.hydrator import hydrate, select_artifact
from blobforge.mdaf import MdafMemberInput, MdafSource, build_mdaf
from blobforge.mdaf.builder import activity
from blobforge.utils import compute_sha256_with_cache


class FakeS3:
    def __init__(self, archives_by_hash):
        self.archives_by_hash = archives_by_hash
        self.exists_calls = []
        self.download_calls = []

    def exists(self, key):
        self.exists_calls.append(key)
        prefix = f"{S3_PREFIX_DONE}/"
        if not key.startswith(prefix) or not key.endswith(".zip"):
            return False
        file_hash = key[len(prefix):-4]
        return file_hash in self.archives_by_hash

    def download_file(self, key, local_path):
        prefix = f"{S3_PREFIX_DONE}/"
        file_hash = key[len(prefix):-4]
        self.download_calls.append(file_hash)
        with open(local_path, "wb") as handle:
            handle.write(self.archives_by_hash[file_hash])


def _write_pdf(path, content=b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n"):
    path.write_bytes(content)


def _build_conversion_zip(markdown_text, assets):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("content.md", markdown_text)
        archive.writestr("info.json", json.dumps({"test": True}))
        for relative_path, payload in assets.items():
            archive.writestr(f"assets/{relative_path}", payload)
    return buffer.getvalue()


def _build_mdaf(tmp_path, markdown_text, assets=None):
    extra_members = [
        MdafMemberInput(
            path=f"assets/{name}", data=payload, role="asset",
            created_by="activity:extract", media_type="application/octet-stream",
        )
        for name, payload in (assets or {}).items()
    ]
    result = build_mdaf(
        tmp_path / "fixture.mdaf",
        text=markdown_text,
        sources=[MdafSource("document", "application/pdf", "blake3:" + "0" * 64)],
        activities=[activity(
            activity_id="activity:extract", kind="document-extraction",
            tools=[{"name": "test", "version": "1"}], inputs=["source:document"],
            outputs=["text.md", "provenance.json", *[member.path for member in extra_members]],
            parameters={},
        )],
        producer={"name": "blobforge-tests", "version": "1"},
        extra_members=extra_members,
    )
    return result.path.read_bytes(), result.identity


class ArtifactCoordinator:
    def __init__(self, statuses_by_hash, archives_by_recipe):
        self.statuses_by_hash = statuses_by_hash
        self.archives_by_recipe = archives_by_recipe
        self.download_calls = []

    def check_statuses(self, hashes, progress=None):
        values = list(hashes)
        if progress:
            progress(len(values), len(values))
        return {
            value: self.statuses_by_hash.get(
                value, {"status": "missing", "artifacts": []}
            )
            for value in values
        }

    def download_output(self, file_hash, local_path, recipe_digest=None):
        self.download_calls.append((file_hash, recipe_digest))
        with open(local_path, "wb") as handle:
            handle.write(self.archives_by_recipe[(file_hash, recipe_digest)])


def test_hydrate_materializes_markdown_and_assets(tmp_path):
    pdf_path = tmp_path / "rules.pdf"
    _write_pdf(pdf_path)
    file_hash = compute_sha256_with_cache(str(pdf_path))

    archive_data = _build_conversion_zip(
        markdown_text="![img](assets/page-1.png)\n",
        assets={"page-1.png": b"image-data"},
    )
    s3 = FakeS3({file_hash: archive_data})

    result = hydrate([str(tmp_path)], client=s3)
    assert result == 0

    markdown_path = tmp_path / "rules.md"
    assert markdown_path.exists()
    markdown_text = markdown_path.read_text(encoding="utf-8")
    assert "(rules.assets/page-1.png)" in markdown_text

    asset_path = tmp_path / "rules.assets" / "page-1.png"
    assert asset_path.exists()
    assert asset_path.read_bytes() == b"image-data"


def test_hydrate_reads_validated_mdaf_text_and_assets(tmp_path):
    pdf_path = tmp_path / "rules.pdf"
    _write_pdf(pdf_path)
    file_hash = compute_sha256_with_cache(str(pdf_path))
    archive, identity = _build_mdaf(
        tmp_path, "![img](assets/page.png)\n", {"page.png": b"image"}
    )
    recipe = "blake3:" + "a" * 64
    coordinator = ArtifactCoordinator(
        {file_hash: {
            "status": "todo", "recipe_digest": recipe,
            "artifacts": [{
                "recipe_digest": recipe, "artifact_type": "mdaf/v1",
                "identity": identity,
            }],
        }},
        {(file_hash, recipe): archive},
    )

    assert hydrate([str(pdf_path)], client=coordinator) == 0
    assert (tmp_path / "rules.md").read_text() == "![img](rules.assets/page.png)\n"
    assert (tmp_path / "rules.assets" / "page.png").read_bytes() == b"image"
    assert coordinator.download_calls == [(file_hash, recipe)]


def test_hydrate_can_write_textpack_directly_from_mdaf(tmp_path):
    pdf_path = tmp_path / "rules.pdf"
    _write_pdf(pdf_path)
    file_hash = compute_sha256_with_cache(str(pdf_path))
    archive, identity = _build_mdaf(
        tmp_path, "![img](assets/page.png)\n", {"page.png": b"image"}
    )
    recipe = "blake3:" + "b" * 64
    coordinator = ArtifactCoordinator(
        {file_hash: {
            "status": "failed", "recipe_digest": recipe,
            "artifacts": [{
                "recipe_digest": recipe, "artifact_type": "mdaf/v1",
                "identity": identity,
            }],
        }},
        {(file_hash, recipe): archive},
    )

    assert hydrate([str(pdf_path)], client=coordinator, output_format="textpack") == 0
    assert not (tmp_path / "rules.md").exists()
    assert not (tmp_path / "rules.assets").exists()
    with zipfile.ZipFile(tmp_path / "rules.textpack") as textpack:
        assert textpack.read("text.md") == b"![img](assets/page.png)\n"
        assert textpack.read("assets/page.png") == b"image"
        metadata = json.loads(textpack.read("info.json"))["dev.tionis.blobforge"]
        assert metadata["artifactIdentity"] == identity
        assert metadata["recipeDigest"] == recipe
        assert metadata["artifactType"] == "mdaf/v1"


def test_artifact_selection_fails_closed_when_multiple_outputs_are_ambiguous():
    first = "blake3:" + "1" * 64
    second = "blake3:" + "2" * 64
    status = {"status": "todo", "recipe_digest": None, "artifacts": [
        {"recipe_digest": first}, {"recipe_digest": second},
    ]}

    with pytest.raises(RuntimeError, match="multiple retained artifacts"):
        select_artifact(status)
    assert select_artifact(status, second)["recipe_digest"] == second


def test_hydrate_skips_when_markdown_exists_without_force(tmp_path):
    pdf_path = tmp_path / "existing.pdf"
    _write_pdf(pdf_path)

    markdown_path = tmp_path / "existing.md"
    markdown_path.write_text("already here\n", encoding="utf-8")

    s3 = FakeS3({})
    result = hydrate([str(tmp_path)], client=s3, force=False)

    assert result == 0
    assert markdown_path.read_text(encoding="utf-8") == "already here\n"
    assert s3.exists_calls == []
    assert s3.download_calls == []


def test_hydrate_downloads_once_for_duplicate_hashes(tmp_path):
    pdf_a = tmp_path / "alpha.pdf"
    pdf_b = tmp_path / "beta.pdf"
    shared_pdf_bytes = b"%PDF-1.4\nsame-content\n%%EOF\n"
    _write_pdf(pdf_a, shared_pdf_bytes)
    _write_pdf(pdf_b, shared_pdf_bytes)

    shared_hash = compute_sha256_with_cache(str(pdf_a))
    archive_data = _build_conversion_zip(
        markdown_text="![img](assets/image.png)\n",
        assets={"image.png": b"same-image"},
    )
    s3 = FakeS3({shared_hash: archive_data})

    result = hydrate([str(tmp_path)], client=s3)
    assert result == 0

    assert len(s3.download_calls) == 1
    assert s3.download_calls[0] == shared_hash

    assert "(alpha.assets/image.png)" in (tmp_path / "alpha.md").read_text(encoding="utf-8")
    assert "(beta.assets/image.png)" in (tmp_path / "beta.md").read_text(encoding="utf-8")
    assert (tmp_path / "alpha.assets" / "image.png").read_bytes() == b"same-image"
    assert (tmp_path / "beta.assets" / "image.png").read_bytes() == b"same-image"


def test_hydrate_checks_each_unique_hash_for_completed_output(tmp_path):
    pdf_known = tmp_path / "known.pdf"
    pdf_unknown = tmp_path / "unknown.pdf"
    _write_pdf(pdf_known, b"%PDF-1.4\nknown\n%%EOF\n")
    _write_pdf(pdf_unknown, b"%PDF-1.4\nunknown\n%%EOF\n")

    known_hash = compute_sha256_with_cache(str(pdf_known))
    unknown_hash = compute_sha256_with_cache(str(pdf_unknown))

    archive_data = _build_conversion_zip(
        markdown_text="![img](assets/p.png)\n",
        assets={"p.png": b"pixel"},
    )
    s3 = FakeS3({known_hash: archive_data})

    result = hydrate([str(tmp_path)], client=s3)
    assert result == 0

    assert len(s3.exists_calls) == 2
    assert any(known_hash in key for key in s3.exists_calls)
    assert any(unknown_hash in key for key in s3.exists_calls)

    assert (tmp_path / "known.md").exists()
    assert not (tmp_path / "unknown.md").exists()


class FakeCoordinator:
    def __init__(self, statuses_by_hash, archives_by_hash, base_url=""):
        self.statuses_by_hash = statuses_by_hash
        self.archives_by_hash = archives_by_hash
        self.base_url = base_url
        self.sync_calls = 0
        self.download_calls = []

    def sync_done_hashes(self, since_ms=0, cursor="", progress=None):
        self.sync_calls += 1
        hashes = [h for h, s in self.statuses_by_hash.items() if s == "done"]
        hashes.sort()
        if progress:
            progress(len(hashes))
        next_since = since_ms
        next_cursor = cursor
        for file_hash in hashes:
            next_since += 1
            next_cursor = file_hash
        return hashes, next_since, next_cursor

    def download_output(self, file_hash, local_path):
        self.download_calls.append(file_hash)
        with open(local_path, "wb") as handle:
            handle.write(self.archives_by_hash[file_hash])


def test_hydrate_uses_coordinator_watermark_sync(tmp_path):
    pdf_done = tmp_path / "done.pdf"
    pdf_todo = tmp_path / "pending.pdf"
    _write_pdf(pdf_done, b"%PDF-1.4\ndone\n%%EOF\n")
    _write_pdf(pdf_todo, b"%PDF-1.4\npending\n%%EOF\n")

    done_hash = compute_sha256_with_cache(str(pdf_done))
    todo_hash = compute_sha256_with_cache(str(pdf_todo))

    archive_data = _build_conversion_zip(
        markdown_text="![img](assets/image.png)\n",
        assets={"image.png": b"coordinator-image"},
    )
    coordinator = FakeCoordinator({done_hash: "done", todo_hash: "todo"}, {done_hash: archive_data})

    result = hydrate([str(tmp_path)], client=coordinator)
    assert result == 0

    assert coordinator.sync_calls == 1
    assert coordinator.download_calls == [done_hash]

    assert (tmp_path / "done.md").exists()
    assert not (tmp_path / "pending.md").exists()
    assert (tmp_path / "done.assets" / "image.png").read_bytes() == b"coordinator-image"


def test_hydrate_syncs_done_set_once_and_reuses_mirror(tmp_path):
    pdf_done = tmp_path / "done.pdf"
    _write_pdf(pdf_done, b"%PDF-1.4\ndone\n%%EOF\n")
    done_hash = compute_sha256_with_cache(str(pdf_done))

    archive_data = _build_conversion_zip(
        markdown_text="![img](assets/image.png)\n",
        assets={"image.png": b"coordinator-image"},
    )
    coordinator = FakeCoordinator({done_hash: "done"}, {done_hash: archive_data})
    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))

    result = hydrate([str(tmp_path)], client=coordinator, index=index)
    assert result == 0
    assert coordinator.sync_calls == 1
    assert index.is_done(done_hash) is True
    assert index.done_count() == 1

    # A second run needs no sync: markdown already exists and no work remains.
    coordinator.sync_calls = 0
    coordinator.download_calls = []
    result = hydrate([str(tmp_path)], client=coordinator, index=index)
    assert result == 0
    assert coordinator.sync_calls == 0
    assert coordinator.download_calls == []


def test_hydrate_refresh_status_resets_done_mirror(tmp_path):
    pdf = tmp_path / "known.pdf"
    _write_pdf(pdf, b"%PDF-1.4\nknown\n%%EOF\n")
    file_hash = compute_sha256_with_cache(str(pdf))

    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    index.add_done_hashes([file_hash])
    index.set_watermark(999, "a" * 64)

    coordinator = FakeCoordinator({}, {})

    result = hydrate([str(tmp_path)], client=coordinator, index=index, refresh_status=True)
    assert result == 0
    assert coordinator.sync_calls == 1
    assert index.done_count() == 0  # mirror rebuilt from empty coordinator result
    assert index.get_watermark() == (0, "")  # watermark reset and re-advanced from zero

    assert not (tmp_path / "known.md").exists()  # coordinator reported nothing done


def test_hydrate_reuses_indexed_hash_without_reading_file(tmp_path, monkeypatch):
    pdf = tmp_path / "known.pdf"
    _write_pdf(pdf, b"%PDF-1.4\nknown\n%%EOF\n")
    stat_result = pdf.stat()
    file_hash = compute_sha256_with_cache(str(pdf))

    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    index.set_file_hash(str(pdf), stat_result.st_size, stat_result.st_mtime_ns, file_hash)
    index.add_done_hashes([file_hash])

    archive_data = _build_conversion_zip(
        markdown_text="![img](assets/image.png)\n",
        assets={"image.png": b"coordinator-image"},
    )
    coordinator = FakeCoordinator({file_hash: "done"}, {file_hash: archive_data})

    def _fail_if_read(*_args, **_kwargs):
        raise AssertionError("hash was computed from disk instead of the index")

    monkeypatch.setattr("blobforge.hydrator.compute_sha256_with_cache", _fail_if_read)

    result = hydrate([str(tmp_path)], client=coordinator, index=index)
    assert result == 0
    assert coordinator.download_calls == [file_hash]
    assert (tmp_path / "known.md").exists()


def test_hydrate_persists_newly_computed_hash_for_next_run(tmp_path, monkeypatch):
    pdf = tmp_path / "missing.pdf"
    _write_pdf(pdf, b"%PDF-1.4\nmissing\n%%EOF\n")
    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    coordinator = FakeCoordinator({}, {})
    calls = 0
    original_compute = compute_sha256_with_cache

    def _count_compute(path):
        nonlocal calls
        calls += 1
        return original_compute(path)

    monkeypatch.setattr("blobforge.hydrator.compute_sha256_with_cache", _count_compute)

    assert hydrate([str(pdf)], client=coordinator, index=index) == 0
    assert hydrate([str(pdf)], client=coordinator, index=index) == 0
    assert calls == 1
    assert index.file_hashes()[str(pdf)] == original_compute(str(pdf))


def test_hydrate_keeps_coordinator_done_mirrors_separate(tmp_path):
    pdf_a = tmp_path / "a.pdf"
    pdf_b = tmp_path / "b.pdf"
    _write_pdf(pdf_a, b"%PDF-1.4\na\n%%EOF\n")
    _write_pdf(pdf_b, b"%PDF-1.4\nb\n%%EOF\n")
    hash_a = compute_sha256_with_cache(str(pdf_a))
    hash_b = compute_sha256_with_cache(str(pdf_b))
    archive = _build_conversion_zip("content\n", {})
    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    coordinator_a = FakeCoordinator(
        {hash_a: "done"}, {hash_a: archive}, "HTTPS://COORD-A.example/"
    )
    coordinator_b = FakeCoordinator(
        {hash_b: "done"}, {hash_b: archive}, "https://coord-b.example"
    )

    assert hydrate([str(pdf_a)], client=coordinator_a, index=index) == 0
    assert hydrate([str(pdf_b)], client=coordinator_b, index=index) == 0

    assert index.is_done(hash_a, "https://coord-a.example") is True
    assert index.is_done(hash_a, "https://coord-b.example") is False
    assert index.is_done(hash_b, "https://coord-b.example") is True


def test_hydrate_redownloads_when_markdown_is_deleted(tmp_path):
    import shutil
    pdf = tmp_path / "deleted.pdf"
    _write_pdf(pdf, b"%PDF-1.4\ndeleted\n%%EOF\n")
    stat_result = pdf.stat()
    file_hash = compute_sha256_with_cache(str(pdf))

    archive_data = _build_conversion_zip(
        markdown_text="![img](assets/image.png)\n",
        assets={"image.png": b"coordinator-image"},
    )
    coordinator = FakeCoordinator({file_hash: "done"}, {file_hash: archive_data})
    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    index.set_file_hash(str(pdf), stat_result.st_size, stat_result.st_mtime_ns, file_hash)
    index.add_done_hashes([file_hash])

    result = hydrate([str(tmp_path)], client=coordinator, index=index)
    assert result == 0
    assert coordinator.download_calls == [file_hash]
    assert (tmp_path / "deleted.md").exists()
    assert (tmp_path / "deleted.assets" / "image.png").read_bytes() == b"coordinator-image"

    # Simulate the user deleting the hydrated output, then hydrate again: the
    # done mirror still knows the conversion, so it must re-download and
    # re-hydrate without re-querying the coordinator's done-set.
    (tmp_path / "deleted.md").unlink()
    shutil.rmtree(tmp_path / "deleted.assets")
    coordinator.download_calls = []
    coordinator.sync_calls = 0

    result = hydrate([str(tmp_path)], client=coordinator, index=index)
    assert result == 0
    assert coordinator.sync_calls == 1  # delta re-synced as usual, membership still True
    assert coordinator.download_calls == [file_hash]
    assert (tmp_path / "deleted.md").exists()
    assert (tmp_path / "deleted.assets" / "image.png").read_bytes() == b"coordinator-image"


def test_hydrate_keeps_done_hash_on_transient_download_error(tmp_path):
    pdf = tmp_path / "transient.pdf"
    _write_pdf(pdf, b"%PDF-1.4\ntransient\n%%EOF\n")
    stat_result = pdf.stat()
    file_hash = compute_sha256_with_cache(str(pdf))

    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    index.set_file_hash(str(pdf), stat_result.st_size, stat_result.st_mtime_ns, file_hash)
    index.add_done_hashes([file_hash])

    class FlakyCoordinator:
        def __init__(self):
            self.attempts = 0

        def sync_done_hashes(self, since_ms=0, cursor="", progress=None):
            return [], since_ms, cursor

        def download_output(self, file_hash, local_path):
            self.attempts += 1
            raise CoordinatorError("Output download failed: temporary network issue")

    coordinator = FlakyCoordinator()
    result = hydrate([str(tmp_path)], client=coordinator, index=index)
    assert result == 1
    assert coordinator.attempts == 1
    assert index.is_done(file_hash) is True  # transient -> mirror entry preserved


def test_hydrate_drops_done_hash_when_output_definitively_gone(tmp_path):
    pdf = tmp_path / "gone.pdf"
    _write_pdf(pdf, b"%PDF-1.4\ngone\n%%EOF\n")
    stat_result = pdf.stat()
    file_hash = compute_sha256_with_cache(str(pdf))

    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    index.set_file_hash(str(pdf), stat_result.st_size, stat_result.st_mtime_ns, file_hash)
    index.add_done_hashes([file_hash])

    class GoneCoordinator:
        def sync_done_hashes(self, since_ms=0, cursor="", progress=None):
            return [], since_ms, cursor

        def download_output(self, file_hash, local_path):
            raise CoordinatorError("Completed output not available", status=409)

    coordinator = GoneCoordinator()
    result = hydrate([str(tmp_path)], client=coordinator, index=index)
    assert result == 1
    assert index.is_done(file_hash) is False  # 409 -> mirror entry dropped
