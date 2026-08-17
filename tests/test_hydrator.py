import io
import json
import zipfile

from blobforge.config import S3_PREFIX_DONE
from blobforge.hash_index import HashIndex
from blobforge.hydrator import hydrate
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
    def __init__(self, statuses_by_hash, archives_by_hash):
        self.statuses_by_hash = statuses_by_hash
        self.archives_by_hash = archives_by_hash
        self.status_calls = 0
        self.download_calls = []

    def check_statuses(self, hashes, progress=None):
        self.status_calls += 1
        if progress:
            progress(len(hashes), len(hashes))
        return {file_hash: {"status": self.statuses_by_hash.get(file_hash, "todo")} for file_hash in hashes}

    def download_output(self, file_hash, local_path):
        self.download_calls.append(file_hash)
        with open(local_path, "wb") as handle:
            handle.write(self.archives_by_hash[file_hash])


def test_hydrate_uses_coordinator_bulk_status(tmp_path):
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

    assert coordinator.status_calls == 1
    assert coordinator.download_calls == [done_hash]

    assert (tmp_path / "done.md").exists()
    assert not (tmp_path / "pending.md").exists()
    assert (tmp_path / "done.assets" / "image.png").read_bytes() == b"coordinator-image"


def test_hydrate_skips_status_query_for_known_done_hashes(tmp_path):
    pdf_done = tmp_path / "done.pdf"
    _write_pdf(pdf_done, b"%PDF-1.4\ndone\n%%EOF\n")
    done_hash = compute_sha256_with_cache(str(pdf_done))

    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    index.set_status(done_hash, True)

    archive_data = _build_conversion_zip(
        markdown_text="![img](assets/image.png)\n",
        assets={"image.png": b"coordinator-image"},
    )
    coordinator = FakeCoordinator({done_hash: "done"}, {done_hash: archive_data})

    result = hydrate([str(tmp_path)], client=coordinator, index=index)
    assert result == 0

    assert coordinator.status_calls == 0
    assert coordinator.download_calls == [done_hash]
    assert (tmp_path / "done.md").exists()


def test_hydrate_requeries_only_after_status_ttl_expires(tmp_path):
    pdf = tmp_path / "pending.pdf"
    _write_pdf(pdf, b"%PDF-1.4\npending\n%%EOF\n")
    file_hash = compute_sha256_with_cache(str(pdf))

    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    index.set_status(file_hash, False)
    coordinator = FakeCoordinator({}, {})

    result = hydrate([str(tmp_path)], client=coordinator, index=index, status_ttl_seconds=3600)
    assert result == 0
    assert coordinator.status_calls == 0  # missing still within TTL -> no query
    assert not (tmp_path / "pending.md").exists()

    index.set_statuses({file_hash: False})
    coordinator.status_calls = 0
    result = hydrate([str(tmp_path)], client=coordinator, index=index, status_ttl_seconds=0.0001)
    assert result == 0
    assert coordinator.status_calls == 1  # stale -> re-queried


def test_hydrate_refresh_status_forces_query_even_when_cached(tmp_path):
    pdf = tmp_path / "pending.pdf"
    _write_pdf(pdf, b"%PDF-1.4\npending\n%%EOF\n")
    file_hash = compute_sha256_with_cache(str(pdf))

    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    index.set_status(file_hash, False)
    coordinator = FakeCoordinator({}, {})

    result = hydrate([str(tmp_path)], client=coordinator, index=index, refresh_status=True)
    assert result == 0
    assert coordinator.status_calls == 1
    assert not (tmp_path / "pending.md").exists()


def test_hydrate_reuses_indexed_hash_without_reading_file(tmp_path, monkeypatch):
    pdf = tmp_path / "known.pdf"
    _write_pdf(pdf, b"%PDF-1.4\nknown\n%%EOF\n")
    stat_result = pdf.stat()
    file_hash = compute_sha256_with_cache(str(pdf))

    index = HashIndex(db_path=str(tmp_path / "index.sqlite3"))
    index.set_file_hash(str(pdf), stat_result.st_size, stat_result.st_mtime_ns, file_hash)
    index.set_status(file_hash, True)

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
    assert coordinator.status_calls == 0
    assert (tmp_path / "known.md").exists()
