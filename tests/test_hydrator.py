import io
import json
import zipfile

from blobforge.config import S3_PREFIX_DONE
from blobforge.hydrator import hydrate
from blobforge.utils import compute_sha256_with_cache


class FakeS3:
    def __init__(self, archives_by_hash, manifest_hashes=None):
        self.archives_by_hash = archives_by_hash
        self.manifest_hashes = set(manifest_hashes) if manifest_hashes is not None else set(archives_by_hash.keys())
        self.exists_calls = []
        self.download_calls = []
        self.manifest_calls = 0

    def get_manifest(self):
        self.manifest_calls += 1
        return {
            "version": 1,
            "updated_at": None,
            "entries": {h: {"paths": []} for h in self.manifest_hashes},
        }

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

    result = hydrate([str(tmp_path)], s3=s3)
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
    result = hydrate([str(tmp_path)], s3=s3, force=False)

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

    result = hydrate([str(tmp_path)], s3=s3)
    assert result == 0

    assert len(s3.download_calls) == 1
    assert s3.download_calls[0] == shared_hash

    assert "(alpha.assets/image.png)" in (tmp_path / "alpha.md").read_text(encoding="utf-8")
    assert "(beta.assets/image.png)" in (tmp_path / "beta.md").read_text(encoding="utf-8")
    assert (tmp_path / "alpha.assets" / "image.png").read_bytes() == b"same-image"
    assert (tmp_path / "beta.assets" / "image.png").read_bytes() == b"same-image"


def test_hydrate_prefilters_hashes_using_manifest(tmp_path):
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
    s3 = FakeS3(
        {known_hash: archive_data},
        manifest_hashes={known_hash},  # unknown_hash intentionally excluded
    )

    result = hydrate([str(tmp_path)], s3=s3)
    assert result == 0

    # Exists check should only happen for the manifest-known hash.
    assert len(s3.exists_calls) == 1
    assert known_hash in s3.exists_calls[0]
    assert unknown_hash not in s3.exists_calls[0]
    assert s3.manifest_calls == 1

    assert (tmp_path / "known.md").exists()
    assert not (tmp_path / "unknown.md").exists()
