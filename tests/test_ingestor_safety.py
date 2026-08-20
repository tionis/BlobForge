import hashlib
from unittest.mock import MagicMock, patch

from blobforge.coordinator_client import CoordinatorError
from blobforge.ingestor import ingest


def _coordinator():
    coordinator = MagicMock()
    coordinator.available = True
    coordinator.raw_upload_url.return_value = {
        "url": "https://objects.example/raw.pdf?signed=yes",
        "already_exists": False,
        "headers": {"content-type": "application/pdf"},
    }
    coordinator.enqueue.return_value = {"status": "todo", "priority": "3_normal"}
    return coordinator


def test_dry_run_never_uploads_or_enqueues(tmp_path):
    pdf = tmp_path / "book.pdf"
    pdf.write_bytes(b"%PDF-1.7 dry run")
    coordinator = _coordinator()

    with patch("blobforge.ingestor.CoordinatorClient", return_value=coordinator):
        ingest([str(pdf)], dry_run=True)

    coordinator.upload_raw.assert_not_called()
    coordinator.enqueue.assert_not_called()


def test_upload_reuses_checked_transfer_and_enqueues(tmp_path):
    pdf = tmp_path / "book.pdf"
    pdf.write_bytes(b"%PDF-1.7 upload")
    coordinator = _coordinator()
    transfer = coordinator.raw_upload_url.return_value
    file_hash = hashlib.sha256(pdf.read_bytes()).hexdigest()

    with patch("blobforge.ingestor.CoordinatorClient", return_value=coordinator):
        ingest([str(pdf)])

    coordinator.upload_raw.assert_called_once_with(
        file_hash,
        str(pdf),
        transfer=transfer,
    )
    assert coordinator.enqueue.call_args.kwargs["size_bytes"] == pdf.stat().st_size


def test_existing_orphaned_raw_pdf_is_enqueued_without_job_lookup(tmp_path):
    pdf = tmp_path / "book.pdf"
    pdf.write_bytes(b"%PDF-1.7 orphan")
    coordinator = _coordinator()
    coordinator.raw_upload_url.return_value["already_exists"] = True

    with patch("blobforge.ingestor.CoordinatorClient", return_value=coordinator):
        ingest([str(pdf)])

    coordinator.get_job.assert_not_called()
    coordinator.enqueue.assert_called_once()
    assert coordinator.enqueue.call_args.kwargs["size_bytes"] == pdf.stat().st_size


def test_existing_orphaned_lfs_raw_is_enqueued_with_unknown_size(tmp_path):
    file_hash = "a" * 64
    pointer = tmp_path / "book.pdf"
    pointer.write_text(
        "version https://git-lfs.github.com/spec/v1\n"
        f"oid sha256:{file_hash}\n"
        "size 1234\n"
    )
    coordinator = _coordinator()
    coordinator.raw_upload_url.return_value["already_exists"] = True
    coordinator.get_job.side_effect = CoordinatorError("not found", status=404)

    with patch("blobforge.ingestor.CoordinatorClient", return_value=coordinator):
        ingest([str(pointer)])

    coordinator.enqueue.assert_called_once_with(
        file_hash,
        priority="3_normal",
        original_name="book.pdf",
        size_bytes=0,
        paths=["book.pdf"],
        tags=["book"],
        source=tmp_path.name,
    )
