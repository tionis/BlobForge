import json
from unittest.mock import patch

import pytest

from blobforge.coordinator_client import CoordinatorClient, CoordinatorError


class FakeResponse:
    def __init__(self, payload=None, status=200):
        self.status = status
        self._payload = b"" if payload is None else json.dumps(payload).encode()
        self.length = len(self._payload)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self._payload


class FakeBinaryResponse:
    def __init__(self, payload):
        self._payload = payload
        self._offset = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, size=-1):
        if size < 0:
            size = len(self._payload) - self._offset
        chunk = self._payload[self._offset:self._offset + size]
        self._offset += len(chunk)
        return chunk


def test_obsolete_empty_claim_response_is_rejected():
    client = CoordinatorClient("https://coord.example", "worker-secret")
    with patch("urllib.request.urlopen", return_value=FakeResponse(status=204)):
        with pytest.raises(CoordinatorError, match="invalid claim response"):
            client.claim_job("worker-1", ["3_normal"])


def test_obsolete_direct_claim_response_is_rejected():
    client = CoordinatorClient("https://coord.example", "worker-secret")
    with patch("urllib.request.urlopen", return_value=FakeResponse({"hash": "a" * 64})):
        with pytest.raises(CoordinatorError, match="invalid claim response"):
            client.claim_job("worker-1", ["3_normal"])


def test_claim_envelope_updates_runtime_config():
    client = CoordinatorClient("https://coord.example", "worker-secret")
    payload = {
        "job": {"hash": "a" * 64, "lease_token": "lease-1"},
        "config": {"heartbeat_enabled": False, "heartbeat_interval": 120},
    }
    with patch("urllib.request.urlopen", return_value=FakeResponse(payload)):
        job = client.claim_job("worker-1", ["3_normal"])

    assert job == payload["job"]
    assert client.runtime_config == payload["config"]


def test_empty_claim_envelope_still_updates_runtime_config():
    client = CoordinatorClient("https://coord.example", "worker-secret")
    payload = {"job": None, "config": {"heartbeat_enabled": True, "heartbeat_interval": 300}}
    with patch("urllib.request.urlopen", return_value=FakeResponse(payload)):
        assert client.claim_job("worker-1", ["3_normal"]) is None

    assert client.runtime_config == payload["config"]


def test_worker_identity_comes_from_enrollment_token():
    client = CoordinatorClient("https://coord.example", "bfw_worker-token")
    with patch("urllib.request.urlopen", return_value=FakeResponse({"worker_id": "gpu-worker-123"})) as opened:
        assert client.worker_identity() == "gpu-worker-123"

    request = opened.call_args.args[0]
    assert request.full_url == "https://coord.example/api/v1/workers/me"
    assert request.headers["Authorization"] == "Bearer bfw_worker-token"


def test_output_upload_requests_fresh_lease_bound_url(tmp_path):
    archive = tmp_path / "result.zip"
    archive.write_bytes(b"zip")
    client = CoordinatorClient("https://coord.example", "bfw_worker-token")
    response = FakeResponse({
        "url": "https://s3.example/bucket/result.zip?signed=yes",
        "headers": {"content-type": "application/zip"},
    })

    with patch("urllib.request.urlopen", return_value=response) as opened, patch.object(client, "_stream_put") as streamed:
        client.upload_job_output(
            "a" * 64,
            str(archive),
            worker_id="gpu-worker-123",
            lease_token="lease-1",
        )

    request = opened.call_args.args[0]
    assert json.loads(request.data) == {"worker_id": "gpu-worker-123", "lease_token": "lease-1"}
    streamed.assert_called_once_with(
        "https://s3.example/bucket/result.zip?signed=yes",
        str(archive),
        {"content-type": "application/zip"},
    )


def test_input_download_streams_signed_url_to_disk(tmp_path):
    target = tmp_path / "source.pdf"
    client = CoordinatorClient("https://coord.example", "bfw_worker-token")
    with patch("urllib.request.urlopen", return_value=FakeBinaryResponse(b"%PDF signed content")) as opened:
        client.download_job_input(
            {"input": {"url": "https://s3.example/raw.pdf?signed=yes"}},
            str(target),
        )

    assert target.read_bytes() == b"%PDF signed content"
    assert opened.call_args.args[0].full_url == "https://s3.example/raw.pdf?signed=yes"


def test_bulk_status_uses_single_request_and_dedupes_hashes():
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")
    payload = {"results": {"a" * 64: {"status": "done", "original_name": "book.pdf", "size_bytes": 123}}}
    with patch("urllib.request.urlopen", return_value=FakeResponse(payload)) as opened:
        results = client.check_statuses(["a" * 64, "a" * 64, "b" * 64])

    assert results == payload["results"]
    request = opened.call_args.args[0]
    assert request.full_url == "https://coord.example/api/v1/jobs/status"
    assert json.loads(request.data)["hashes"] == ["a" * 64, "b" * 64]


def test_bulk_status_returns_empty_for_no_hashes():
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")
    assert client.check_statuses([]) == {}


def test_bulk_status_chunks_hashes_to_server_limit_and_reports_progress():
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")
    first = {f"a{i:063d}": {"status": "done", "original_name": "book.pdf", "size_bytes": 1} for i in range(5000)}
    second = {"b" * 64: {"status": "todo", "original_name": "other.pdf", "size_bytes": 2}}
    calls = {"count": 0}

    def urlopen(request, *_args, **_kwargs):
        calls["count"] += 1
        payload = json.loads(request.data)
        hashes = payload["hashes"]
        if calls["count"] == 1:
            return FakeResponse({"results": {h: first[h] for h in hashes}})
        return FakeResponse({"results": {h: second[h] for h in hashes}})

    progress = []
    with patch("urllib.request.urlopen", side_effect=urlopen):
        results = client.check_statuses([*first.keys(), "b" * 64], progress=lambda checked, total: progress.append((checked, total)))

    assert calls["count"] == 2
    assert progress == [(5000, 5001), (5001, 5001)]
    assert len(results) == 5001
    assert results["b" * 64]["status"] == "todo"


def test_output_download_streams_signed_url_to_disk(tmp_path):
    target = tmp_path / "result.zip"
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")
    response = FakeResponse({"url": "https://s3.example/done.zip?signed=yes"})

    with patch("urllib.request.urlopen", side_effect=[response, FakeBinaryResponse(b"zip payload")]) as opened:
        client.download_output("a" * 64, str(target))

    assert target.read_bytes() == b"zip payload"
    assert opened.call_args.args[0].full_url == "https://s3.example/done.zip?signed=yes"


def test_raw_upload_streams_to_signed_url_with_headers(tmp_path):
    pdf = tmp_path / "book.pdf"
    pdf.write_bytes(b"%PDF upload")
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")
    response = FakeResponse({
        "url": "https://s3.example/raw/abc.pdf?signed=yes",
        "already_exists": False,
        "headers": {"content-type": "application/pdf"},
    })

    with patch("urllib.request.urlopen", return_value=response) as opened, patch.object(client, "_stream_put") as streamed:
        client.upload_raw("a" * 64, str(pdf))

    request = opened.call_args.args[0]
    assert request.full_url == f"https://coord.example/api/v1/jobs/{'a' * 64}/raw-upload-url"
    streamed.assert_called_once_with(
        "https://s3.example/raw/abc.pdf?signed=yes",
        str(pdf),
        {"content-type": "application/pdf"},
    )
