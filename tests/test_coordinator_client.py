import json
from unittest.mock import patch

from blobforge.coordinator_client import CoordinatorClient


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


def test_empty_claim_returns_none():
    client = CoordinatorClient("https://coord.example", "worker-secret")
    with patch("urllib.request.urlopen", return_value=FakeResponse(status=204)):
        assert client.claim_job("worker-1", ["3_normal"]) is None


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
