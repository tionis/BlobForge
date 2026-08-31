import json
import urllib.error
from unittest.mock import patch

import pytest

from blobforge.coordinator_client import (
    CoordinatorClient,
    CoordinatorError,
    CoordinatorTransferUnavailable,
)


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


def test_claim_advertises_conversion_recipe():
    client = CoordinatorClient("https://coord.example", "worker-secret")
    recipe_digest = "b" * 64
    recipe = {"engine": "marker", "engine_generation": "1"}
    with patch(
        "urllib.request.urlopen",
        return_value=FakeResponse({"job": None, "config": {}}),
    ) as opened:
        client.claim_job(
            "worker-1",
            ["3_normal"],
            recipe_digest=recipe_digest,
            recipe=recipe,
        )

    assert json.loads(opened.call_args.args[0].data) == {
        "worker_id": "worker-1",
        "priorities": ["3_normal"],
        "recipe_digest": recipe_digest,
        "recipe": recipe,
    }


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


def test_input_download_classifies_network_unavailability(tmp_path):
    client = CoordinatorClient("https://coord.example", "bfw_worker-token")
    with patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.URLError(OSError(101, "Network is unreachable")),
    ):
        with pytest.raises(CoordinatorTransferUnavailable, match="Network is unreachable"):
            client.download_job_input(
                {"input": {"url": "https://coord.example/signed-source"}},
                str(tmp_path / "source.pdf"),
            )


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


def test_output_download_can_select_recipe(tmp_path):
    target = tmp_path / "result.zip"
    recipe_digest = "c" * 64
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")
    response = FakeResponse({"url": "https://s3.example/recipe.zip?signed=yes"})

    with patch(
        "urllib.request.urlopen",
        side_effect=[response, FakeBinaryResponse(b"recipe payload")],
    ) as opened:
        client.download_output("a" * 64, str(target), recipe_digest)

    assert target.read_bytes() == b"recipe payload"
    assert json.loads(opened.call_args_list[0].args[0].data) == {
        "recipe_digest": recipe_digest
    }


def test_sync_done_hashes_paginates_until_complete_and_reports_progress():
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")
    page1 = [f"a{i:063d}" for i in range(5000)]
    page2 = ["b" * 64]
    calls = {"count": 0}
    urls = []

    def urlopen(request, *_args, **_kwargs):
        calls["count"] += 1
        urls.append(request.full_url)
        if calls["count"] == 1:
            return FakeResponse({
                "hashes": page1,
                "next_since": 1000,
                "next_cursor": page1[-1],
                "complete": False,
            })
        return FakeResponse({
            "hashes": page2,
            "next_since": 1001,
            "next_cursor": page2[0],
            "complete": True,
        })

    progress = []
    with patch("urllib.request.urlopen", side_effect=urlopen):
        hashes, next_since, next_cursor = client.sync_done_hashes(
            500, "c" * 64, progress=lambda n: progress.append(n)
        )

    assert calls["count"] == 2
    assert "since=500" in urls[0] and f"cursor={'c' * 64}" in urls[0] and "limit=5000" in urls[0]
    assert "since=1000" in urls[1] and urls[1].endswith(f"cursor={page1[-1]}&limit=5000")
    assert hashes == page1 + page2
    assert next_since == 1001
    assert next_cursor == page2[0]
    assert progress == [len(page1), len(page1) + len(page2)]


def test_sync_done_hashes_single_page_with_defaults():
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")
    hashes = ["a" * 64]
    with patch("urllib.request.urlopen", return_value=FakeResponse({
        "hashes": hashes,
        "next_since": 42,
        "next_cursor": hashes[0],
        "complete": True,
    })) as opened:
        result, next_since, next_cursor = client.sync_done_hashes()

    assert result == hashes
    assert next_since == 42
    assert next_cursor == hashes[0]
    url = opened.call_args.args[0].full_url
    assert url == "https://coord.example/api/v1/jobs/done-since?since=0&cursor=&limit=5000"


def test_sync_done_hashes_refuses_to_loop_without_advance():
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")
    stalled = {
        "hashes": [],
        "next_since": 100,
        "next_cursor": "a" * 64,
        "complete": False,
    }
    with patch("urllib.request.urlopen", return_value=FakeResponse(stalled)):
        with pytest.raises(CoordinatorError, match="did not advance"):
            client.sync_done_hashes(100, "a" * 64)


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


def test_raw_upload_can_reuse_an_existing_transfer(tmp_path):
    pdf = tmp_path / "book.pdf"
    pdf.write_bytes(b"%PDF upload")
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")
    transfer = {
        "url": "https://s3.example/raw/abc.pdf?signed=yes",
        "already_exists": False,
        "headers": {"content-type": "application/pdf"},
    }

    with patch.object(client, "raw_upload_url") as requested, patch.object(client, "_stream_put") as streamed:
        client.upload_raw("a" * 64, str(pdf), transfer=transfer)

    requested.assert_not_called()
    streamed.assert_called_once_with(transfer["url"], str(pdf), transfer["headers"])


def test_admin_source_upload_streams_body_and_metadata(tmp_path):
    pdf = tmp_path / "rule book.pdf"
    pdf.write_bytes(b"%PDF upload")
    client = CoordinatorClient("https://coord.example", "bfa_admin-token")

    class Response:
        status = 200
        reason = "OK"

        @staticmethod
        def read():
            return json.dumps({"hash": "a" * 64, "status": "todo"}).encode()

    class Connection:
        instance = None

        def __init__(self, host, port, timeout):
            self.host, self.port, self.timeout = host, port, timeout
            self.headers = {}
            self.sent = b""
            Connection.instance = self

        def putrequest(self, method, path):
            self.method, self.path = method, path

        def putheader(self, name, value):
            self.headers[name] = value

        def endheaders(self):
            pass

        def send(self, chunk):
            self.sent += chunk

        def getresponse(self):
            return Response()

        def close(self):
            pass

    with patch("http.client.HTTPSConnection", Connection):
        result = client.upload_admin_source(
            str(pdf), filename=pdf.name, media_type="application/pdf",
            priority="1_urgent", tags=["rulebook", "test"],
            recipe_digest="blake3:" + "b" * 64,
        )

    connection = Connection.instance
    assert result["status"] == "todo"
    assert connection.method == "POST"
    assert connection.host == "coord.example"
    assert "filename=rule+book.pdf" in connection.path
    assert "priority=1_urgent" in connection.path
    assert "tags=rulebook%2Ctest" in connection.path
    assert connection.headers["Authorization"] == "Bearer bfa_admin-token"
    assert connection.headers["Content-Length"] == str(pdf.stat().st_size)
    assert connection.sent == pdf.read_bytes()
