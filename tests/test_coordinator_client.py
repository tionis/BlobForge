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


def test_migration_uses_separate_token():
    client = CoordinatorClient("https://coord.example", "worker-secret")
    with patch("urllib.request.urlopen", return_value=FakeResponse({"imported": 1})) as opened:
        result = client.import_items([{"hash": "a" * 64, "status": "todo"}], "migration-secret")

    request = opened.call_args.args[0]
    assert request.headers["Authorization"] == "Bearer migration-secret"
    assert result == {"imported": 1}


def test_empty_claim_returns_none():
    client = CoordinatorClient("https://coord.example", "worker-secret")
    with patch("urllib.request.urlopen", return_value=FakeResponse(status=204)):
        assert client.claim_job("worker-1", ["3_normal"]) is None
