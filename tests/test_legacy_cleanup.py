from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from blobforge.cli import cmd_cleanup_legacy
from blobforge.s3_client import S3Client


def test_purge_prefix_previews_without_deleting():
    client = S3Client.__new__(S3Client)
    client.mock = False
    client.s3 = MagicMock()
    client.s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "pdf/queue/a"}, {"Key": "pdf/queue/b"}]},
    ]

    result = client.purge_prefix("pdf/queue/", dry_run=True)

    assert result == {
        "prefix": "pdf/queue/",
        "count": 2,
        "deleted": 0,
        "preview": ["pdf/queue/a", "pdf/queue/b"],
    }
    client.s3.delete_objects.assert_not_called()


def test_purge_prefix_deletes_listed_objects_in_batches():
    client = S3Client.__new__(S3Client)
    client.mock = False
    client.s3 = MagicMock()
    client.s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": f"pdf/registry/{index}"} for index in range(1001)]},
    ]
    client.s3.delete_objects.return_value = {}

    result = client.purge_prefix("pdf/registry/", dry_run=False)

    assert result["count"] == 1001
    assert result["deleted"] == 1001
    assert client.s3.delete_objects.call_count == 2


def test_cleanup_command_is_dry_run_by_default():
    store = MagicMock()
    store.purge_prefix.side_effect = [
        {"prefix": "pdf/queue/", "count": 2, "deleted": 0, "preview": []},
        {"prefix": "pdf/registry/", "count": 1, "deleted": 0, "preview": []},
    ]
    with patch("blobforge.cli.S3Client", return_value=store):
        assert cmd_cleanup_legacy(SimpleNamespace(execute=False, yes=False)) == 0
    assert [call.kwargs["dry_run"] for call in store.purge_prefix.call_args_list] == [True, True]
