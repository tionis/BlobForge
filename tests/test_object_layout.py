import pytest

from blobforge.object_layout import artifact_key, migration_manifest_key, source_key


def test_v2_keys_are_algorithm_tagged_and_sharded():
    digest = "blake3:" + "ab" + "1" * 62
    assert source_key(digest) == f"store/v2/sources/blake3/ab/{digest[7:]}"
    assert artifact_key(digest, "attempt-42") == (
        f"store/v2/artifacts/mdaf/v1/blake3/ab/{digest[7:]}/attempt-42.mdaf"
    )
    assert migration_manifest_key("run-20260827") == (
        "store/v2/migrations/run-20260827/manifest.json"
    )


@pytest.mark.parametrize("value", ["abc", "blake3:xyz", "blake3:" + "A" * 64])
def test_v2_keys_reject_noncanonical_digests(value):
    with pytest.raises(ValueError):
        source_key(value)


def test_v2_keys_reject_path_like_attempts():
    with pytest.raises(ValueError):
        artifact_key("blake3:" + "0" * 64, "../escape")
