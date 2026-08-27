import hashlib

from blake3 import blake3

from blobforge import utils


def test_compute_transition_hashes_in_one_api(tmp_path, monkeypatch):
    monkeypatch.setattr(utils, "XATTR_AVAILABLE", False)
    data = "BLAKE3 café".encode()
    path = tmp_path / "source.pdf"
    path.write_bytes(data)
    digests = utils.compute_hashes_with_cache(str(path))
    assert digests == {
        "blake3": blake3(data).hexdigest(),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
