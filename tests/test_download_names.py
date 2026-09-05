import pytest

from blobforge.download_names import artifact_filename, content_disposition


@pytest.mark.parametrize(("source", "expected"), [
    ("Book.PDF", "Book.mdaf"),
    ("C:\\books\\Über Rules.pdf", "Über Rules.mdaf"),
    ("../../A\r\nBook.pdf", "ABook.mdaf"),
    ('a"b.pdf', "a_b.mdaf"),
    ("CON.pdf", "_CON.mdaf"),
    ("", "abc.mdaf"),
])
def test_artifact_names(source, expected):
    assert artifact_filename(source, "abc", "mdaf/v1") == expected


def test_legacy_extension_and_unicode_header():
    assert artifact_filename("Book.pdf", "abc", "legacy-archive") == "Book.zip"
    header = content_disposition('Über "Rules".mdaf\r\n')
    assert '\r' not in header and '\n' not in header
    assert "filename*=UTF-8''%C3%9Cber%20_Rules_.mdaf" in header
    header.encode("ascii")
    assert len(artifact_filename("é" * 300 + ".pdf", "abc", "mdaf/v1").encode()) < 255
    long_name = artifact_filename("é" * 300 + ".pdf", "abc", "mdaf/v1")
    assert content_disposition(long_name).endswith(".mdaf")
