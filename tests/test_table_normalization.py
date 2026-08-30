import json
import zipfile

import pytest

from blobforge.mdaf import MdafSource, build_mdaf, validate_mdaf
from blobforge.mdaf.builder import activity
from blobforge.normalization import (
    TableCell,
    TableRow,
    markdown_table_to_html,
    semantic_html_table,
)


def test_semantic_table_is_deterministic_escaped_and_span_aware():
    table = semantic_html_table(
        caption='Stats <unsafe> & "quoted"',
        head=[
            TableRow([TableCell("SR5", True, colspan=3, scope="colgroup")]),
            TableRow(
                [
                    TableCell("B", True, scope="col"),
                    TableCell("A", True, scope="col"),
                    TableCell("R", True, scope="col"),
                ]
            ),
        ],
        body=[
            TableRow(
                [
                    TableCell("Skills", True, rowspan=2, scope="rowgroup"),
                    TableCell("Firearms", colspan=2),
                ]
            ),
            TableRow([TableCell("<script>alert(1)</script>", colspan=2)]),
        ],
    )
    assert '<th colspan="3" scope="colgroup">SR5</th>' in table
    assert '<th rowspan="2" scope="rowgroup">Skills</th>' in table
    assert "<script>" not in table
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in table
    assert 'Stats &lt;unsafe&gt; &amp; &quot;quoted&quot;' in table
    assert table == semantic_html_table(
        caption='Stats <unsafe> & "quoted"',
        head=[
            TableRow([TableCell("SR5", True, colspan=3, scope="colgroup")]),
            TableRow(
                [
                    TableCell("B", True, scope="col"),
                    TableCell("A", True, scope="col"),
                    TableCell("R", True, scope="col"),
                ]
            ),
        ],
        body=[
            TableRow(
                [
                    TableCell("Skills", True, rowspan=2, scope="rowgroup"),
                    TableCell("Firearms", colspan=2),
                ]
            ),
            TableRow([TableCell("<script>alert(1)</script>", colspan=2)]),
        ],
    )


def test_pipe_table_blank_runs_become_semantic_colspans():
    table = markdown_table_to_html(
        """| SR5 | | | |
| --- | --- | --- | --- |
| B | A | R | S |
| 4 | 5 | 4 | 2 |
| Skills | | Perception 8 | |
| Gear | | Commlink <b>DR 7</b> | |"""
    )
    assert '<th colspan="4" scope="colgroup">SR5</th>' in table
    assert '<th colspan="2" scope="row">Skills</th>' in table
    assert '<td colspan="2">Perception 8</td>' in table
    assert "Commlink <strong>DR 7</strong>" in table
    assert "| --- |" not in table


@pytest.mark.parametrize(
    "rows, message",
    [
        ([TableRow([TableCell("bad", colspan=0)])], "colspan"),
        (
            [
                TableRow([TableCell("bad", rowspan=2)]),
                TableRow([TableCell("ragged"), TableCell("extra")]),
            ],
            "overlap|rectangular",
        ),
    ],
)
def test_semantic_table_rejects_invalid_grids(rows, message):
    with pytest.raises(ValueError, match=message):
        semantic_html_table(head=[], body=rows)


def test_mdaf_declares_and_validates_safe_html_table_features(tmp_path):
    table = semantic_html_table(
        head=[TableRow([TableCell("Header", True, colspan=2, scope="colgroup")])],
        body=[TableRow([TableCell("One"), TableCell("Two")])],
    )
    result = build_mdaf(
        tmp_path / "html-table.mdaf",
        text=f"# Tables\n\n{table}\n",
        sources=[MdafSource("document", "application/pdf", "blake3:" + "0" * 64)],
        activities=[
            activity(
                activity_id="activity:test",
                kind="normalization",
                tools=[{"name": "blobforge-table-normalizer", "version": "1"}],
                inputs=["source:document"],
                outputs=["text.md", "provenance.json"],
                parameters={"table_output": "safe-html-v1"},
            )
        ],
        producer={"name": "blobforge", "version": "test"},
        markdown_features=["raw-html", "semantic-html-table-v1"],
    )
    validate_mdaf(result.path)
    with zipfile.ZipFile(result.path) as archive:
        info = json.loads(archive.read("info.json"))
        assert info["markdown"]["features"] == [
            "raw-html",
            "semantic-html-table-v1",
        ]
        assert b'<th colspan="2" scope="colgroup">Header</th>' in archive.read(
            "text.md"
        )
