from blobforge.evaluation import measure
from blobforge.mdaf import MdafSource, build_mdaf
from blobforge.mdaf.builder import activity


def test_measure_valid_artifact(tmp_path):
    result = build_mdaf(
        tmp_path / "result.mdaf",
        text="# Heading\n\n| A |\n|---|\n",
        sources=[MdafSource("document", "application/pdf", "blake3:" + "0" * 64)],
        activities=[
            activity(
                activity_id="activity:test",
                kind="test",
                tools=[{"name": "test", "version": "1"}],
                inputs=["source:document"],
                outputs=["text.md", "provenance.json"],
                parameters={},
            )
        ],
        producer={"name": "test", "version": "1"},
    )
    metrics = measure(result.path)
    assert metrics.headings == 1
    assert metrics.table_rows == 2
    assert metrics.nul_characters == 0


def test_measure_counts_semantic_html_rows_without_counting_tags_as_words(tmp_path):
    result = build_mdaf(
        tmp_path / "html.mdaf",
        text=(
            "<table>\n<thead>\n<tr><th colspan=\"2\">Name</th></tr>\n</thead>\n"
            "<tbody>\n<tr><td>Ada</td><td>5</td></tr>\n</tbody>\n</table>\n"
        ),
        sources=[MdafSource("document", "application/pdf", "blake3:" + "0" * 64)],
        activities=[
            activity(
                activity_id="activity:test",
                kind="test",
                tools=[{"name": "test", "version": "1"}],
                inputs=["source:document"],
                outputs=["text.md", "provenance.json"],
                parameters={},
            )
        ],
        producer={"name": "test", "version": "1"},
        markdown_features=["raw-html", "semantic-html-table-v1"],
    )
    metrics = measure(result.path)
    assert metrics.table_rows == 2
    assert metrics.words == 3
