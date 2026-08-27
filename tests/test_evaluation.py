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
