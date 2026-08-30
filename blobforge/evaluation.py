"""Backend-neutral structural measurements for MDAF comparison."""

from __future__ import annotations

import json
import re
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path

from .mdaf import validate_mdaf

SEMANTIC_TABLE_TAG_RE = re.compile(
    r"</?(?:table|caption|thead|tbody|tr|th|td)(?:\s+[^>]*)?>", re.IGNORECASE
)


@dataclass(frozen=True)
class ArtifactMetrics:
    path: str
    identity: str
    producer: str
    text_bytes: int
    words: int
    headings: int
    table_rows: int
    assets: int
    mappings: int
    mapped_pages: int
    replacement_characters: int
    nul_characters: int


def measure(path: str | Path) -> ArtifactMetrics:
    artifact = Path(path)
    validated = validate_mdaf(artifact)
    with zipfile.ZipFile(artifact) as archive:
        text_bytes = archive.read("text.md")
        text = text_bytes.decode("utf-8")
        manifest = validated.manifest
        source_map = (
            json.loads(archive.read("source-map.json"))
            if "source-map.json" in archive.namelist()
            else {"mappings": []}
        )
    mappings = source_map.get("mappings", [])
    pages = set()
    for mapping in mappings:
        for selector in mapping.get("source", {}).get("selectors", []):
            if selector.get("type") == "interval" and selector.get("unit") == "page":
                pages.update(range(int(selector["start"]), int(selector["end"])))
    producer = manifest["producer"]
    visible_text = SEMANTIC_TABLE_TAG_RE.sub(" ", text)
    markdown_table_rows = len(re.findall(r"^\s*\|.*\|\s*$", text, re.MULTILINE))
    html_table_rows = len(re.findall(r"<tr(?:\s[^>]*)?>", text, re.IGNORECASE))
    return ArtifactMetrics(
        path=str(artifact),
        identity=validated.identity,
        producer=f"{producer['name']} {producer['version']}",
        text_bytes=len(text_bytes),
        words=len(re.findall(r"\b\w+\b", visible_text, re.UNICODE)),
        headings=len(re.findall(r"^#{1,6}\s+", text, re.MULTILINE)),
        table_rows=markdown_table_rows + html_table_rows,
        assets=sum(member.get("role") == "asset" for member in manifest["members"]),
        mappings=len(mappings),
        mapped_pages=len(pages),
        replacement_characters=text.count("\ufffd"),
        nul_characters=text.count("\x00"),
    )


def compare(paths: list[str | Path], output: str | Path | None = None) -> list[ArtifactMetrics]:
    metrics = [measure(path) for path in paths]
    if output:
        destination = Path(output)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps([asdict(item) for item in metrics], indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    return metrics
