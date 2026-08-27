"""Deterministic Poppler text-layer baseline adapter."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

CONTRACT = "dev.tionis.blobforge.converter-bundle/v1"


def main() -> int:
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if request.get("contract") != CONTRACT:
        raise ValueError("unsupported converter request contract")
    source = Path(request["source_path"])
    output = Path(request["output_dir"])
    data = output / "data"
    native = data / "native"
    native.mkdir(parents=True)
    completed = subprocess.run(
        ["pdftotext", "-layout", str(source), "-"],
        capture_output=True,
        check=True,
    )
    text = completed.stdout.decode("utf-8", errors="strict").replace("\r\n", "\n")
    pages = text.split("\f")
    if pages and not pages[-1]:
        pages.pop()
    markdown = ""
    mappings = []
    for page, page_text in enumerate(pages):
        if markdown:
            markdown += "\n\n"
        start = len(markdown.encode("utf-8"))
        markdown += page_text.rstrip()
        end = len(markdown.encode("utf-8"))
        if end > start:
            mappings.append(
                {
                    "document": {"start": start, "end": end},
                    "source": {
                        "source_id": "document",
                        "selectors": [
                            {"type": "interval", "unit": "page", "start": page, "end": page + 1}
                        ],
                    },
                    "confidence": 1,
                    "method": "dev.tionis.blobforge/poppler-form-feed",
                }
            )
    (data / "text.md").write_text(markdown + "\n", encoding="utf-8")
    (native / "pages.json").write_text(
        json.dumps({"pages": pages}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (data / "source-map.json").write_text(
        json.dumps({"mappings": mappings, "references": []}, indent=2) + "\n", encoding="utf-8"
    )
    reported = subprocess.run(["pdftotext", "-v"], capture_output=True, text=True).stderr
    match = re.search(r"pdftotext version ([^\s]+)", reported)
    tool_version = match.group(1) if match else "unavailable"
    bundle = {
        "contract": CONTRACT,
        "text_path": "data/text.md",
        "source_map": "data/source-map.json",
        "members": [
            {
                "path": "renditions/org.freedesktop.poppler/pages.json",
                "file": "data/native/pages.json",
                "role": "rendition",
                "media_type": "application/json",
                "namespace": "org.freedesktop.poppler",
            }
        ],
        "tool": {"name": "pdftotext", "version": tool_version},
        "models": [],
        "parameters": {"layout": True, "encoding": "UTF-8"},
        "diagnostics": [],
    }
    (output / "bundle.json").write_text(
        json.dumps(bundle, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
