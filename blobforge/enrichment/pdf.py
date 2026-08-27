"""Poppler-backed PDF layout evidence extraction."""

from __future__ import annotations

import re
import math
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

from .contract import PdfBlock, PdfEvidence, PdfLine, PdfPage, PdfWord


def poppler_version() -> str:
    completed = subprocess.run(
        ["pdftotext", "-v"], capture_output=True, text=True, check=False
    )
    reported = completed.stderr or completed.stdout
    match = re.search(r"pdftotext version ([^\s]+)", reported)
    return match.group(1) if match else "unavailable"


def _number(element: ET.Element, key: str, *, nonnegative: bool = False) -> float:
    value = float(element.attrib[key])
    if not math.isfinite(value) or (nonnegative and value < 0):
        raise ValueError(f"invalid PDF layout coordinate {key}={value}")
    return value


def _box(element: ET.Element) -> tuple[float, float, float, float]:
    x_min, y_min = _number(element, "xMin"), _number(element, "yMin")
    x_max, y_max = _number(element, "xMax"), _number(element, "yMax")
    return x_min, y_min, x_max - x_min, y_max - y_min


def extract_pdf_evidence(path: str | Path) -> PdfEvidence:
    """Extract ordered blocks and point geometry without OCR or model calls."""
    source = Path(path)
    completed = subprocess.run(
        ["pdftotext", "-bbox-layout", "-enc", "UTF-8", str(source), "-"],
        capture_output=True,
        check=False,
    )
    if completed.returncode:
        message = completed.stderr.decode("utf-8", errors="replace")[-4000:]
        raise RuntimeError(f"pdftotext layout extraction failed: {message}")
    try:
        root = ET.fromstring(completed.stdout)
    except ET.ParseError as exc:
        raise ValueError(f"pdftotext returned invalid XHTML: {exc}") from exc

    pages: list[PdfPage] = []
    order = 0
    for page_index, page in enumerate(root.findall(".//{*}page")):
        blocks: list[PdfBlock] = []
        for block_index, element in enumerate(page.findall(".//{*}block")):
            lines: list[PdfLine] = []
            for line_index, line in enumerate(element.findall("./{*}line")):
                words: list[PdfWord] = []
                for word_index, word in enumerate(line.findall("./{*}word")):
                    word_text = "".join(word.itertext()).strip()
                    if not word_text:
                        continue
                    x, y, width, height = _box(word)
                    if width <= 0 or height <= 0:
                        continue
                    words.append(
                        PdfWord(
                            id=(
                                f"pdf-p{page_index:06d}-b{block_index:06d}"
                                f"-l{line_index:06d}-w{word_index:06d}"
                            ),
                            text=word_text,
                            x=x,
                            y=y,
                            width=width,
                            height=height,
                        )
                    )
                line_text = " ".join(word.text for word in words)
                if line_text:
                    x, y, width, height = _box(line)
                    if width > 0 and height > 0:
                        lines.append(
                            PdfLine(
                                id=(
                                    f"pdf-p{page_index:06d}-b{block_index:06d}"
                                    f"-l{line_index:06d}"
                                ),
                                text=line_text,
                                x=x,
                                y=y,
                                width=width,
                                height=height,
                                words=tuple(words),
                            )
                        )
            text = "\n".join(line.text for line in lines).strip()
            if not text:
                continue
            x, y, width, height = _box(element)
            if width <= 0 or height <= 0:
                continue
            blocks.append(
                PdfBlock(
                    id=f"pdf-p{page_index:06d}-b{block_index:06d}",
                    page=page_index,
                    order=order,
                    text=text,
                    x=x,
                    y=y,
                    width=width,
                    height=height,
                    lines=tuple(lines),
                )
            )
            order += 1
        pages.append(
            PdfPage(
                index=page_index,
                width=_number(page, "width", nonnegative=True),
                height=_number(page, "height", nonnegative=True),
                blocks=tuple(blocks),
            )
        )
    if not pages:
        raise ValueError("PDF layout extraction returned no pages")
    return PdfEvidence("poppler-pdftotext-bbox-layout", poppler_version(), tuple(pages))
