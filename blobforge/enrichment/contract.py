"""Versioned backend-neutral evidence types used by enrichment stages."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

EVIDENCE_CONTRACT = "dev.tionis.blobforge.document-evidence/v1"


@dataclass(frozen=True)
class PdfWord:
    id: str
    text: str
    x: float
    y: float
    width: float
    height: float

    def as_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PdfLine:
    id: str
    text: str
    x: float
    y: float
    width: float
    height: float
    words: tuple[PdfWord, ...] = ()

    def as_json(self) -> dict[str, Any]:
        return {
            **{key: value for key, value in asdict(self).items() if key != "words"},
            "words": [word.as_json() for word in self.words],
        }


@dataclass(frozen=True)
class PdfBlock:
    id: str
    page: int
    order: int
    text: str
    x: float
    y: float
    width: float
    height: float
    lines: tuple[PdfLine, ...] = ()

    @property
    def words(self) -> tuple[PdfWord, ...]:
        return tuple(word for line in self.lines for word in line.words)

    def as_json(self) -> dict[str, Any]:
        return {
            **{key: value for key, value in asdict(self).items() if key != "lines"},
            "lines": [line.as_json() for line in self.lines],
        }


@dataclass(frozen=True)
class PdfPage:
    index: int
    width: float
    height: float
    blocks: tuple[PdfBlock, ...]

    def as_json(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "width": self.width,
            "height": self.height,
            "coordinate_origin": "top-left",
            "unit": "point",
            "blocks": [block.as_json() for block in self.blocks],
        }


@dataclass(frozen=True)
class PdfEvidence:
    extractor: str
    extractor_version: str
    pages: tuple[PdfPage, ...]

    @property
    def blocks(self) -> tuple[PdfBlock, ...]:
        return tuple(block for page in self.pages for block in page.blocks)

    def as_json(self) -> dict[str, Any]:
        return {
            "contract": EVIDENCE_CONTRACT,
            "source_media_type": "application/pdf",
            "extractor": {
                "name": self.extractor,
                "version": self.extractor_version,
            },
            "pages": [page.as_json() for page in self.pages],
        }


@dataclass(frozen=True)
class MarkdownBlock:
    id: str
    kind: str
    start: int
    end: int
    text: str
    normalized_text: str

    def as_json(self) -> dict[str, Any]:
        return asdict(self)
