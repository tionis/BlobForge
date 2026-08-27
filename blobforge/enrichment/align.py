"""Conservative monotonic Markdown-to-PDF block alignment."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from collections import defaultdict
from typing import Any, Iterable, Mapping

from .contract import MarkdownBlock, PdfBlock, PdfEvidence
from .markdown import normalize_for_alignment, segment_markdown


@dataclass(frozen=True)
class AlignmentDiagnostic:
    block_id: str
    kind: str
    reason: str
    best_score: float | None = None
    second_score: float | None = None


@dataclass(frozen=True)
class AlignmentResult:
    mappings: tuple[Mapping[str, Any], ...]
    markdown_blocks: tuple[MarkdownBlock, ...]
    diagnostics: tuple[AlignmentDiagnostic, ...]
    mapped_blocks: int
    mapped_bytes: int
    total_blocks: int
    total_bytes: int

    def report(self) -> dict[str, Any]:
        return {
            "contract": "dev.tionis.blobforge.pdf-enrichment-report/v1",
            "summary": {
                "total_blocks": self.total_blocks,
                "mapped_blocks": self.mapped_blocks,
                "block_coverage": self.mapped_blocks / self.total_blocks if self.total_blocks else 0,
                "total_bytes": self.total_bytes,
                "mapped_bytes": self.mapped_bytes,
                "byte_coverage": self.mapped_bytes / self.total_bytes if self.total_bytes else 0,
            },
            "markdown_blocks": [block.as_json() for block in self.markdown_blocks],
            "diagnostics": [asdict(item) for item in self.diagnostics],
        }


@dataclass(frozen=True)
class _Candidate:
    start: int
    end: int
    page: int
    score: float
    blocks: tuple[PdfBlock, ...]
    normalized_text: str


def _tokens(value: str) -> set[str]:
    return {token for token in value.split() if token}


def _score(left: str, right: str) -> float:
    if not left or not right:
        return 0
    if left == right:
        return 1
    left_tokens, right_tokens = _tokens(left), _tokens(right)
    overlap = len(left_tokens & right_tokens)
    token_f1 = (
        2 * overlap / (len(left_tokens) + len(right_tokens))
        if left_tokens and right_tokens
        else 0
    )
    containment = 0.0
    if left in right or right in left:
        containment = 0.9 * min(len(left), len(right)) / max(len(left), len(right)) + 0.1
    return max(containment, token_f1)


def _refine_score(left: str, candidate: _Candidate) -> float:
    right = candidate.normalized_text
    if candidate.score < 0.35 or max(len(left), len(right)) > 5000:
        return candidate.score
    sequence = SequenceMatcher(None, left, right, autojunk=False).ratio()
    return max(candidate.score, 0.65 * sequence + 0.35 * candidate.score)


def _seed_pages(block: MarkdownBlock, mappings: Iterable[Mapping[str, Any]]) -> set[int]:
    pages: set[int] = set()
    for mapping in mappings:
        document = mapping.get("document", {})
        if document.get("end", 0) <= block.start or document.get("start", 0) >= block.end:
            continue
        for selector in mapping.get("source", {}).get("selectors", []):
            if selector.get("type") == "interval" and selector.get("unit") == "page":
                pages.update(range(int(selector["start"]), int(selector["end"])))
    return pages


def _candidates(
    markdown: MarkdownBlock,
    pdf_blocks: tuple[PdfBlock, ...],
    normalized_pdf: tuple[str, ...],
    token_index: Mapping[str, tuple[int, ...]],
    cursor: int,
    seed_pages: set[int],
) -> list[_Candidate]:
    if seed_pages:
        allowed = {i for i, block in enumerate(pdf_blocks) if block.page in seed_pages}
    else:
        lower = max(0, cursor - 2)
        allowed = set(range(lower, min(len(pdf_blocks), cursor + 121)))
    anchor_positions: set[int] = set()
    anchor_tokens = sorted(
        (token for token in _tokens(markdown.normalized_text) if len(token) >= 4),
        key=lambda token: (len(token_index.get(token, ())), -len(token), token),
    )[:8]
    for token in anchor_tokens:
        anchor_positions.update(token_index.get(token, ()))
    indexed_starts = {
        start
        for position in anchor_positions
        for start in (position - 2, position - 1, position)
        if start in allowed
    }
    starts = indexed_starts or allowed
    if len(starts) > 200:
        starts = set(sorted(starts, key=lambda value: (abs(value - cursor), value))[:200])
    result: list[_Candidate] = []
    for start in sorted(starts):
        selected: list[PdfBlock] = []
        for end in range(start, min(len(pdf_blocks), start + 3)):
            candidate = pdf_blocks[end]
            if selected and candidate.page != selected[0].page:
                break
            selected.append(candidate)
            text = " ".join(normalized_pdf[index] for index in range(start, end + 1))
            result.append(
                _Candidate(
                    start,
                    end + 1,
                    candidate.page,
                    _score(markdown.normalized_text, text),
                    tuple(selected),
                    text,
                )
            )
    preliminary = sorted(result, key=lambda item: (-item.score, item.start, item.end))
    refined = {
        (item.start, item.end): _refine_score(markdown.normalized_text, item)
        for item in preliminary[:12]
    }
    return sorted(
        (
            _Candidate(
                item.start,
                item.end,
                item.page,
                refined.get((item.start, item.end), item.score),
                item.blocks,
                item.normalized_text,
            )
            for item in preliminary
        ),
        key=lambda item: (-item.score, item.start, item.end),
    )


def _mapping(
    block: MarkdownBlock,
    candidate: _Candidate,
    page_width: float,
    page_height: float,
) -> tuple[dict[str, Any], bool]:
    raw_x = min(item.x for item in candidate.blocks)
    raw_y = min(item.y for item in candidate.blocks)
    raw_right = max(item.x + item.width for item in candidate.blocks)
    raw_bottom = max(item.y + item.height for item in candidate.blocks)
    x, y = max(0.0, raw_x), max(0.0, raw_y)
    right, bottom = min(page_width, raw_right), min(page_height, raw_bottom)
    clipped = (x, y, right, bottom) != (raw_x, raw_y, raw_right, raw_bottom)
    selectors: list[dict[str, Any]] = [
        {
            "type": "interval",
            "unit": "page",
            "start": candidate.page,
            "end": candidate.page + 1,
        }
    ]
    if right > x and bottom > y:
        selectors.append(
            {
                "type": "rectangle",
                "unit": "point",
                "x": round(x, 6),
                "y": round(y, 6),
                "width": round(right - x, 6),
                "height": round(bottom - y, 6),
            }
        )
    mapping = {
        "document": {"start": block.start, "end": block.end},
        "source": {
            "source_id": "document",
            "selectors": selectors,
        },
        "confidence": round(candidate.score, 6),
        "method": "dev.tionis.blobforge/poppler-monotonic-block-alignment-v1",
    }
    return mapping, clipped


def align_markdown_to_pdf(
    markdown: str,
    evidence: PdfEvidence,
    *,
    seed_mappings: Iterable[Mapping[str, Any]] = (),
    minimum_score: float = 0.72,
    ambiguity_margin: float = 0.08,
) -> AlignmentResult:
    blocks = segment_markdown(markdown)
    pdf_blocks = evidence.blocks
    page_dimensions = {page.index: (page.width, page.height) for page in evidence.pages}
    normalized_pdf = tuple(normalize_for_alignment(block.text) for block in pdf_blocks)
    mutable_index: dict[str, list[int]] = defaultdict(list)
    for index, text in enumerate(normalized_pdf):
        for token in _tokens(text):
            mutable_index[token].append(index)
    token_index = {token: tuple(indexes) for token, indexes in mutable_index.items()}
    seeds = tuple(seed_mappings)
    cursor = 0
    mappings: list[Mapping[str, Any]] = []
    diagnostics: list[AlignmentDiagnostic] = []
    mapped_bytes = 0
    for block in blocks:
        if len(block.normalized_text) < 4 or block.kind in {"image", "code"}:
            diagnostics.append(AlignmentDiagnostic(block.id, block.kind, "insufficient-text"))
            continue
        candidates = _candidates(
            block,
            pdf_blocks,
            normalized_pdf,
            token_index,
            cursor,
            _seed_pages(block, seeds),
        )
        if not candidates:
            diagnostics.append(AlignmentDiagnostic(block.id, block.kind, "no-candidates"))
            continue
        best = candidates[0]
        second = next(
            (
                item
                for item in candidates[1:]
                if item.end <= best.start or item.start >= best.end
            ),
            None,
        )
        second_score = second.score if second else 0.0
        if best.score < minimum_score:
            diagnostics.append(
                AlignmentDiagnostic(block.id, block.kind, "below-threshold", best.score, second_score)
            )
            continue
        if second and best.score - second_score < ambiguity_margin:
            diagnostics.append(
                AlignmentDiagnostic(block.id, block.kind, "ambiguous", best.score, second_score)
            )
            continue
        mapping, clipped = _mapping(block, best, *page_dimensions[best.page])
        mappings.append(mapping)
        if clipped:
            diagnostics.append(
                AlignmentDiagnostic(block.id, block.kind, "geometry-clipped", best.score, second_score)
            )
        mapped_bytes += block.end - block.start
        cursor = max(cursor, best.end)
    total_bytes = sum(block.end - block.start for block in blocks)
    return AlignmentResult(
        tuple(mappings),
        blocks,
        tuple(diagnostics),
        len(mappings),
        mapped_bytes,
        len(blocks),
        total_bytes,
    )
