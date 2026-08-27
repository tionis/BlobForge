"""Conservative anchor-bounded Markdown-to-PDF alignment."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from typing import Any, Iterable, Mapping, Sequence

from .contract import MarkdownBlock, PdfBlock, PdfEvidence, PdfWord
from .markdown import normalize_for_alignment, segment_markdown

REGION_METHOD = "dev.tionis.blobforge/poppler-word-region-alignment-v2"
PAGE_METHOD = "dev.tionis.blobforge/poppler-page-alignment-v2"
ALIGNMENT_METHODS = frozenset({REGION_METHOD, PAGE_METHOD})


@dataclass(frozen=True)
class AlignmentDiagnostic:
    block_id: str
    kind: str
    reason: str
    best_score: float | None = None
    second_score: float | None = None
    page: int | None = None
    region_score: float | None = None


@dataclass(frozen=True)
class AlignmentResult:
    mappings: tuple[Mapping[str, Any], ...]
    markdown_blocks: tuple[MarkdownBlock, ...]
    diagnostics: tuple[AlignmentDiagnostic, ...]
    mapped_blocks: int
    mapped_bytes: int
    region_mapped_blocks: int
    page_only_mapped_blocks: int
    total_blocks: int
    total_bytes: int

    def report(self) -> dict[str, Any]:
        return {
            "contract": "dev.tionis.blobforge.pdf-enrichment-report/v1",
            "summary": {
                "total_blocks": self.total_blocks,
                "mapped_blocks": self.mapped_blocks,
                "region_mapped_blocks": self.region_mapped_blocks,
                "page_only_mapped_blocks": self.page_only_mapped_blocks,
                "block_coverage": self.mapped_blocks / self.total_blocks
                if self.total_blocks
                else 0,
                "total_bytes": self.total_bytes,
                "mapped_bytes": self.mapped_bytes,
                "byte_coverage": self.mapped_bytes / self.total_bytes
                if self.total_bytes
                else 0,
            },
            "markdown_blocks": [block.as_json() for block in self.markdown_blocks],
            "diagnostics": [asdict(item) for item in self.diagnostics],
        }


@dataclass(frozen=True)
class _Region:
    words: tuple[PdfWord, ...]
    score: float
    markdown_token_coverage: float
    length_ratio: float
    source_block_count: int


@dataclass(frozen=True)
class _Candidate:
    start: int
    end: int
    page: int
    score: float
    blocks: tuple[PdfBlock, ...]
    normalized_text: str
    region: _Region | None = None

    @property
    def evidence_ids(self) -> frozenset[str]:
        if self.region:
            return frozenset(word.id for word in self.region.words)
        return frozenset(block.id for block in self.blocks)


@dataclass(frozen=True)
class _AnchorWindow:
    direct_pages: frozenset[int]
    lower_page: int
    upper_page: int
    conflict: bool = False


def validate_alignment_publication(
    source_map: Mapping[str, Any], report: Mapping[str, Any]
) -> tuple[str, ...]:
    """Check v2 publication invariants independently of candidate scoring."""
    errors: list[str] = []
    mappings = sorted(
        (
            item
            for item in source_map.get("mappings", [])
            if item.get("method") in ALIGNMENT_METHODS
        ),
        key=lambda item: (item["document"]["start"], item["document"]["end"]),
    )
    previous_page = -1
    rectangles: set[tuple[Any, ...]] = set()
    region_count = 0
    page_count = 0
    for mapping in mappings:
        selectors = mapping.get("source", {}).get("selectors", [])
        intervals = [
            item
            for item in selectors
            if item.get("type") == "interval" and item.get("unit") == "page"
        ]
        if len(intervals) != 1:
            errors.append(f"{mapping['document']}: expected exactly one page interval")
            continue
        page = int(intervals[0]["start"])
        if page < previous_page:
            errors.append(
                f"{mapping['document']}: page {page} regresses from {previous_page}"
            )
        previous_page = max(previous_page, page)
        regions = [item for item in selectors if item.get("type") == "rectangle"]
        if mapping.get("method") == PAGE_METHOD:
            page_count += 1
            if regions:
                errors.append(f"{mapping['document']}: page-only method has a rectangle")
        else:
            region_count += 1
            if len(regions) != 1:
                errors.append(f"{mapping['document']}: region method lacks one rectangle")
                continue
            region = regions[0]
            key = (
                page,
                region.get("x"),
                region.get("y"),
                region.get("width"),
                region.get("height"),
            )
            if key in rectangles:
                errors.append(f"{mapping['document']}: reuses published rectangle {key}")
            rectangles.add(key)
    summary = report.get("summary", {})
    expected = {
        "mapped_blocks": len(mappings),
        "region_mapped_blocks": region_count,
        "page_only_mapped_blocks": page_count,
    }
    for key, value in expected.items():
        if summary.get(key) != value:
            errors.append(f"report {key}={summary.get(key)!r}, expected {value}")
    return tuple(errors)


def _tokens(value: str) -> list[str]:
    return [token for token in value.split() if token]


def _token_score(left: str, right: str) -> float:
    if not left or not right:
        return 0
    if left == right:
        return 1
    left_tokens, right_tokens = set(_tokens(left)), set(_tokens(right))
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


def _sequence_score(left: str, right: str, token_score: float | None = None) -> float:
    if not left or not right:
        return 0
    if left == right:
        return 1
    lexical = _token_score(left, right) if token_score is None else token_score
    if lexical < 0.25 or max(len(left), len(right)) > 5000:
        return lexical
    sequence = SequenceMatcher(None, left, right, autojunk=False).ratio()
    return 0.7 * sequence + 0.3 * lexical


def _mapping_pages(mapping: Mapping[str, Any]) -> frozenset[int]:
    pages: set[int] = set()
    for selector in mapping.get("source", {}).get("selectors", []):
        if selector.get("type") == "interval" and selector.get("unit") == "page":
            pages.update(range(int(selector["start"]), int(selector["end"])))
    return frozenset(pages)


def _anchor_window(
    block: MarkdownBlock,
    mappings: Sequence[Mapping[str, Any]],
    last_page: int,
    final_page: int,
) -> _AnchorWindow:
    direct: set[int] = set()
    previous: list[tuple[int, frozenset[int]]] = []
    following: list[tuple[int, frozenset[int]]] = []
    for mapping in mappings:
        pages = _mapping_pages(mapping)
        if not pages:
            continue
        document = mapping.get("document", {})
        start, end = int(document.get("start", 0)), int(document.get("end", 0))
        if end > block.start and start < block.end:
            direct.update(pages)
        elif end <= block.start:
            previous.append((end, pages))
        elif start >= block.end:
            following.append((start, pages))
    if direct:
        return _AnchorWindow(frozenset(direct), min(direct), max(direct))
    lower = last_page
    if previous:
        nearest_end = max(item[0] for item in previous)
        nearest_pages = set().union(
            *(pages for end, pages in previous if end == nearest_end)
        )
        lower = max(lower, max(nearest_pages))
    upper = final_page
    if following:
        nearest_start = min(item[0] for item in following)
        nearest_pages = set().union(
            *(pages for start, pages in following if start == nearest_start)
        )
        upper = min(upper, min(nearest_pages))
    return _AnchorWindow(frozenset(), lower, upper, lower > upper)


def _refine_words(markdown_text: str, blocks: Sequence[PdfBlock]) -> _Region | None:
    words = tuple(word for block in blocks for word in block.words)
    word_blocks = {word.id: block.id for block in blocks for word in block.words}
    left_tokens = _tokens(markdown_text)
    if not words or not left_tokens:
        return None
    source_tokens: list[str] = []
    token_words: list[PdfWord] = []
    for word in words:
        normalized = normalize_for_alignment(word.text)
        for token in _tokens(normalized):
            source_tokens.append(token)
            token_words.append(word)
    if not source_tokens:
        return None
    matcher = SequenceMatcher(None, left_tokens, source_tokens, autojunk=False)
    matches = [item for item in matcher.get_matching_blocks() if item.size]
    if not matches:
        return None
    matched_tokens = sum(item.size for item in matches)
    first = min(item.b for item in matches)
    last = max(item.b + item.size for item in matches)
    selected: list[PdfWord] = []
    seen: set[str] = set()
    for word in token_words[first:last]:
        if word.id not in seen:
            selected.append(word)
            seen.add(word.id)
    source_text = normalize_for_alignment(" ".join(word.text for word in selected))
    if not source_text:
        return None
    length_ratio = min(len(markdown_text), len(source_text)) / max(
        len(markdown_text), len(source_text)
    )
    return _Region(
        tuple(selected),
        _sequence_score(markdown_text, source_text),
        matched_tokens / len(left_tokens),
        length_ratio,
        len({word_blocks[word.id] for word in selected}),
    )


def _candidate(
    markdown: MarkdownBlock,
    start: int,
    end: int,
    blocks: tuple[PdfBlock, ...],
    normalized_text: str,
) -> _Candidate:
    lexical = _token_score(markdown.normalized_text, normalized_text)
    score = _sequence_score(markdown.normalized_text, normalized_text, lexical)
    region = _refine_words(markdown.normalized_text, blocks)
    if region and region.markdown_token_coverage >= 0.5:
        score = max(score, region.score)
    return _Candidate(start, end, blocks[0].page, score, blocks, normalized_text, region)


def _candidates(
    markdown: MarkdownBlock,
    pdf_blocks: tuple[PdfBlock, ...],
    normalized_pdf: tuple[str, ...],
    token_index: Mapping[str, tuple[int, ...]],
    cursor: int,
    window: _AnchorWindow,
) -> list[_Candidate]:
    if window.direct_pages:
        allowed = {
            index
            for index, block in enumerate(pdf_blocks)
            if block.page in window.direct_pages
        }
    else:
        lower = max(0, cursor - 2)
        upper = min(len(pdf_blocks), cursor + 121)
        allowed = {
            index
            for index in range(lower, upper)
            if window.lower_page <= pdf_blocks[index].page <= window.upper_page
        }
    anchor_positions: set[int] = set()
    anchor_tokens = sorted(
        (token for token in set(_tokens(markdown.normalized_text)) if len(token) >= 4),
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
    preliminary: list[tuple[float, int, int, tuple[PdfBlock, ...], str]] = []
    for start in sorted(starts):
        selected: list[PdfBlock] = []
        for end in range(start, min(len(pdf_blocks), start + 3)):
            pdf_block = pdf_blocks[end]
            if selected and pdf_block.page != selected[0].page:
                break
            selected.append(pdf_block)
            text = " ".join(normalized_pdf[index] for index in range(start, end + 1))
            preliminary.append(
                (
                    _token_score(markdown.normalized_text, text),
                    start,
                    end + 1,
                    tuple(selected),
                    text,
                )
            )
    preliminary.sort(key=lambda item: (-item[0], item[1], item[2]))
    refined_keys = {(item[1], item[2]) for item in preliminary[:20]}
    result = []
    for lexical, start, end, blocks, text in preliminary:
        if (start, end) in refined_keys:
            result.append(_candidate(markdown, start, end, blocks, text))
        else:
            result.append(_Candidate(start, end, blocks[0].page, lexical, blocks, text))
    return sorted(result, key=lambda item: (-item.score, item.start, item.end))


def _word_region_supported(markdown: MarkdownBlock, candidate: _Candidate) -> bool:
    region = candidate.region
    return bool(
        region
        and region.score >= 0.9
        and region.markdown_token_coverage >= 0.85
        and region.length_ratio >= (0.8 if markdown.kind == "table" else 0.85)
        and (region.source_block_count == 1 or markdown.kind == "table")
    )


def _geometry_items(markdown: MarkdownBlock, candidate: _Candidate) -> Sequence[Any]:
    if _word_region_supported(markdown, candidate) and candidate.region:
        return candidate.region.words
    if candidate.normalized_text == markdown.normalized_text:
        return candidate.blocks
    return ()


def _mapping(
    block: MarkdownBlock,
    candidate: _Candidate,
    page_width: float,
    page_height: float,
) -> tuple[dict[str, Any], bool, bool]:
    selectors: list[dict[str, Any]] = [
        {
            "type": "interval",
            "unit": "page",
            "start": candidate.page,
            "end": candidate.page + 1,
        }
    ]
    clipped = False
    word_region = _word_region_supported(block, candidate)
    items = _geometry_items(block, candidate)
    if items:
        raw_x = min(item.x for item in items)
        raw_y = min(item.y for item in items)
        raw_right = max(item.x + item.width for item in items)
        raw_bottom = max(item.y + item.height for item in items)
        x, y = max(0.0, raw_x), max(0.0, raw_y)
        right, bottom = min(page_width, raw_right), min(page_height, raw_bottom)
        clipped = (x, y, right, bottom) != (raw_x, raw_y, raw_right, raw_bottom)
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
    has_region = len(selectors) == 2
    confidence = candidate.score
    if has_region and word_region and candidate.region:
        confidence = min(confidence, candidate.region.score)
    mapping = {
        "document": {"start": block.start, "end": block.end},
        "source": {"source_id": "document", "selectors": selectors},
        "confidence": round(confidence, 6),
        "method": REGION_METHOD if has_region else PAGE_METHOD,
    }
    return mapping, clipped, has_region


def align_markdown_to_pdf(
    markdown: str,
    evidence: PdfEvidence,
    *,
    seed_mappings: Iterable[Mapping[str, Any]] = (),
    minimum_score: float = 0.82,
    ambiguity_margin: float = 0.08,
) -> AlignmentResult:
    blocks = segment_markdown(markdown)
    pdf_blocks = evidence.blocks
    page_dimensions = {page.index: (page.width, page.height) for page in evidence.pages}
    normalized_pdf = tuple(normalize_for_alignment(block.text) for block in pdf_blocks)
    mutable_index: dict[str, list[int]] = defaultdict(list)
    for index, text in enumerate(normalized_pdf):
        for token in set(_tokens(text)):
            mutable_index[token].append(index)
    token_index = {token: tuple(indexes) for token, indexes in mutable_index.items()}
    seeds = tuple(seed_mappings)
    cursor = 0
    last_page = 0
    final_page = max(page_dimensions, default=0)
    consumed: set[str] = set()
    mappings: list[Mapping[str, Any]] = []
    diagnostics: list[AlignmentDiagnostic] = []
    mapped_bytes = 0
    region_mapped_blocks = 0
    page_only_mapped_blocks = 0
    for block in blocks:
        if len(block.normalized_text) < 4 or block.kind in {"image", "code"}:
            diagnostics.append(AlignmentDiagnostic(block.id, block.kind, "insufficient-text"))
            continue
        window = _anchor_window(block, seeds, last_page, final_page)
        if window.conflict:
            diagnostics.append(AlignmentDiagnostic(block.id, block.kind, "anchor-conflict"))
            continue
        candidates = _candidates(block, pdf_blocks, normalized_pdf, token_index, cursor, window)
        if not candidates:
            diagnostics.append(AlignmentDiagnostic(block.id, block.kind, "no-candidates"))
            continue
        available = [item for item in candidates if not (item.evidence_ids & consumed)]
        if not available:
            diagnostics.append(
                AlignmentDiagnostic(
                    block.id,
                    block.kind,
                    "evidence-reused",
                    candidates[0].score,
                    page=candidates[0].page,
                    region_score=candidates[0].region.score if candidates[0].region else None,
                )
            )
            continue
        best = available[0]
        second = next(
            (
                item
                for item in available[1:]
                if item.end <= best.start or item.start >= best.end
            ),
            None,
        )
        second_score = second.score if second else 0.0
        if best.score < minimum_score:
            diagnostics.append(
                AlignmentDiagnostic(
                    block.id,
                    block.kind,
                    "below-threshold",
                    best.score,
                    second_score,
                    best.page,
                    best.region.score if best.region else None,
                )
            )
            continue
        if second and best.score - second_score < ambiguity_margin:
            diagnostics.append(
                AlignmentDiagnostic(
                    block.id,
                    block.kind,
                    "ambiguous",
                    best.score,
                    second_score,
                    best.page,
                    best.region.score if best.region else None,
                )
            )
            continue
        if best.page < last_page:
            diagnostics.append(
                AlignmentDiagnostic(
                    block.id,
                    block.kind,
                    "page-regression",
                    best.score,
                    second_score,
                    best.page,
                    best.region.score if best.region else None,
                )
            )
            continue
        mapping, clipped, has_region = _mapping(block, best, *page_dimensions[best.page])
        mappings.append(mapping)
        if has_region:
            region_mapped_blocks += 1
        else:
            page_only_mapped_blocks += 1
            diagnostics.append(
                AlignmentDiagnostic(
                    block.id,
                    block.kind,
                    "page-only",
                    best.score,
                    second_score,
                    best.page,
                    best.region.score if best.region else None,
                )
            )
        if clipped:
            diagnostics.append(
                AlignmentDiagnostic(
                    block.id,
                    block.kind,
                    "geometry-clipped",
                    best.score,
                    second_score,
                    best.page,
                    best.region.score if best.region else None,
                )
            )
        consumed.update(best.evidence_ids)
        mapped_bytes += block.end - block.start
        cursor = max(cursor, best.end)
        last_page = best.page
    total_bytes = sum(block.end - block.start for block in blocks)
    return AlignmentResult(
        tuple(mappings),
        blocks,
        tuple(diagnostics),
        len(mappings),
        mapped_bytes,
        region_mapped_blocks,
        page_only_mapped_blocks,
        len(blocks),
        total_bytes,
    )
