"""Safe semantic table serialization for Markdown artifacts."""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Iterable, Sequence

SEPARATOR_RE = re.compile(r"^:?-{3,}:?$")
INLINE_TAGS = {
    "b": "strong",
    "strong": "strong",
    "i": "em",
    "em": "em",
    "code": "code",
    "sub": "sub",
    "sup": "sup",
}
VOID_INLINE_TAGS = {"br"}
SCOPES = {"row", "col", "rowgroup", "colgroup"}
MAX_SPAN = 1_000


@dataclass(frozen=True)
class TableCell:
    text: str
    header: bool = False
    colspan: int = 1
    rowspan: int = 1
    scope: str | None = None


@dataclass(frozen=True)
class TableRow:
    cells: tuple[TableCell, ...]

    def __init__(self, cells: Iterable[TableCell]):
        object.__setattr__(self, "cells", tuple(cells))


class _InlineSanitizer(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.output: list[str] = []
        self.stack: list[str] = []
        self.valid = True

    def handle_starttag(self, tag: str, attrs) -> None:
        if attrs:
            self.valid = False
            return
        if tag in VOID_INLINE_TAGS:
            self.output.append("<br>")
            return
        normalized = INLINE_TAGS.get(tag)
        if normalized is None:
            self.valid = False
            return
        self.stack.append(normalized)
        self.output.append(f"<{normalized}>")

    def handle_startendtag(self, tag: str, attrs) -> None:
        if tag not in VOID_INLINE_TAGS or attrs:
            self.valid = False
            return
        self.output.append("<br>")

    def handle_endtag(self, tag: str) -> None:
        normalized = INLINE_TAGS.get(tag)
        if normalized is None or not self.stack or self.stack[-1] != normalized:
            self.valid = False
            return
        self.stack.pop()
        self.output.append(f"</{normalized}>")

    def handle_data(self, data: str) -> None:
        self.output.append(html.escape(data, quote=True))

    def handle_comment(self, data: str) -> None:
        self.valid = False

    def handle_decl(self, decl: str) -> None:
        self.valid = False


def _safe_inline(value: str) -> str:
    parser = _InlineSanitizer()
    try:
        parser.feed(value)
        parser.close()
    except Exception:
        parser.valid = False
    if not parser.valid or parser.stack:
        return html.escape(value, quote=True).replace("\n", "<br>")
    return "".join(parser.output).replace("\n", "<br>")


def _validate_grid(rows: Sequence[TableRow]) -> int:
    if not rows:
        raise ValueError("a semantic table needs at least one row")
    occupied: dict[int, set[int]] = {}
    width = 0
    for row_index, row in enumerate(rows):
        if not row.cells:
            raise ValueError("a semantic table row needs at least one cell")
        used = occupied.setdefault(row_index, set())
        column = 0
        for cell in row.cells:
            if isinstance(cell.colspan, bool) or not 1 <= cell.colspan <= MAX_SPAN:
                raise ValueError("table colspan must be a bounded positive integer")
            if isinstance(cell.rowspan, bool) or not 1 <= cell.rowspan <= MAX_SPAN:
                raise ValueError("table rowspan must be a bounded positive integer")
            if cell.scope is not None and (not cell.header or cell.scope not in SCOPES):
                raise ValueError("table scope is valid only on header cells")
            while column in used:
                column += 1
            columns = range(column, column + cell.colspan)
            for target_row in range(row_index, row_index + cell.rowspan):
                target = occupied.setdefault(target_row, set())
                if any(item in target for item in columns):
                    raise ValueError("table cell spans overlap")
                target.update(columns)
            column += cell.colspan
        width = max(width, max(used) + 1)
    if any(row_index >= len(rows) for row_index in occupied):
        raise ValueError("table rowspan extends beyond the final row")
    expected = set(range(width))
    if any(occupied[index] != expected for index in range(len(rows))):
        raise ValueError("table rows do not form one rectangular grid")
    return width


def _cell_html(cell: TableCell, indent: str) -> str:
    tag = "th" if cell.header else "td"
    attributes = []
    if cell.colspan != 1:
        attributes.append(f'colspan="{cell.colspan}"')
    if cell.rowspan != 1:
        attributes.append(f'rowspan="{cell.rowspan}"')
    if cell.scope is not None:
        attributes.append(f'scope="{cell.scope}"')
    suffix = " " + " ".join(attributes) if attributes else ""
    return f"{indent}<{tag}{suffix}>{_safe_inline(cell.text)}</{tag}>"


def semantic_html_table(
    *,
    head: Sequence[TableRow],
    body: Sequence[TableRow],
    caption: str | None = None,
) -> str:
    """Serialize one rectangular logical grid using a strict HTML allowlist."""
    rows = [*head, *body]
    _validate_grid(rows)
    output = ["<table>"]
    if caption is not None:
        output.append(f"  <caption>{_safe_inline(caption)}</caption>")
    if head:
        output.append("  <thead>")
        for row in head:
            output.append("    <tr>")
            output.extend(_cell_html(cell, "      ") for cell in row.cells)
            output.append("    </tr>")
        output.append("  </thead>")
    if body:
        output.append("  <tbody>")
        for row in body:
            output.append("    <tr>")
            output.extend(_cell_html(cell, "      ") for cell in row.cells)
            output.append("    </tr>")
        output.append("  </tbody>")
    output.append("</table>")
    return "\n".join(output)


def _split_pipe_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        raise ValueError("Markdown table rows must have leading and trailing pipes")
    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for character in stripped[1:-1]:
        if escaped:
            current.append(character)
            escaped = False
        elif character == "\\":
            current.append(character)
            escaped = True
        elif character == "|":
            cells.append("".join(current).strip())
            current = []
        else:
            current.append(character)
    if escaped:
        raise ValueError("Markdown table row ends with an incomplete escape")
    cells.append("".join(current).strip())
    return cells


def _spanned_cells(values: Sequence[str], *, header_row: bool) -> TableRow:
    nonempty = [value for value in values if value]
    label_row = (
        not header_row
        and len(nonempty) >= max(2, len(values) // 2)
        and all(len(value) <= 16 and " " not in value for value in nonempty)
    )
    cells: list[TableCell] = []
    index = 0
    while index < len(values):
        value = values[index]
        end = index + 1
        if value:
            while end < len(values) and not values[end]:
                end += 1
        colspan = end - index
        is_first_sparse_label = bool(value) and index == 0 and colspan > 1
        is_header = header_row or label_row or is_first_sparse_label
        scope = None
        if is_header:
            if header_row:
                scope = "colgroup" if colspan > 1 else "col"
            elif label_row:
                scope = "col"
            else:
                scope = "row"
        cells.append(TableCell(value, is_header, colspan, 1, scope))
        index = end
    return TableRow(cells)


def markdown_table_to_html(source: str) -> str:
    """Convert one provider pipe table, interpreting blank runs as merged cells."""
    lines = [line for line in source.strip().splitlines() if line.strip()]
    if len(lines) < 3:
        raise ValueError("Markdown table needs a header, separator, and body")
    parsed = [_split_pipe_row(line) for line in lines]
    width = len(parsed[0])
    if width == 0 or any(len(row) != width for row in parsed):
        raise ValueError("Markdown table rows have inconsistent widths")
    if not all(SEPARATOR_RE.fullmatch(cell) for cell in parsed[1]):
        raise ValueError("Markdown table has an invalid separator row")
    head = [_spanned_cells(parsed[0], header_row=True)]
    body = [_spanned_cells(row, header_row=False) for row in parsed[2:]]
    return semantic_html_table(head=head, body=body)
