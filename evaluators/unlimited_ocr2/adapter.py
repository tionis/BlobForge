"""Package cached Unlimited-OCR-2 runner output through BlobForge's converter ABI.

The adapter never runs the model. It reads the units that
``run_unlimited_ocr2.py`` retained, verifies their raw digests and rendered
rasters, and emits a converter bundle v1 for the shared MDAF builder:

- headings come only from model-emitted ``<LEVELn>`` titles;
- ``<MERGE>`` and ``append_text`` join a block to the preceding paragraph;
- HTML tables are re-serialized through the strict semantic-table allowlist,
  and a grid that is not rectangular is kept verbatim in a fenced block;
- header, footer, page-number, empty and malformed blocks stay out of the
  Markdown but remain in the retained native output;
- image blocks become raster crops of the rendered page (derived assets, not
  embedded PDF images); and
- each block maps to its page and, where the raster transform is a plain
  unrotated scale, to a point rectangle. The model reports no calibrated
  confidence, so mappings carry none.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from html.parser import HTMLParser
from importlib.metadata import version
from pathlib import Path
from typing import Any

from PIL import Image

from blobforge.normalization.tables import TableCell, TableRow, semantic_html_table

CONTRACT = "dev.tionis.blobforge.converter-bundle/v1"
PACKAGER_VERSION = "1.0.0"
NAMESPACE = "com.baidu.unlimited-ocr-2"
METHOD = "dev.tionis.blobforge/unlimited-ocr2-grounding"
BLOCK_RE = re.compile(r"(?P<label>[a-z_]+)?(?:<LEVEL(?P<level>\d+)>)?(?P<box>\[[^\]<\n]*\])?(?P<fork><FORK>)?")
MERGE = "<MERGE>"
DROPPED_LABELS = {"header", "footer", "page_number"}
PARAGRAPH_LABELS = {"text", "other", "cross_text", "append_text", "title"}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_box(raw: str | None) -> list[int] | None:
    try:
        box = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        return None
    if (isinstance(box, list) and len(box) == 4 and all(isinstance(v, int) and 0 <= v <= 999 for v in box)
            and box[0] < box[2] and box[1] < box[3]):
        return box
    return None


def page_blocks(page_text: str) -> list[dict[str, Any]]:
    blocks = []
    merge_pending = False
    for segment in page_text.split("<BLOCK>")[1:]:
        match = BLOCK_RE.match(segment)
        body = segment[match.end():]
        merges_next = body.rstrip().endswith(MERGE)
        if merges_next:
            body = body.rstrip()[:-len(MERGE)]
        blocks.append({
            "label": match.group("label") or "",
            "level": int(match.group("level")) if match.group("level") else None,
            "box": parse_box(match.group("box")),
            "well_formed": bool(match.group("label") and match.group("fork")),
            "text": body.strip(),
            "merged_from_previous": merge_pending,
        })
        merge_pending = merges_next
    return blocks


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[TableCell]] = []
        self.cell: dict[str, Any] | None = None
        self.valid = True
        self.tables = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        values = dict(attrs)
        if tag == "table":
            self.tables += 1
        elif tag == "tr":
            self.rows.append([])
        elif tag in ("td", "th"):
            if not self.rows or self.cell is not None:
                self.valid = False
                return
            try:
                colspan, rowspan = int(values.get("colspan") or 1), int(values.get("rowspan") or 1)
            except ValueError:
                self.valid = False
                return
            self.cell = {"header": tag == "th", "colspan": colspan, "rowspan": rowspan, "text": []}
        elif tag == "br" and self.cell is not None:
            self.cell["text"].append("\n")
        else:
            self.valid = False

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self.cell is not None:
            cell = self.cell
            self.rows[-1].append(TableCell("".join(cell["text"]).strip(), cell["header"],
                                           cell["colspan"], cell["rowspan"]))
            self.cell = None

    def handle_data(self, data: str) -> None:
        if self.cell is not None:
            self.cell["text"].append(data)
        elif data.strip():
            self.valid = False


def table_markdown(raw: str) -> tuple[str, bool]:
    parser = _TableParser()
    try:
        parser.feed(raw)
        parser.close()
        if parser.valid and parser.tables == 1 and parser.rows:
            return semantic_html_table(head=[], body=[TableRow(row) for row in parser.rows]), True
    except ValueError:
        pass
    # Keep a non-rectangular or unparseable grid verbatim rather than inventing cells.
    return "```html\n" + raw.replace("```", "&#96;&#96;&#96;") + "\n```", False


class Builder:
    """Accumulates Markdown elements and their source pieces."""

    def __init__(self) -> None:
        self.elements: list[dict[str, Any]] = []

    def add(self, kind: str, text: str, source: dict[str, Any], level: int | None = None) -> None:
        self.elements.append({"kind": kind, "level": level, "pieces": [(text, source)]})

    def join(self, text: str, source: dict[str, Any]) -> bool:
        if not self.elements or self.elements[-1]["kind"] != "paragraph":
            return False
        self.elements[-1]["pieces"].append((text, source))
        return True

    def render(self) -> tuple[str, list[dict[str, Any]]]:
        output = bytearray()
        mappings: list[dict[str, Any]] = []
        for element in self.elements:
            if output:
                output += b"\n\n"
            if element["kind"] == "heading":
                output += ("#" * element["level"] + " ").encode("utf-8")
            for index, (text, source) in enumerate(element["pieces"]):
                if index:
                    previous = element["pieces"][index - 1][0]
                    # A trailing hyphen is kept as printed; only the space is omitted.
                    output += b"" if previous.endswith("-") else b" "
                start = len(output)
                output += text.encode("utf-8")
                mappings.append({"document": {"start": start, "end": len(output)}, "source": source,
                                 "method": METHOD})
        output += b"\n"
        return output.decode("utf-8"), mappings


def locator(meta: dict[str, Any], page_index: int, box: list[int] | None) -> dict[str, Any]:
    selectors: list[dict[str, Any]] = [{"type": "interval", "unit": "page", "start": page_index,
                                        "end": page_index + 1}]
    plain = (meta.get("rotation") == 0 and meta.get("cropbox") == meta.get("page_rect")
             and meta.get("cropbox", [None])[:2] == [0.0, 0.0])
    if box is not None and plain:
        scale = meta["dpi"] / 72
        x1, y1, x2, y2 = (v / 999 for v in box)
        x, y = x1 * meta["width"] / scale, y1 * meta["height"] / scale
        selectors.append({"type": "rectangle", "unit": "point", "x": round(x, 3), "y": round(y, 3),
                          "width": round((x2 - x1) * meta["width"] / scale, 3),
                          "height": round((y2 - y1) * meta["height"] / scale, 3)})
    return {"source_id": "document", "selectors": selectors}


def main() -> int:
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if request.get("contract") != CONTRACT:
        raise ValueError("unsupported converter request contract")
    parameters = request["parameters"]
    output = Path(request["output_dir"])
    source = Path(request["source_path"])
    if sha256_bytes(source.read_bytes()) != parameters["source_sha256"]:
        raise ValueError("source does not match the runner plan")
    rendered_root = Path(parameters["rendered_root"])
    data = output / "data"
    (data / "assets").mkdir(parents=True)
    (data / "native").mkdir()

    builder = Builder()
    members: list[dict[str, Any]] = []
    counts = {"tables": 0, "tables_kept_verbatim": 0, "dropped": 0, "empty": 0, "malformed": 0,
              "title_without_level": 0, "joined": 0, "images": 0, "rectangles_withheld": 0}
    seen_pages: list[int] = []
    for unit_dir in map(Path, parameters["unit_dirs"]):
        result = json.loads((unit_dir / "result.json").read_text(encoding="utf-8"))
        raw = (unit_dir / "raw.txt").read_bytes()
        if sha256_bytes(raw) != result["raw_sha256"]:
            raise ValueError(f"{unit_dir}: raw output does not match its recorded digest")
        if result["recipe_id"] != parameters["recipe_id"]:
            raise ValueError(f"{unit_dir}: unit recipe differs from the packaged recipe")
        key = unit_dir.name
        for name, media in (("raw.txt", "text/plain"), ("result.json", "application/json"),
                            ("token_ids.json", "application/json")):
            target = data / "native" / f"{key}.{name}"
            target.write_bytes((unit_dir / name).read_bytes())
            members.append({"path": f"renditions/{NAMESPACE}/{key}.{name}", "file": f"data/native/{key}.{name}",
                            "role": "rendition", "media_type": media, "namespace": NAMESPACE})
        page_texts = raw.decode("utf-8").split("<PAGE>")[1:]
        if len(page_texts) != len(result["pages"]):
            raise ValueError(f"{unit_dir}: page markers do not match the unit's pages")
        for page_no, page_text, meta in zip(result["pages"], page_texts, result["rendered"]):
            seen_pages.append(page_no)
            raster = rendered_root / Path(meta["image"]).name
            raster_ok = raster.is_file() and sha256_bytes(raster.read_bytes()) == meta["png_sha256"]
            image_index = 0
            for block in page_blocks(page_text):
                source_locator = locator(meta, page_no - 1, block["box"])
                if block["box"] is not None and len(source_locator["selectors"]) == 1:
                    counts["rectangles_withheld"] += 1
                label = block["label"]
                if not block["well_formed"] or (block["box"] is None and label != "image"):
                    counts["malformed"] += 1
                elif label in DROPPED_LABELS:
                    counts["dropped"] += 1
                elif label == "image":
                    if block["box"] is None or not raster_ok:
                        counts["malformed"] += 1
                        continue
                    image_index += 1
                    name = f"p{page_no:04d}-{image_index:02d}.png"
                    with Image.open(raster) as page_image:
                        width, height = page_image.size
                        x1, y1, x2, y2 = block["box"]
                        page_image.crop((round(x1 / 999 * width), round(y1 / 999 * height),
                                         round(x2 / 999 * width), round(y2 / 999 * height))
                                        ).save(data / "assets" / name, format="PNG", optimize=True)
                    members.append({"path": f"assets/{name}", "file": f"data/assets/{name}", "role": "asset",
                                    "media_type": "image/png"})
                    builder.add("image", f"![](assets/{name})", source_locator)
                    counts["images"] += 1
                elif not block["text"]:
                    counts["empty"] += 1
                elif label == "table":
                    markdown, semantic = table_markdown(block["text"])
                    counts["tables"] += 1
                    counts["tables_kept_verbatim"] += not semantic
                    builder.add("table", markdown, source_locator)
                elif label == "title" and block["level"]:
                    builder.add("heading", " ".join(block["text"].split()).replace("<", "&lt;"), source_locator,
                                level=min(max(block["level"], 1), 6))
                else:
                    counts["title_without_level"] += label == "title"
                    # Escape "<" so model text can never become raw HTML in the Markdown.
                    text = " ".join(block["text"].split()).replace("<", "&lt;")
                    joinable = block["merged_from_previous"] or label == "append_text"
                    if joinable and builder.join(text, source_locator):
                        counts["joined"] += 1
                    else:
                        builder.add("paragraph", text, source_locator)
    if sorted(seen_pages) != list(range(1, len(seen_pages) + 1)):
        raise ValueError(f"units do not cover pages 1..N exactly once: {sorted(seen_pages)}")

    markdown, mappings = builder.render()
    (data / "text.md").write_text(markdown, encoding="utf-8")
    (data / "source-map.json").write_text(json.dumps({"mappings": mappings, "references": []}, indent=2) + "\n",
                                          encoding="utf-8")
    recipe = Path(parameters["recipe_path"])
    (data / "native" / "recipe.json").write_bytes(recipe.read_bytes())
    members.append({"path": f"renditions/{NAMESPACE}/recipe.json", "file": "data/native/recipe.json",
                    "role": "rendition", "media_type": "application/json", "namespace": NAMESPACE})
    environment = parameters["environment"]
    packages = environment["packages"]
    bundle = {
        "contract": CONTRACT,
        "text_path": "data/text.md",
        "source_map": "data/source-map.json",
        "members": members,
        "tool": {"name": "blobforge-unlimited-ocr2-evaluator", "version": "0.1.0"},
        "additional_tools": [
            {"name": "transformers", "version": packages["transformers"]},
            {"name": "torch", "version": packages["torch"]},
            {"name": "pymupdf", "version": packages["pymupdf"]},
            {"name": "blobforge-unlimited-ocr2-packager", "version": PACKAGER_VERSION},
            {"name": "pillow", "version": version("pillow")},
        ],
        "markdown_features": ["raw-html", "semantic-html-table-v1"],
        "models": [{"provider": "baidu", "identifier": environment["model"]["repo"],
                    "revision": environment["model"]["revision"], "resolution": "pinned"}],
        "parameters": {
            "run": parameters["run"],
            "mode": parameters["mode"],
            "evaluation_recipe_id": parameters["recipe_id"],
            "units": [Path(unit).name for unit in parameters["unit_dirs"]],
            "gpu": environment.get("nvidia_smi"),
            "packager": PACKAGER_VERSION,
        },
        "diagnostics": [
            {"severity": "info", "code": "packaging-counts", "message": json.dumps(counts, sort_keys=True)},
            {"severity": "warning", "code": "derived-image-crops",
             "message": "Assets are raster crops of model image boxes, not embedded PDF images."},
            {"severity": "warning", "code": "no-calibrated-confidence",
             "message": "Unlimited-OCR-2 reports no block confidence; mappings carry none."},
            {"severity": "warning", "code": "evaluation-only",
             "message": "Evaluation recipe identity; not a registered BlobForge recipe."},
        ],
    }
    (output / "bundle.json").write_text(json.dumps(bundle, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
