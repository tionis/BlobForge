"""Deterministic, blinded, source-backed MDAF review bundles."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .mdaf import blake3_file, validate_mdaf
from .mdaf.digest import blake3_bytes, canonical_json_bytes

REVIEW_FORMAT_V1 = "dev.tionis.blobforge.review/v1"
REVIEW_FORMAT_V2 = "dev.tionis.blobforge.review/v2"
REVIEW_FORMAT = REVIEW_FORMAT_V2
REVIEW_SUMMARY_FORMAT = "dev.tionis.blobforge.review-summary/v1"
V1_DIMENSIONS = (
    "text",
    "reading-order",
    "hierarchy",
    "lists",
    "tables",
    "assets",
    "references",
    "source-mapping",
    "wiki-utility",
)
DEFAULT_DIMENSIONS = (
    "text",
    "inline-formatting",
    *V1_DIMENSIONS[1:],
)
DIMENSIONS_BY_FORMAT = {
    REVIEW_FORMAT_V1: V1_DIMENSIONS,
    REVIEW_FORMAT_V2: DEFAULT_DIMENSIONS,
}
DIMENSION_GUIDANCE = {
    "text": "Complete and accurate words, punctuation, symbols, and paragraphs.",
    "inline-formatting": (
        "Correct bold, emphasis, code, superscript, and other meaningful inline styling."
    ),
    "reading-order": "Narrative order across columns, sidebars, captions, headers, and footers.",
    "hierarchy": "Sensible Markdown levels for titles, sections, subsections, and callouts.",
    "lists": "Correct bullets, numbering, indentation, nesting, and continuation paragraphs.",
    "tables": "Correct row, column, header, and cell associations; visual similarity is secondary.",
    "assets": "Correct illustrations, diagrams, captions, references, and placement.",
    "references": "Retained footnotes, citations, links, and figure, table, or page references.",
    "source-mapping": "Shown text belongs to this PDF page without meaningful bleed or omission.",
    "wiki-utility": "Overall manual effort needed to produce clean, navigable wiki content.",
}
SCORE_GUIDANCE = (
    ("1", "Unusable — rewriting is easier"),
    ("2", "Poor — substantial correction"),
    ("3", "Acceptable — routine cleanup"),
    ("4", "Good — minor corrections"),
    ("5", "Excellent — publication-ready"),
    ("N/A", "Not present or not assessable"),
)
MARKDOWN_LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^\s)]+)(\))")
PREVIEW_SIGNATURES = {
    "image/png": ("png", lambda value: value.startswith(b"\x89PNG\r\n\x1a\n")),
    "image/jpeg": ("jpg", lambda value: value.startswith(b"\xff\xd8\xff")),
    "image/gif": ("gif", lambda value: value.startswith((b"GIF87a", b"GIF89a"))),
    "image/webp": (
        "webp",
        lambda value: value.startswith(b"RIFF") and value[8:12] == b"WEBP",
    ),
}


@dataclass(frozen=True)
class ReviewBundleResult:
    root: Path
    key_path: Path
    campaign_digest: str
    artifacts: int
    pages: int


def _json_object(path: str | Path, *, max_bytes: int = 10 * 1024**2) -> dict[str, Any]:
    source = Path(path)
    if source.stat().st_size > max_bytes:
        raise ValueError(f"review JSON exceeds {max_bytes} bytes: {source}")
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid review JSON {source}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"review JSON must contain an object: {source}")
    return value


def _candidate_label(index: int) -> str:
    return chr(ord("A") + index) if index < 26 else f"C{index + 1}"


def summarize_review_result(
    result_path: str | Path, key_path: str | Path
) -> dict[str, Any]:
    """Validate a blinded export against its private key and summarize it."""
    result = _json_object(result_path)
    key = _json_object(key_path)
    review_format = result.get("format")
    if review_format != key.get("format") or review_format not in DIMENSIONS_BY_FORMAT:
        raise ValueError("review result and key must use the supported review format")
    digest = result.get("campaign_digest")
    if not isinstance(digest, str) or digest != key.get("campaign_digest"):
        raise ValueError("review result does not match the private campaign key")
    campaign_body = {
        field: key.get(field)
        for field in (
            "format",
            "source_digest",
            "artifact_identities",
            "pages",
            "dimensions",
            "seed_sha256",
        )
    }
    if blake3_bytes(canonical_json_bytes(campaign_body)) != digest:
        raise ValueError("private campaign key does not match its campaign digest")
    dimensions = key.get("dimensions")
    pages = key.get("pages")
    candidates = key.get("candidates")
    if (
        dimensions != list(DIMENSIONS_BY_FORMAT[review_format])
        or not isinstance(pages, list)
        or not all(isinstance(page, int) for page in pages)
        or not isinstance(candidates, list)
        or len(candidates) < 2
        or not all(isinstance(candidate, dict) for candidate in candidates)
    ):
        raise ValueError("private campaign key has an invalid review contract")
    labels = [candidate.get("label") for candidate in candidates]
    if any(not isinstance(label, str) for label in labels) or len(set(labels)) != len(labels):
        raise ValueError("private campaign key has invalid candidate labels")
    seed = key.get("seed")
    identities = key.get("artifact_identities")
    if (
        not isinstance(seed, str)
        or "sha256:" + hashlib.sha256(seed.encode("utf-8")).hexdigest()
        != key.get("seed_sha256")
        or not isinstance(identities, list)
        or not all(isinstance(identity, str) for identity in identities)
        or len(set(identities)) != len(identities)
    ):
        raise ValueError("private campaign key cannot verify its label seed")
    ranked_identities = sorted(
        identities,
        key=lambda identity: hashlib.sha256(f"{seed}\0{identity}".encode("utf-8")).digest(),
    )
    expected_assignments = [
        (_candidate_label(index), identity)
        for index, identity in enumerate(ranked_identities)
    ]
    observed_assignments = [
        (candidate.get("label"), candidate.get("identity")) for candidate in candidates
    ]
    if observed_assignments != expected_assignments:
        raise ValueError("private campaign key has an invalid label assignment")
    scores = result.get("scores")
    if not isinstance(scores, dict):
        raise ValueError("review result scores must be an object")

    values: dict[str, dict[str, list[int]]] = {
        label: {dimension: [] for dimension in dimensions} for label in labels
    }
    na_counts: dict[str, dict[str, int]] = {
        label: {dimension: 0 for dimension in dimensions} for label in labels
    }
    pages_reviewed: set[int] = set()
    ratings_recorded = 0
    na_recorded = 0
    for page_text, page_entry in scores.items():
        try:
            page = int(page_text)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid reviewed page index: {page_text!r}") from exc
        if str(page) != page_text or page not in pages or not isinstance(page_entry, dict):
            raise ValueError(f"reviewed page is outside the campaign: {page_text!r}")
        notes = page_entry.get("notes", "")
        if not isinstance(notes, str):
            raise ValueError(f"review notes must be text on page {page + 1}")
        ratings = page_entry.get("ratings", {})
        if not isinstance(ratings, dict) or set(ratings) - set(dimensions):
            raise ValueError(f"review contains an unknown rating dimension on page {page + 1}")
        page_has_review = bool(notes.strip())
        for dimension, candidate_values in ratings.items():
            if not isinstance(candidate_values, dict) or set(candidate_values) - set(labels):
                raise ValueError(
                    f"review contains an unknown candidate for {dimension} on page {page + 1}"
                )
            for label, score in candidate_values.items():
                if score == "na":
                    na_counts[label][dimension] += 1
                    na_recorded += 1
                elif isinstance(score, str) and score in {"1", "2", "3", "4", "5"}:
                    values[label][dimension].append(int(score))
                    ratings_recorded += 1
                elif score != "":
                    raise ValueError(
                        f"invalid score for {label}/{dimension} on page {page + 1}: {score!r}"
                    )
                page_has_review = page_has_review or score != ""
        if page_has_review:
            pages_reviewed.add(page)

    summary_candidates = []
    for candidate in candidates:
        label = candidate["label"]
        tools = candidate.get("tools", [])
        converter = tools[0] if tools and isinstance(tools[0], dict) else None
        summary_candidates.append(
            {
                "label": label,
                "identity": candidate.get("identity"),
                "producer": candidate.get("producer"),
                "converter": converter,
                "tools": tools,
                "models": candidate.get("models", []),
                "dimensions": {
                    dimension: {
                        "count": len(values[label][dimension]),
                        "n_a": na_counts[label][dimension],
                        "mean": (
                            round(
                                sum(values[label][dimension])
                                / len(values[label][dimension]),
                                3,
                            )
                            if values[label][dimension]
                            else None
                        ),
                    }
                    for dimension in dimensions
                },
            }
        )
    possible = len(pages) * len(labels) * len(dimensions)
    completed = ratings_recorded + na_recorded
    return {
        "format": REVIEW_SUMMARY_FORMAT,
        "campaign_digest": digest,
        "result_exported_at": result.get("exported_at"),
        "coverage": {
            "campaign_pages": len(pages),
            "reviewed_pages": len(pages_reviewed),
            "ratings": ratings_recorded,
            "n_a": na_recorded,
            "possible_slots": possible,
            "completed_slots": completed,
            "fraction": round(completed / possible, 6) if possible else 0,
        },
        "candidates": summary_candidates,
    }


def parse_page_selection(value: str | None, available: Iterable[int]) -> tuple[int, ...]:
    """Parse human-facing one-based pages and return zero-based page indices."""
    allowed = set(available)
    if value is None:
        return tuple(sorted(allowed))
    selected: set[int] = set()
    for part in value.split(","):
        token = part.strip()
        if not token:
            raise ValueError("page selection contains an empty item")
        if "-" in token:
            start_text, end_text = token.split("-", 1)
            start, end = int(start_text), int(end_text)
            if start < 1 or end < start:
                raise ValueError(f"invalid one-based page range: {token}")
            selected.update(range(start - 1, end))
        else:
            page = int(token)
            if page < 1:
                raise ValueError(f"invalid one-based page: {token}")
            selected.add(page - 1)
    missing = selected - allowed
    if missing:
        shown = ", ".join(str(page + 1) for page in sorted(missing))
        raise ValueError(f"selected pages are not mapped by every artifact: {shown}")
    return tuple(sorted(selected))


def _page_text(text: bytes, source_map: dict[str, Any]) -> dict[int, str]:
    spans: dict[int, list[tuple[int, int]]] = {}
    for mapping in source_map.get("mappings", []):
        document = mapping.get("document", {})
        start, end = document.get("start"), document.get("end")
        if not isinstance(start, int) or not isinstance(end, int) or end <= start:
            continue
        for selector in mapping.get("source", {}).get("selectors", []):
            if selector.get("type") != "interval" or selector.get("unit") != "page":
                continue
            page_start, page_end = selector.get("start"), selector.get("end")
            if not isinstance(page_start, int) or not isinstance(page_end, int):
                continue
            if page_end != page_start + 1:
                raise ValueError(
                    "blinded review requires page-exact source mappings; "
                    f"found page interval [{page_start}, {page_end})"
                )
            spans.setdefault(page_start, []).append((start, end))
    result = {}
    for page, ranges in spans.items():
        unique = sorted(set(ranges))
        # Mapping methods may publish nested regions. Retain each source byte at
        # most once so the review view does not manufacture duplicated text.
        merged: list[list[int]] = []
        for start, end in unique:
            if merged and start <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        result[page] = "\n\n".join(
            text[start:end].decode("utf-8") for start, end in merged
        )
    return result


def _asset_target(target: str) -> str:
    return target.split("#", 1)[0].split("?", 1)[0]


def _prepare_candidate(
    item: dict[str, Any], label: str, selected_pages: tuple[int, ...]
) -> tuple[dict[str, str], dict[str, list[dict[str, Any]]], list[tuple[str, str]]]:
    """Neutralize linked asset names and select safe raster previews."""
    referenced: list[str] = []
    for page in selected_pages:
        for match in MARKDOWN_LINK_RE.finditer(item["pages"][page]):
            target = _asset_target(match.group(2))
            if target in item["assets"] and target not in referenced:
                referenced.append(target)

    neutral: dict[str, dict[str, Any]] = {}
    copies: list[tuple[str, str]] = []
    with zipfile.ZipFile(item["path"]) as archive:
        for index, source_name in enumerate(referenced, start=1):
            metadata = item["assets"][source_name]
            signature = PREVIEW_SIGNATURES.get(metadata.get("media_type"))
            with archive.open(source_name) as member:
                header = member.read(12)
            if signature and signature[1](header):
                output_name = f"assets/{label}/{index:03d}.{signature[0]}"
                neutral[source_name] = {
                    "path": output_name,
                    "media_type": metadata["media_type"],
                    "previewable": True,
                }
                copies.append((source_name, output_name))
            else:
                neutral[source_name] = {
                    "media_type": metadata.get("media_type", "application/octet-stream"),
                    "previewable": False,
                }

    pages: dict[str, str] = {}
    galleries: dict[str, list[dict[str, Any]]] = {}
    for page in selected_pages:
        page_assets: list[dict[str, Any]] = []

        def replace(match: re.Match[str]) -> str:
            source_name = _asset_target(match.group(2))
            asset = neutral.get(source_name)
            if asset is None:
                return match.group(0)
            if asset not in page_assets:
                page_assets.append(asset)
            target = asset.get("path", f"asset-unavailable-{referenced.index(source_name) + 1}")
            return f"{match.group(1)}{target}{match.group(3)}"

        pages[str(page)] = MARKDOWN_LINK_RE.sub(replace, item["pages"][page])
        galleries[str(page)] = page_assets
    return pages, galleries, copies


def _artifact(path: Path) -> dict[str, Any]:
    validated = validate_mdaf(path)
    with zipfile.ZipFile(path) as archive:
        text = archive.read("text.md")
        if "source-map.json" not in archive.namelist():
            raise ValueError(f"review artifact has no source map: {path}")
        source_map = json.loads(archive.read("source-map.json"))
        provenance = json.loads(archive.read("provenance.json"))
    sources = validated.manifest.get("sources", [])
    if len(sources) != 1:
        raise ValueError(f"review requires exactly one source: {path}")
    activity = provenance["activities"][-1]
    assets = {
        member["path"]: {
            "media_type": member.get("media_type", "application/octet-stream")
        }
        for member in validated.manifest.get("members", [])
        if member.get("role") == "asset" and member.get("path", "").startswith("assets/")
    }
    return {
        "path": str(path.resolve()),
        "identity": validated.identity,
        "source_digest": sources[0]["digest"],
        "producer": validated.manifest["producer"],
        "tools": activity.get("tools", []),
        "models": activity.get("models", []),
        "pages": _page_text(text, source_map),
        "assets": assets,
    }


def _html(public: dict[str, Any]) -> str:
    embedded = json.dumps(public, ensure_ascii=False, separators=(",", ":")).replace(
        "<", "\\u003c"
    )
    score_guide = "".join(
        f"<li><strong>{score}</strong> {description}</li>"
        for score, description in SCORE_GUIDANCE
    )
    dimensions = "".join(
        f'<tr><th>{dimension}<small>{DIMENSION_GUIDANCE[dimension]}</small></th>'
        f'<td class="scores" data-dimension="{dimension}"></td></tr>'
        for dimension in public["dimensions"]
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BlobForge blinded conversion review</title>
<style>
:root{{--bg:#11151b;--panel:#1a2029;--line:#344052;--text:#edf2f7;--muted:#a7b3c2;--accent:#77bdfb}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px system-ui,sans-serif}}
header{{position:sticky;top:0;z-index:3;background:#11151bf2;border-bottom:1px solid var(--line);padding:.8rem 1rem;display:flex;gap:1rem;align-items:center;flex-wrap:wrap}}
h1{{font-size:1.05rem;margin:0}}button,select,textarea{{background:var(--panel);color:var(--text);border:1px solid var(--line);border-radius:5px;padding:.45rem}}
main{{display:grid;grid-template-columns:minmax(320px,38vw) 1fr;min-height:calc(100vh - 60px)}}
.source{{border-right:1px solid var(--line);position:sticky;top:61px;height:calc(100vh - 61px)}}iframe{{width:100%;height:100%;border:0;background:white}}
.review{{padding:1rem;min-width:0}}.columns{{display:grid;grid-template-columns:repeat(var(--count),minmax(300px,1fr));gap:.8rem;overflow:auto}}
.candidate{{background:var(--panel);border:1px solid var(--line);border-radius:7px;min-width:300px}}.candidate h2{{font-size:1rem;margin:0;padding:.65rem;border-bottom:1px solid var(--line);color:var(--accent)}}
pre{{white-space:pre-wrap;overflow-wrap:anywhere;margin:0;padding:.8rem;font:13px/1.45 ui-monospace,monospace}}
table{{width:100%;border-collapse:collapse;margin-top:1rem}}th,td{{border:1px solid var(--line);padding:.45rem;text-align:left}}.scores{{display:flex;gap:.6rem;flex-wrap:wrap}}
.score{{display:flex;align-items:center;gap:.3rem}}textarea{{width:100%;min-height:8rem;margin-top:1rem}}small{{color:var(--muted);display:block;font-weight:normal;margin-top:.2rem}}
.guide{{margin-bottom:1rem;background:var(--panel);border:1px solid var(--line);border-radius:7px;padding:.7rem}}.guide summary{{cursor:pointer;color:var(--accent);font-weight:600}}.guide ol{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:.35rem 1.2rem}}
.assets{{border-top:1px solid var(--line);padding:.7rem;display:grid;gap:.6rem}}.assets h3{{font-size:.85rem;margin:0;color:var(--muted)}}.assets img{{display:block;max-width:100%;max-height:320px;margin:auto;background:white;border:1px solid var(--line)}}.unavailable{{color:var(--muted);font-size:.85rem}}
.table-previews{{border-top:1px solid var(--line);padding:.7rem;overflow:auto}}.table-previews h3{{font-size:.85rem;margin:0 0 .6rem;color:var(--muted)}}.table-previews table{{border-collapse:collapse;margin:.6rem 0;width:100%;font-size:.82rem;background:#fff;color:#17202a}}.table-previews th,.table-previews td{{border:1px solid #8a98a8;padding:.3rem;vertical-align:top;text-align:left;background:#fff;color:#17202a}}.table-previews th{{background:#e4edf5;color:#111820}}.table-previews caption{{color:#17202a}}
@media(max-width:900px){{main{{display:block}}.source{{position:relative;top:0;height:55vh;border-right:0;border-bottom:1px solid var(--line)}}}}
</style></head><body>
<script id="review-data" type="application/json">{embedded}</script>
<header><h1>Blinded conversion review</h1><label>PDF page <select id="page"></select></label><button id="copy-previous">Copy previous ratings</button><button id="import">Import scores</button><input id="import-file" type="file" accept="application/json,.json" hidden><button id="export">Export scores</button><small id="status"></small></header>
<main><section class="source"><iframe id="pdf" title="Source PDF"></iframe></section><section class="review"><details class="guide"><summary>Rating guide</summary><p>Score candidates independently: 1 unusable, 3 acceptable, 5 publication-ready. Use N/A when the feature is absent or cannot be assessed.</p><ol>{score_guide}</ol></details><div id="columns" class="columns"></div>
<table><thead><tr><th>Dimension</th><th>Scores: 1 unusable · 3 acceptable · 5 excellent</th></tr></thead><tbody>{dimensions}</tbody></table>
<label>Page notes<textarea id="notes" placeholder="Dropped text, order errors, hierarchy, tables, assets, references…"></textarea></label></section></main>
<script>
const data=JSON.parse(document.querySelector('#review-data').textContent),pageSelect=document.querySelector('#page'),stateKey='blobforge-review:'+data.campaign_digest;
let scores={{}};try{{scores=JSON.parse(localStorage.getItem(stateKey)||'{{}}')}}catch(_error){{document.querySelector('#status').textContent='autosave unavailable; use Export scores'}}
for(const page of data.pages){{const o=document.createElement('option');o.value=page.index;o.textContent=page.label;pageSelect.append(o)}}
document.documentElement.style.setProperty('--count',data.candidates.length);
function save(){{try{{localStorage.setItem(stateKey,JSON.stringify(scores));document.querySelector('#status').textContent='saved locally';return true}}catch(_error){{document.querySelector('#status').textContent='autosave unavailable; use Export scores';return false}}}}
function safeTable(fragment){{const parsed=new DOMParser().parseFromString(fragment,'text/html'),source=parsed.body.firstElementChild,allowed=new Set(['TABLE','CAPTION','THEAD','TBODY','TFOOT','TR','TH','TD','STRONG','EM','CODE','SUB','SUP','BR']),scopes=new Set(['row','col','rowgroup','colgroup']);if(!source||source.tagName!=='TABLE'||parsed.body.children.length!==1)return null;function copy(node){{if(node.nodeType===Node.TEXT_NODE)return document.createTextNode(node.textContent);if(node.nodeType!==Node.ELEMENT_NODE||!allowed.has(node.tagName))return null;const target=document.createElement(node.tagName.toLowerCase());for(const attribute of node.attributes){{const name=attribute.name.toLowerCase(),value=attribute.value;if((name==='colspan'||name==='rowspan')&&/^\\d+$/.test(value)&&Number(value)>=1&&Number(value)<=1000)target.setAttribute(name,String(Number(value)));else if(name==='scope'&&node.tagName==='TH'&&scopes.has(value))target.setAttribute(name,value);else return null}}for(const child of node.childNodes){{const copied=copy(child);if(copied===null)return null;target.append(copied)}}return target}}return copy(source)}}
function tablePreviews(markdown){{const fragments=markdown.match(/<table(?:\\s[^>]*)?>[\\s\\S]*?<\\/table>/gi)||[],tables=fragments.map(safeTable).filter(Boolean);if(!tables.length)return null;const section=document.createElement('section');section.className='table-previews';const title=document.createElement('h3');title.textContent='Rendered semantic tables (strict allowlist)';section.append(title,...tables);return section}}
function render(){{const page=String(pageSelect.value),entry=data.pages.find(x=>String(x.index)===page);document.querySelector('#pdf').src='source.pdf#page='+entry.label;const columns=document.querySelector('#columns');columns.replaceChildren();for(const c of data.candidates){{const box=document.createElement('article');box.className='candidate';const h=document.createElement('h2');h.textContent='Candidate '+c.label,markdown=c.pages[page]||'[no mapped text]';const pre=document.createElement('pre');pre.textContent=markdown;box.append(h,pre);const previews=tablePreviews(markdown);if(previews)box.append(previews);const assets=document.createElement('section');assets.className='assets';const title=document.createElement('h3');title.textContent='Extracted assets on this page';assets.append(title);const pageAssets=c.assets[page]||[];if(!pageAssets.length){{const empty=document.createElement('span');empty.className='unavailable';empty.textContent='No linked extracted asset';assets.append(empty)}}for(const asset of pageAssets){{if(asset.previewable){{const img=document.createElement('img');img.src=asset.path;img.alt='Blinded extracted asset';img.loading='lazy';assets.append(img)}}else{{const unavailable=document.createElement('span');unavailable.className='unavailable';unavailable.textContent='Linked asset is not a safe raster preview';assets.append(unavailable)}}}}box.append(assets);columns.append(box)}}for(const cell of document.querySelectorAll('.scores')){{cell.replaceChildren();for(const c of data.candidates){{const wrap=document.createElement('label');wrap.className='score';wrap.append('Candidate '+c.label+' ');const select=document.createElement('select');select.innerHTML='<option value="">—</option>'+[1,2,3,4,5].map(x=>`<option value="${{x}}">${{x}}</option>`).join('')+'<option value="na">N/A</option>';select.value=scores[page]?.ratings?.[cell.dataset.dimension]?.[c.label]||'';select.onchange=()=>{{scores[page]??={{}};scores[page].ratings??={{}};scores[page].ratings[cell.dataset.dimension]??={{}};scores[page].ratings[cell.dataset.dimension][c.label]=select.value;save()}};wrap.append(select);cell.append(wrap)}}const notes=document.querySelector('#notes');notes.value=scores[page]?.notes||'';notes.oninput=()=>{{scores[page]??={{}};scores[page].notes=notes.value;save()}};const position=data.pages.findIndex(x=>String(x.index)===page),previous=position>0?String(data.pages[position-1].index):null;document.querySelector('#copy-previous').disabled=!previous||!scores[previous]?.ratings}}}}
pageSelect.onchange=render;document.querySelector('#copy-previous').onclick=()=>{{const position=data.pages.findIndex(x=>String(x.index)===String(pageSelect.value));if(position<1)return;const page=String(data.pages[position].index),previous=String(data.pages[position-1].index);if(!scores[previous]?.ratings)return;if(scores[page]?.ratings&&!confirm('Replace this page’s ratings with the previous page?'))return;scores[page]??={{}};scores[page].ratings=JSON.parse(JSON.stringify(scores[previous].ratings));const stored=save();render();document.querySelector('#status').textContent=stored?'previous ratings copied and saved locally':'previous ratings copied for this session; export to retain'}};const importFile=document.querySelector('#import-file');document.querySelector('#import').onclick=()=>importFile.click();importFile.onchange=async()=>{{const file=importFile.files[0];if(!file)return;try{{const imported=JSON.parse(await file.text());if(imported.format!==data.format||imported.campaign_digest!==data.campaign_digest||!imported.scores||typeof imported.scores!=='object'||Array.isArray(imported.scores))throw new Error('wrong campaign or invalid result');scores=imported.scores;const stored=save();render();document.querySelector('#status').textContent=stored?'scores imported and saved locally':'scores imported for this session; export to retain'}}catch(error){{document.querySelector('#status').textContent='import failed: '+error.message}}finally{{importFile.value=''}}}};document.querySelector('#export').onclick=()=>{{const output={{format:data.format,campaign_digest:data.campaign_digest,exported_at:new Date().toISOString(),scores}};const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(output,null,2)+'\\n'],{{type:'application/json'}}));a.download='review-'+data.campaign_digest.slice(7,19)+'.json';a.click();URL.revokeObjectURL(a.href)}};render();
</script></body></html>"""


def build_review_bundle(
    source_path: str | Path,
    artifact_paths: Iterable[str | Path],
    output_dir: str | Path,
    *,
    pages: str | None = None,
    seed: str = "blobforge-review-v2",
    key_output: str | Path | None = None,
    review_format: str = REVIEW_FORMAT,
) -> ReviewBundleResult:
    source = Path(source_path).resolve()
    destination = Path(output_dir).resolve()
    if not source.is_file():
        raise ValueError(f"source is not a file: {source}")
    if destination.exists():
        raise ValueError(f"review destination already exists: {destination}")
    if review_format not in DIMENSIONS_BY_FORMAT:
        raise ValueError(f"unsupported review format: {review_format}")
    dimensions = DIMENSIONS_BY_FORMAT[review_format]
    artifacts = [_artifact(Path(path)) for path in artifact_paths]
    if len(artifacts) < 2:
        raise ValueError("a blinded review requires at least two artifacts")
    source_digest = blake3_file(source)
    if any(item["source_digest"] != source_digest for item in artifacts):
        raise ValueError("review artifacts do not all match the supplied source")
    common_pages = set(artifacts[0]["pages"])
    for item in artifacts[1:]:
        common_pages &= set(item["pages"])
    selected_pages = parse_page_selection(pages, common_pages)
    if not selected_pages:
        raise ValueError("review artifacts have no commonly mapped pages")
    ranked = sorted(
        artifacts,
        key=lambda item: hashlib.sha256(
            f"{seed}\0{item['identity']}".encode("utf-8")
        ).digest(),
    )
    candidates = []
    key_candidates = []
    asset_copies: dict[str, list[tuple[str, str]]] = {}
    for index, item in enumerate(ranked):
        label = _candidate_label(index)
        candidate_pages, candidate_assets, copies = _prepare_candidate(
            item, label, selected_pages
        )
        candidates.append(
            {
                "label": label,
                "pages": candidate_pages,
                "assets": candidate_assets,
            }
        )
        asset_copies[item["path"]] = copies
        key_candidates.append(
            {
                "label": label,
                **{key: item[key] for key in ("path", "identity", "producer", "tools", "models")},
            }
        )
    campaign_body = {
        "format": review_format,
        "source_digest": source_digest,
        "artifact_identities": sorted(item["identity"] for item in artifacts),
        "pages": list(selected_pages),
        "dimensions": list(dimensions),
        "seed_sha256": "sha256:" + hashlib.sha256(seed.encode("utf-8")).hexdigest(),
    }
    campaign_digest = blake3_bytes(canonical_json_bytes(campaign_body))
    public = {
        "format": review_format,
        "campaign_digest": campaign_digest,
        "pages": [
            {"index": page, "label": page + 1} for page in selected_pages
        ],
        "dimensions": list(dimensions),
        "candidates": candidates,
    }
    key = {
        **campaign_body,
        "campaign_digest": campaign_digest,
        "seed": seed,
        "source_path": str(source),
        "candidates": key_candidates,
    }
    key_path = Path(key_output).resolve() if key_output else destination.with_suffix(".key.json")
    if key_path.exists():
        raise ValueError(f"review key destination already exists: {key_path}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    temporary_key = key_path.with_name(f".{key_path.name}.{os.getpid()}.tmp")
    try:
        shutil.copy2(source, temporary / "source.pdf")
        for artifact_path, copies in asset_copies.items():
            with zipfile.ZipFile(artifact_path) as archive:
                for source_name, output_name in copies:
                    asset_output = temporary / output_name
                    asset_output.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(source_name) as input_stream:
                        with asset_output.open("wb") as output_stream:
                            shutil.copyfileobj(input_stream, output_stream)
        (temporary / "review.json").write_text(
            json.dumps(public, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (temporary / "index.html").write_text(_html(public), encoding="utf-8")
        temporary_key.write_text(
            json.dumps(key, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        os.chmod(temporary_key, 0o600)
        os.replace(temporary, destination)
        os.replace(temporary_key, key_path)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
        temporary_key.unlink(missing_ok=True)
    return ReviewBundleResult(
        destination, key_path, campaign_digest, len(artifacts), len(selected_pages)
    )
