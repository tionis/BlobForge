"""Deterministic, blinded, source-backed MDAF review bundles."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .mdaf import blake3_file, validate_mdaf
from .mdaf.digest import blake3_bytes, canonical_json_bytes

REVIEW_FORMAT = "dev.tionis.blobforge.review/v1"
DEFAULT_DIMENSIONS = (
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


@dataclass(frozen=True)
class ReviewBundleResult:
    root: Path
    key_path: Path
    campaign_digest: str
    artifacts: int
    pages: int


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
    return {
        "path": str(path.resolve()),
        "identity": validated.identity,
        "source_digest": sources[0]["digest"],
        "producer": validated.manifest["producer"],
        "tools": activity.get("tools", []),
        "models": activity.get("models", []),
        "pages": _page_text(text, source_map),
    }


def _html(public: dict[str, Any]) -> str:
    embedded = json.dumps(public, ensure_ascii=False, separators=(",", ":")).replace(
        "<", "\\u003c"
    )
    dimensions = "".join(
        f'<tr><th>{dimension}</th><td class="scores" data-dimension="{dimension}"></td></tr>'
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
.score{{display:flex;align-items:center;gap:.3rem}}textarea{{width:100%;min-height:8rem;margin-top:1rem}}small{{color:var(--muted)}}
@media(max-width:900px){{main{{display:block}}.source{{position:relative;top:0;height:55vh;border-right:0;border-bottom:1px solid var(--line)}}}}
</style></head><body>
<script id="review-data" type="application/json">{embedded}</script>
<header><h1>Blinded conversion review</h1><label>PDF page <select id="page"></select></label><button id="export">Export scores</button><small id="status"></small></header>
<main><section class="source"><iframe id="pdf" title="Source PDF"></iframe></section><section class="review"><div id="columns" class="columns"></div>
<table><thead><tr><th>Dimension</th><th>Scores: 1 unusable · 3 acceptable · 5 excellent</th></tr></thead><tbody>{dimensions}</tbody></table>
<label>Page notes<textarea id="notes" placeholder="Dropped text, order errors, hierarchy, tables, assets, references…"></textarea></label></section></main>
<script>
const data=JSON.parse(document.querySelector('#review-data').textContent),pageSelect=document.querySelector('#page'),stateKey='blobforge-review:'+data.campaign_digest;
let scores={{}};try{{scores=JSON.parse(localStorage.getItem(stateKey)||'{{}}')}}catch(_error){{document.querySelector('#status').textContent='autosave unavailable; use Export scores'}}
for(const page of data.pages){{const o=document.createElement('option');o.value=page.index;o.textContent=page.label;pageSelect.append(o)}}
document.documentElement.style.setProperty('--count',data.candidates.length);
function save(){{try{{localStorage.setItem(stateKey,JSON.stringify(scores));document.querySelector('#status').textContent='saved locally'}}catch(_error){{document.querySelector('#status').textContent='autosave unavailable; use Export scores'}}}}
function render(){{const page=String(pageSelect.value),entry=data.pages.find(x=>String(x.index)===page);document.querySelector('#pdf').src='source.pdf#page='+entry.label;const columns=document.querySelector('#columns');columns.replaceChildren();for(const c of data.candidates){{const box=document.createElement('article');box.className='candidate';const h=document.createElement('h2');h.textContent='Candidate '+c.label;const pre=document.createElement('pre');pre.textContent=c.pages[page]||'[no mapped text]';box.append(h,pre);columns.append(box)}}for(const cell of document.querySelectorAll('.scores')){{cell.replaceChildren();for(const c of data.candidates){{const wrap=document.createElement('label');wrap.className='score';wrap.append('Candidate '+c.label+' ');const select=document.createElement('select');select.innerHTML='<option value="">—</option>'+[1,2,3,4,5].map(x=>`<option>${{x}}</option>`).join('');select.value=scores[page]?.ratings?.[cell.dataset.dimension]?.[c.label]||'';select.onchange=()=>{{scores[page]??={{}};scores[page].ratings??={{}};scores[page].ratings[cell.dataset.dimension]??={{}};scores[page].ratings[cell.dataset.dimension][c.label]=select.value;save()}};wrap.append(select);cell.append(wrap)}}const notes=document.querySelector('#notes');notes.value=scores[page]?.notes||'';notes.oninput=()=>{{scores[page]??={{}};scores[page].notes=notes.value;save()}}}}}}
pageSelect.onchange=render;document.querySelector('#export').onclick=()=>{{const output={{format:data.format,campaign_digest:data.campaign_digest,exported_at:new Date().toISOString(),scores}};const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(output,null,2)+'\\n'],{{type:'application/json'}}));a.download='review-'+data.campaign_digest.slice(7,19)+'.json';a.click();URL.revokeObjectURL(a.href)}};render();
</script></body></html>"""


def build_review_bundle(
    source_path: str | Path,
    artifact_paths: Iterable[str | Path],
    output_dir: str | Path,
    *,
    pages: str | None = None,
    seed: str = "blobforge-review-v1",
    key_output: str | Path | None = None,
) -> ReviewBundleResult:
    source = Path(source_path).resolve()
    destination = Path(output_dir).resolve()
    if not source.is_file():
        raise ValueError(f"source is not a file: {source}")
    if destination.exists():
        raise ValueError(f"review destination already exists: {destination}")
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
    for index, item in enumerate(ranked):
        label = chr(ord("A") + index) if index < 26 else f"C{index + 1}"
        candidates.append(
            {
                "label": label,
                "pages": {str(page): item["pages"][page] for page in selected_pages},
            }
        )
        key_candidates.append(
            {
                "label": label,
                **{key: item[key] for key in ("path", "identity", "producer", "tools", "models")},
            }
        )
    campaign_body = {
        "format": REVIEW_FORMAT,
        "source_digest": source_digest,
        "artifact_identities": sorted(item["identity"] for item in artifacts),
        "pages": list(selected_pages),
        "dimensions": list(DEFAULT_DIMENSIONS),
        "seed_sha256": "sha256:" + hashlib.sha256(seed.encode("utf-8")).hexdigest(),
    }
    campaign_digest = blake3_bytes(canonical_json_bytes(campaign_body))
    public = {
        "format": REVIEW_FORMAT,
        "campaign_digest": campaign_digest,
        "pages": [
            {"index": page, "label": page + 1} for page in selected_pages
        ],
        "dimensions": list(DEFAULT_DIMENSIONS),
        "candidates": candidates,
    }
    key = {
        **campaign_body,
        "campaign_digest": campaign_digest,
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
